#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <functional>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace chhttp {

using namespace std::chrono_literals;

template <class T> class Task;

namespace detail {

// Completion channel shared between a coroutine and its single consumer.
//
// One atomic word carries the whole hand-off. It holds either a sentinel (no
// consumer yet / a thread is parked / already completed) or the address of the
// awaiting coroutine. Awaiting is therefore a single compare-exchange with no
// lock and no condition variable, which matters because every coroutine in the
// library hosts one of these and the previous mutex + condition variable pair
// dominated the cost of short-lived internal coroutines. Only a blocking
// consumer pays for a condition variable, and only the ones that actually
// block: that object is created lazily in wait(), so the coroutine-to-coroutine
// case -- every request on the hot path -- stays allocation-free.
//
// The state lives inside the coroutine frame, so a coroutine costs exactly one
// allocation instead of a frame plus a control block. Frame lifetime is tracked
// with `references`: one reference for the Task handed back to the caller and
// one held by the running coroutine itself, released at the final suspend
// point. The second reference is what keeps a frame alive when its Task is
// abandoned while a callback still has to resume it.
template <class T> struct TaskState {
  // Sentinels for `handoff`. A registered coroutine always contributes a real
  // address, which is above both values.
  static constexpr std::uintptr_t unclaimed = 0;
  static constexpr std::uintptr_t completed = 1;
  static constexpr std::uintptr_t parked = 2;

  // A blocking waiter parks on this instead of on `handoff`. Created on first
  // use, so the overwhelmingly common shape -- a coroutine whose result is
  // consumed by another coroutine -- neither stores a mutex nor allocates one.
  // std::atomic::wait is not a substitute: implementations poll with an
  // exponential backoff before entering the kernel, which measured ~4.5 us of
  // user CPU per short wait on macOS/libc++ against ~0.5 us for a condition
  // variable, and every synchronous request pays for exactly one such wait.
  struct Block {
    std::mutex mutex;
    std::condition_variable cv;
  };

  std::atomic<std::uintptr_t> handoff{unclaimed};
  std::atomic<unsigned> references{2};
  std::optional<T> value;
  std::exception_ptr exception;
  std::unique_ptr<Block> block;

  [[nodiscard]] bool done() const noexcept {
    return handoff.load(std::memory_order_acquire) == completed;
  }

  // Publishes completion. The result must already be stored in value/exception.
  void publish() noexcept {
    const auto previous = handoff.exchange(completed, std::memory_order_acq_rel);
    if (previous > parked) {
      std::coroutine_handle<>::from_address(
          reinterpret_cast<void *>(previous)).resume();
      return;
    }
    if (previous != parked) return;
    // A blocking waiter owns the word, so `block` was allocated before it
    // announced itself. Taking the mutex here closes the window in which the
    // waiter has tested the predicate but not yet entered wait(); the coroutine
    // still holds its own reference at this point, so the frame -- and with it
    // this block -- cannot be destroyed under us.
    std::lock_guard lock(block->mutex);
    block->cv.notify_one();
  }

  // Registers `continuation` unless completion already happened, in which case
  // the awaiter must not suspend.
  [[nodiscard]] bool suspend(std::coroutine_handle<> continuation) noexcept {
    const auto address =
        reinterpret_cast<std::uintptr_t>(continuation.address());
    auto expected = unclaimed;
    for (;;) {
      if (handoff.compare_exchange_strong(expected, address,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire))
        return true;
      if (expected == completed) return false;
      // Only a blocking consumer can own the word without being registered
      // above. Poll until it leaves, which happens when the result is
      // published; parking instead would block on a value nobody notifies.
      expected = unclaimed;
      std::this_thread::yield();
    }
  }

  // Parks the calling thread until completion.
  void wait() noexcept {
    auto current = handoff.load(std::memory_order_acquire);
    while (current != completed) {
      if (current == unclaimed) {
        // The block has to exist before `parked` becomes visible, because
        // publish() dereferences it as soon as it observes that value. Creating
        // it on this branch rather than on entry keeps the overwhelmingly
        // common case -- get() on a coroutine that already finished -- free of
        // an allocation, while still ordering it before the store that publishes
        // `parked`.
        if (!block) block = std::make_unique<Block>();
        // Announce the imminent park so completion knows a thread must be woken.
        handoff.compare_exchange_strong(current, parked,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire);
        continue;
      }
      if (current == parked) {
        // Only this thread can ever store `parked`, and it always allocates
        // first, so the block exists here.
        std::unique_lock lock(block->mutex);
        // The predicate re-reads `handoff` under the mutex. publish() stores
        // `completed` before it takes that mutex, so a completion racing with
        // the transition above is either already visible here or blocks in
        // publish() until this thread is inside wait(); neither order can drop
        // the wake-up.
        block->cv.wait(lock, [this] {
          return handoff.load(std::memory_order_acquire) == completed;
        });
        return;
      }
      // An awaiting coroutine owns the word. It is resumed by publish(), which
      // does not notify, so parking here could miss the wake-up entirely.
      std::this_thread::yield();
      current = handoff.load(std::memory_order_acquire);
    }
  }
};

template <> struct TaskState<void> {
  static constexpr std::uintptr_t unclaimed = 0;
  static constexpr std::uintptr_t completed = 1;
  static constexpr std::uintptr_t parked = 2;

  // See TaskState<T>: only a blocking consumer creates this, and only before
  // it announces `parked`.
  struct Block {
    std::mutex mutex;
    std::condition_variable cv;
  };

  std::atomic<std::uintptr_t> handoff{unclaimed};
  std::atomic<unsigned> references{2};
  std::exception_ptr exception;
  std::unique_ptr<Block> block;

  [[nodiscard]] bool done() const noexcept {
    return handoff.load(std::memory_order_acquire) == completed;
  }

  void publish() noexcept {
    const auto previous = handoff.exchange(completed, std::memory_order_acq_rel);
    if (previous > parked) {
      std::coroutine_handle<>::from_address(
          reinterpret_cast<void *>(previous)).resume();
      return;
    }
    if (previous != parked) return;
    std::lock_guard lock(block->mutex);
    block->cv.notify_one();
  }

  [[nodiscard]] bool suspend(std::coroutine_handle<> continuation) noexcept {
    const auto address =
        reinterpret_cast<std::uintptr_t>(continuation.address());
    auto expected = unclaimed;
    for (;;) {
      if (handoff.compare_exchange_strong(expected, address,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire))
        return true;
      if (expected == completed) return false;
      expected = unclaimed;
      std::this_thread::yield();
    }
  }

  void wait() noexcept {
    auto current = handoff.load(std::memory_order_acquire);
    while (current != completed) {
      if (current == unclaimed) {
        // See TaskState<T>: the block must exist before `parked` becomes
        // visible, and only this branch ever publishes that value.
        if (!block) block = std::make_unique<Block>();
        handoff.compare_exchange_strong(current, parked,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire);
        continue;
      }
      if (current == parked) {
        // Only this thread can ever store `parked`, and it always allocates
        // first, so the block exists here.
        std::unique_lock lock(block->mutex);
        block->cv.wait(lock, [this] {
          return handoff.load(std::memory_order_acquire) == completed;
        });
        return;
      }
      std::this_thread::yield();
      current = handoff.load(std::memory_order_acquire);
    }
  }
};

// Drops one reference and reports whether the caller took the last one, in
// which case the frame hosting this state has to be destroyed. Deliberately a
// free function: the state lives inside that frame, so nothing may touch it
// after the decrement, and the short-circuit guarantees `done()` is evaluated
// only while this caller still holds the state alive. The reference count is
// also what makes the hand-off race-free -- exactly one of the two parties that
// can drop the final reference observes the transition to zero.
template <class State>
[[nodiscard]] bool release_task_state(State &state) noexcept {
  return state.references.fetch_sub(1, std::memory_order_acq_rel) == 1 &&
         state.done();
}

// Final suspend point of every Task. The result is already in the frame when
// publish() runs, so resuming the consumer here is safe. The coroutine then
// releases its own reference: if that was the last one nobody else can hold the
// frame, so returning false lets the run-time destroy it; otherwise the frame
// stays alive for the consumer that still holds a reference.
struct TaskFinalAwaiter {
  bool await_ready() const noexcept { return false; }
  template <class Promise>
  static bool await_suspend(std::coroutine_handle<Promise> frame) noexcept {
    Promise &promise = frame.promise();
    promise.state.publish();
    return !release_task_state(promise.state);
  }
  void await_resume() const noexcept {}
};

} // namespace detail

// A small executor-neutral C++20 coroutine result. Network operations resume
// on the libuv loop that owns them; get() may be used by blocking callers.
//
// The result state lives inside the coroutine frame and is reference counted by
// hand, so a coroutine allocates exactly one block -- the frame -- instead of a
// frame plus a control block. A Task and the Awaiter created from it each hold
// one reference; the frame is destroyed by whichever party drops the last one
// once the result has been published, or by the run-time when the coroutine
// finishes with no consumer left.
template <class T> class [[nodiscard]] Task {
public:
  using state_type = detail::TaskState<T>;

  struct promise_type {
    state_type state{};

    Task get_return_object() noexcept {
      return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_never initial_suspend() const noexcept { return {}; }
    detail::TaskFinalAwaiter final_suspend() const noexcept { return {}; }
    template <class U> void return_value(U &&result) {
      state.value.emplace(std::forward<U>(result));
    }
    void unhandled_exception() noexcept {
      state.exception = std::current_exception();
    }
  };

  Task() noexcept = default;
  Task(Task &&other) noexcept : frame_(std::exchange(other.frame_, {})) {}
  Task &operator=(Task &&other) noexcept {
    if (this != &other) {
      release();
      frame_ = std::exchange(other.frame_, {});
    }
    return *this;
  }
  Task(const Task &) = delete;
  Task &operator=(const Task &) = delete;
  ~Task() { release(); }

  [[nodiscard]] bool ready() const noexcept {
    return !frame_ || frame_.promise().state.done();
  }

  T get() {
    if (!frame_) throw std::logic_error("Empty chhttp::Task");
    const auto frame = std::exchange(frame_, {});
    auto &state = frame.promise().state;
    state.wait();
    // Everything is copied out of the state before the reference is dropped,
    // because releasing the last one destroys the frame that stores it.
    if (state.exception) {
      const auto failure = state.exception;
      if (detail::release_task_state(state)) frame.destroy();
      std::rethrow_exception(failure);
    }
    T result = std::move(*state.value);
    if (detail::release_task_state(state)) frame.destroy();
    return result;
  }

  struct Awaiter {
    std::coroutine_handle<promise_type> frame{};

    Awaiter() noexcept = default;
    explicit Awaiter(std::coroutine_handle<promise_type> frame) noexcept
        : frame(frame) {}
    Awaiter(Awaiter &&other) noexcept : frame(std::exchange(other.frame, {})) {}
    Awaiter(const Awaiter &) = delete;
    Awaiter &operator=(const Awaiter &) = delete;
    Awaiter &operator=(Awaiter &&) = delete;
    // A parent coroutine destroyed while suspended must not strand the
    // reference it holds over the task it was awaiting.
    ~Awaiter() { release(); }

    [[nodiscard]] bool await_ready() const noexcept {
      return !frame || frame.promise().state.done();
    }
    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      return frame && frame.promise().state.suspend(continuation);
    }
    T await_resume() {
      if (!frame) throw std::logic_error("Empty chhttp::Task");
      const auto owner = std::exchange(frame, {});
      auto &state = owner.promise().state;
      if (state.exception) {
        const auto failure = state.exception;
        if (detail::release_task_state(state)) owner.destroy();
        std::rethrow_exception(failure);
      }
      T result = std::move(*state.value);
      if (detail::release_task_state(state)) owner.destroy();
      return result;
    }

  private:
    void release() noexcept {
      if (!frame) return;
      const auto owner = std::exchange(frame, {});
      if (detail::release_task_state(owner.promise().state))
        owner.destroy();
    }
  };

  Awaiter operator co_await() && noexcept {
    return Awaiter{std::exchange(frame_, {})};
  }

private:
  explicit Task(std::coroutine_handle<promise_type> frame) noexcept
      : frame_(frame) {}

  void release() noexcept {
    if (!frame_) return;
    const auto frame = std::exchange(frame_, {});
    if (detail::release_task_state(frame.promise().state))
      frame.destroy();
  }

  std::coroutine_handle<promise_type> frame_{};
};

template <> class [[nodiscard]] Task<void> {
public:
  using state_type = detail::TaskState<void>;

  struct promise_type {
    state_type state{};

    Task get_return_object() noexcept {
      return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_never initial_suspend() const noexcept { return {}; }
    detail::TaskFinalAwaiter final_suspend() const noexcept { return {}; }
    void return_void() const noexcept {}
    void unhandled_exception() noexcept {
      state.exception = std::current_exception();
    }
  };

  Task() noexcept = default;
  Task(Task &&other) noexcept : frame_(std::exchange(other.frame_, {})) {}
  Task &operator=(Task &&other) noexcept {
    if (this != &other) {
      release();
      frame_ = std::exchange(other.frame_, {});
    }
    return *this;
  }
  Task(const Task &) = delete;
  Task &operator=(const Task &) = delete;
  ~Task() { release(); }

  [[nodiscard]] bool ready() const noexcept {
    return !frame_ || frame_.promise().state.done();
  }
  void get() {
    if (!frame_) throw std::logic_error("Empty chhttp::Task");
    const auto frame = std::exchange(frame_, {});
    auto &state = frame.promise().state;
    state.wait();
    const auto failure = state.exception;
    if (detail::release_task_state(state)) frame.destroy();
    if (failure) std::rethrow_exception(failure);
  }
  struct Awaiter {
    std::coroutine_handle<promise_type> frame{};

    Awaiter() noexcept = default;
    explicit Awaiter(std::coroutine_handle<promise_type> frame) noexcept
        : frame(frame) {}
    Awaiter(Awaiter &&other) noexcept : frame(std::exchange(other.frame, {})) {}
    Awaiter(const Awaiter &) = delete;
    Awaiter &operator=(const Awaiter &) = delete;
    Awaiter &operator=(Awaiter &&) = delete;
    ~Awaiter() { release(); }

    [[nodiscard]] bool await_ready() const noexcept {
      return !frame || frame.promise().state.done();
    }
    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      return frame && frame.promise().state.suspend(continuation);
    }
    void await_resume() {
      if (!frame) throw std::logic_error("Empty chhttp::Task");
      const auto owner = std::exchange(frame, {});
      auto &state = owner.promise().state;
      const auto failure = state.exception;
      if (detail::release_task_state(state)) owner.destroy();
      if (failure) std::rethrow_exception(failure);
    }

  private:
    void release() noexcept {
      if (!frame) return;
      const auto owner = std::exchange(frame, {});
      if (detail::release_task_state(owner.promise().state))
        owner.destroy();
    }
  };
  Awaiter operator co_await() && noexcept {
    return Awaiter{std::exchange(frame_, {})};
  }

private:
  explicit Task(std::coroutine_handle<promise_type> frame) noexcept
      : frame_(frame) {}

  void release() noexcept {
    if (!frame_) return;
    const auto frame = std::exchange(frame_, {});
    if (detail::release_task_state(frame.promise().state))
      frame.destroy();
  }

  std::coroutine_handle<promise_type> frame_{};
};

Task<void> sleep_for(std::chrono::milliseconds duration);

class Headers {
public:
  using value_type = std::pair<std::string, std::string>;
  using container_type = std::vector<value_type>;
  using iterator = container_type::iterator;
  using const_iterator = container_type::const_iterator;

  Headers() = default;
  Headers(std::initializer_list<value_type> values);

  void add(std::string name, std::string value);
  void set(std::string name, std::string value);
  bool erase(std::string_view name);
  // Reserves capacity for a known number of fields so bulk parsing performs a
  // single allocation instead of growing the container repeatedly.
  void reserve(std::size_t count);
  [[nodiscard]] bool contains(std::string_view name) const;
  [[nodiscard]] std::string get(std::string_view name,
                                std::string_view fallback = {}) const;
  [[nodiscard]] std::vector<std::string> get_all(std::string_view name) const;
  [[nodiscard]] std::size_t size() const noexcept { return values_.size(); }
  [[nodiscard]] bool empty() const noexcept { return values_.empty(); }
  iterator begin() noexcept { return values_.begin(); }
  iterator end() noexcept { return values_.end(); }
  const_iterator begin() const noexcept { return values_.begin(); }
  const_iterator end() const noexcept { return values_.end(); }
  const_iterator cbegin() const noexcept { return values_.cbegin(); }
  const_iterator cend() const noexcept { return values_.cend(); }

private:
  container_type values_;
};

using Params = std::vector<std::pair<std::string, std::string>>;
using PathParams = std::unordered_map<std::string, std::string>;

struct MultipartPart {
  std::string name;
  std::string filename;
  std::string content_type;
  Headers headers;
  std::string content;
};
using MultipartForm = std::vector<MultipartPart>;

enum class Error {
  none,
  invalid_argument,
  cancelled,
  invalid_url,
  resolve,
  connect,
  timeout,
  write,
  read,
  protocol,
  body_too_large,
  redirect_limit,
  tls_unavailable,
  tls_configuration,
  tls_handshake,
  tls_verification,
  proxy,
  compression,
  multipart,
  websocket_handshake,
  websocket_closed,
  server_stopped,
  internal
};

enum class TransferPhase {
  none,
  resolve,
  connect,
  tls_handshake,
  request_head,
  request_body,
  response_head,
  response_body
};

struct TransferProgress {
  std::uint64_t transferred{0};
  std::optional<std::uint64_t> total;
};

using ProgressHandler = std::function<bool(const TransferProgress &)>;

struct TransferContext {
  TransferPhase phase{TransferPhase::none};
  std::uint64_t uploaded{0};
  std::uint64_t downloaded{0};
  bool response_started{false};
};

struct ErrorInfo {
  Error code{Error::none};
  std::string message;
  int system_code{0};
  long tls_code{0};
  std::optional<TransferContext> transfer{};

  explicit operator bool() const noexcept { return code != Error::none; }
};

template <class T> class Result {
public:
  Result() : error_{Error::internal, "Empty result"} {}
  Result(T value) : value_(std::move(value)) {}
  Result(ErrorInfo error) : error_(std::move(error)) {}

  [[nodiscard]] explicit operator bool() const noexcept {
    return value_.has_value();
  }
  [[nodiscard]] bool has_value() const noexcept { return value_.has_value(); }
  T &value() & { return value_.value(); }
  const T &value() const & { return value_.value(); }
  T &&value() && { return std::move(value_.value()); }
  T *operator->() { return &value_.value(); }
  const T *operator->() const { return &value_.value(); }
  T &operator*() { return value_.value(); }
  const T &operator*() const { return value_.value(); }
  [[nodiscard]] const ErrorInfo &error() const noexcept { return error_; }

private:
  std::optional<T> value_;
  ErrorInfo error_;
};

template <> class Result<void> {
public:
  Result() = default;
  Result(ErrorInfo error) : error_(std::move(error)) {}

  [[nodiscard]] explicit operator bool() const noexcept { return !error_; }
  [[nodiscard]] bool has_value() const noexcept { return !error_; }
  void value() const {
    if (error_)
      throw std::logic_error("chhttp::Result<void> has no value");
  }
  [[nodiscard]] const ErrorInfo &error() const noexcept { return error_; }

private:
  ErrorInfo error_;
};

class StreamWriter;
using StreamHandler = std::function<Task<void>(StreamWriter &)>;

struct FileBodyOptions {
  std::uint64_t offset{0};
  std::optional<std::uint64_t> length;
};

struct Request {
  std::string method{"GET"};
  std::string target{"/"};
  std::string path{"/"};
  unsigned version{11};
  Headers headers;
  Params query;
  PathParams path_params;
  std::string body;
  // A streamed client request body. Without a declared length, HTTP/1.1 uses
  // chunked transfer coding. StreamWriter::write() provides transport
  // backpressure without buffering the complete upload.
  StreamHandler body_stream;
  std::optional<std::uint64_t> body_stream_length;
  MultipartForm files;
  std::string remote_address;
  std::uint16_t remote_port{0};
  bool keep_alive{true};

  [[nodiscard]] bool has_header(std::string_view name) const;
  [[nodiscard]] std::string get_header(std::string_view name,
                                       std::string_view fallback = {}) const;
  [[nodiscard]] bool has_param(std::string_view name) const;
  [[nodiscard]] std::string get_param(std::string_view name,
                                      std::string_view fallback = {}) const;
  void
  set_stream_body(StreamHandler handler,
                  std::optional<std::uint64_t> content_length = std::nullopt);
  Result<void> set_file_body(std::filesystem::path path,
                             FileBodyOptions options = {});
  [[nodiscard]] bool is_streaming_body() const noexcept;
};

struct SseEvent {
  std::string data;
  std::string event;
  std::string id;
  std::optional<std::chrono::milliseconds> retry;
};

class StreamWriter {
public:
  struct Sink {
    virtual ~Sink() = default;
    virtual Task<bool> write(std::string data) = 0;
    virtual Task<bool> flush() = 0;
    virtual bool open() const noexcept = 0;
  };

  StreamWriter() = default;
  explicit StreamWriter(std::shared_ptr<Sink> sink) : sink_(std::move(sink)) {}
  Task<bool> write(std::string_view data);
  Task<bool> write(std::span<const std::byte> data);
  Task<bool> flush();
  [[nodiscard]] bool open() const noexcept;

private:
  std::shared_ptr<Sink> sink_;
};

enum class FileSinkMode { truncate, append, write_at };

struct FileSinkOptions {
  FileSinkMode mode{FileSinkMode::truncate};
  std::uint64_t offset{0};
};

class AsyncFileSink {
public:
  // Opening is synchronous; write/flush/sync run in order on the disk pool.
  // When awaited on a chhttp I/O loop, completion resumes on that same loop.
  static Result<AsyncFileSink> open(std::filesystem::path path,
                                    FileSinkOptions options = {});
  ~AsyncFileSink();
  AsyncFileSink(AsyncFileSink &&) noexcept;
  AsyncFileSink &operator=(AsyncFileSink &&) noexcept;
  AsyncFileSink(const AsyncFileSink &) = delete;
  AsyncFileSink &operator=(const AsyncFileSink &) = delete;

  Task<ErrorInfo> write(std::string_view data);
  Task<ErrorInfo> flush();
  Task<ErrorInfo> sync();
  [[nodiscard]] std::uint64_t written() const noexcept;
  [[nodiscard]] std::uint64_t position() const noexcept;

private:
  class Impl;
  explicit AsyncFileSink(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}
  std::shared_ptr<Impl> impl_;
};

using AsyncBodyConsumer = std::function<Task<bool>(std::string_view)>;
using BodyConsumer = std::function<bool(std::string_view)>;

struct StoredBody {
  std::filesystem::path path;
  std::uint64_t size{0};
};

// A single-use, pull-owned server request body. consume() does not read ahead
// while its awaited callback is suspended, so the callback is the backpressure
// boundary. Chunk views remain valid only until the callback returns.
class RequestBodyStream {
public:
  struct Source {
    virtual ~Source() = default;
    virtual Task<ErrorInfo> consume(AsyncBodyConsumer consumer) = 0;
    virtual void cancel() noexcept = 0;
    [[nodiscard]] virtual std::optional<std::uint64_t>
    content_length() const noexcept = 0;
    [[nodiscard]] virtual std::uint64_t received() const noexcept = 0;
    [[nodiscard]] virtual bool consumed() const noexcept = 0;
    [[nodiscard]] virtual bool complete() const noexcept = 0;
  };

  RequestBodyStream() = default;
  explicit RequestBodyStream(std::shared_ptr<Source> source)
      : source_(std::move(source)) {}
  Task<ErrorInfo> consume(AsyncBodyConsumer consumer);
  Task<ErrorInfo> consume(BodyConsumer consumer);
  Task<ErrorInfo> discard();
  Task<Result<StoredBody>> save_to_file(std::filesystem::path path);
  Task<Result<StoredBody>>
  save_to_temp(std::filesystem::path directory = {});
  void cancel() noexcept;
  [[nodiscard]] std::optional<std::uint64_t> content_length() const noexcept;
  [[nodiscard]] std::uint64_t received() const noexcept;
  [[nodiscard]] bool consumed() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  std::shared_ptr<Source> source_;
};

struct MultipartParserOptions {
  std::size_t max_parts{1024};
  std::size_t max_header_size{64 * 1024};
  std::uint64_t max_part_size{128ull * 1024 * 1024};
  std::uint64_t max_total_size{128ull * 1024 * 1024};
};

enum class MultipartEventType { part_begin, part_data, part_end };

struct MultipartEvent {
  MultipartEventType type{MultipartEventType::part_data};
  std::size_t part_index{0};
  MultipartPart part;
  std::string data;
};

// An incremental multipart/form-data parser. feed() may return several owned
// events; it retains at most boundary/header look-behind bytes between calls.
class MultipartParser {
public:
  explicit MultipartParser(std::string content_type,
                           MultipartParserOptions options = {});
  ~MultipartParser();
  MultipartParser(MultipartParser &&) noexcept;
  MultipartParser &operator=(MultipartParser &&) noexcept;
  MultipartParser(const MultipartParser &) = delete;
  MultipartParser &operator=(const MultipartParser &) = delete;

  Result<std::vector<MultipartEvent>> feed(std::string_view data);
  Result<std::vector<MultipartEvent>> finish();
  [[nodiscard]] bool complete() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

struct MultipartCallbacks {
  std::function<Task<bool>(const MultipartPart &)> on_part_begin;
  AsyncBodyConsumer on_part_data;
  std::function<Task<bool>(const MultipartPart &)> on_part_end;
};

Task<ErrorInfo> consume_multipart(RequestBodyStream &body,
                                  std::string content_type,
                                  MultipartCallbacks callbacks,
                                  MultipartParserOptions options = {});

class MultipartWriter {
public:
  explicit MultipartWriter(std::string boundary = {});
  MultipartWriter &add_field(std::string name, std::string value,
                             std::string content_type = {});
  MultipartWriter &add_file(std::string name, std::filesystem::path path,
                            std::string content_type = {},
                            std::string filename = {});
  MultipartWriter &add_file_slice(std::string name, std::filesystem::path path,
                                  FileBodyOptions slice,
                                  std::string content_type = {},
                                  std::string filename = {});
  MultipartWriter &add_stream(std::string name, std::string filename,
                              std::string content_type, StreamHandler producer,
                              std::optional<std::uint64_t> size = std::nullopt,
                              Headers headers = {});
  [[nodiscard]] std::string content_type() const;
  [[nodiscard]] std::optional<std::uint64_t> content_length() const;
  Task<void> write(StreamWriter &writer) const;
  void apply(Request &request) const;

private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

class Base64Encoder {
public:
  std::string update(std::span<const std::byte> data);
  std::string finish();
  void reset() noexcept;

private:
  std::array<std::byte, 3> pending_{};
  std::size_t pending_size_{0};
  bool finished_{false};
};

[[nodiscard]] std::optional<std::uint64_t>
base64_encoded_size(std::uint64_t input_size) noexcept;

// Writes JSON syntax directly to a streamed request body. String helpers
// escape incrementally; the Base64 string API retains only two source bytes.
class JsonStreamWriter {
public:
  explicit JsonStreamWriter(StreamWriter &writer) : writer_(writer) {}
  Task<bool> raw(std::string_view json);
  Task<bool> string(std::string_view value);
  Task<bool> begin_base64_string(std::string_view prefix = {});
  Task<bool> base64(std::span<const std::byte> data);
  Task<bool> end_base64_string();
  [[nodiscard]] bool open() const noexcept;

private:
  StreamWriter &writer_;
  Base64Encoder base64_;
  bool base64_string_open_{false};
};

class SseWriter {
public:
  explicit SseWriter(StreamWriter writer) : writer_(std::move(writer)) {}
  Task<bool> send(const SseEvent &event);
  Task<bool> data(std::string_view value);
  Task<bool> comment(std::string_view value = "keep-alive");
  [[nodiscard]] bool open() const noexcept { return writer_.open(); }

private:
  StreamWriter writer_;
};

using SseHandler = std::function<Task<void>(SseWriter &)>;

struct Response {
  int status{200};
  unsigned version{11};
  Headers headers;
  std::string body;
  bool keep_alive{true};

  void set_header(std::string name, std::string value);
  void set_content(std::string value,
                   std::string_view content_type = "text/plain; charset=utf-8");
  void set_redirect(std::string location, int redirect_status = 302);
  void set_file(std::filesystem::path path,
                std::string_view content_type = {});
  void set_stream(std::string content_type, StreamHandler handler);
  void set_sse(SseHandler handler);
  [[nodiscard]] bool is_streaming() const noexcept;

private:
  friend class Server;
  friend struct ServerAccess;
  std::optional<std::filesystem::path> file_path_;
  StreamHandler stream_handler_;
};

using ResponseResult = Result<Response>;

// Immutable response metadata delivered before any response body bytes. The
// callback receives the original wire headers, including Content-Encoding.
struct ResponseHead {
  int status{0};
  unsigned version{11};
  Headers headers;
  bool keep_alive{true};
};

using Handler = std::function<void(const Request &, Response &)>;
using AsyncHandler = std::function<Task<void>(const Request &, Response &)>;
using StreamRequestHandler =
    std::function<Task<void>(Request &, RequestBodyStream &, Response &)>;
using Middleware = std::function<bool(const Request &, Response &)>;
using Logger = std::function<void(const Request &, const Response &)>;
using ErrorHandler = std::function<void(const Request &, Response &)>;
using ExceptionHandler =
    std::function<void(const Request &, Response &, std::exception_ptr)>;

struct TlsServerOptions {
  std::filesystem::path certificate_file;
  std::filesystem::path private_key_file;
  std::filesystem::path private_key_password_file;
  std::filesystem::path client_ca_file;
  std::string certificate_pem;
  std::string private_key_pem;
  bool require_client_certificate{false};
  std::string ciphers;
};

struct ServerOptions {
  std::size_t worker_threads{0};
  std::size_t max_body_size{64 * 1024 * 1024};
  std::size_t max_header_size{64 * 1024};
  std::size_t keep_alive_max_requests{1000};
  std::chrono::seconds keep_alive_timeout{30};
  std::chrono::seconds request_timeout{60};
  std::chrono::milliseconds shutdown_timeout{5s};
  std::size_t compression_threshold{1024};
  bool tcp_no_delay{true};
  bool reuse_address{true};
  bool auto_decompress_request{true};
  bool auto_compress_response{true};
  std::optional<TlsServerOptions> tls;
};

struct StreamRouteOptions {
  // Zero inherits ServerOptions::max_body_size.
  std::size_t max_body_size{0};
  std::optional<std::chrono::milliseconds> first_body_byte_timeout;
  std::optional<std::chrono::milliseconds> idle_timeout;
  std::optional<std::chrono::milliseconds> total_timeout;
  bool auto_decompress{true};
};

enum class RoutingResult { pass, handled, rejected };

class WebSocket {
public:
  enum class MessageType { text, binary, ping, pong, close };
  struct Message {
    MessageType type{MessageType::text};
    std::string data;
  };
  struct Channel {
    virtual ~Channel() = default;
    virtual Task<Result<Message>> read() = 0;
    virtual Task<bool> send(std::string data, bool binary) = 0;
    virtual Task<void> ping(std::string data) = 0;
    virtual Task<void> close(std::uint16_t code, std::string reason) = 0;
    virtual bool open() const noexcept = 0;
    virtual std::string subprotocol() const = 0;
  };

  explicit WebSocket(std::shared_ptr<Channel> channel)
      : channel_(std::move(channel)) {}
  Task<Result<Message>> read();
  Task<bool> send_text(std::string_view data);
  Task<bool> send_binary(std::span<const std::byte> data);
  Task<void> ping(std::string_view data = {});
  Task<void> close(std::uint16_t code = 1000, std::string_view reason = {});
  [[nodiscard]] bool open() const noexcept;
  [[nodiscard]] std::string subprotocol() const;

private:
  std::shared_ptr<Channel> channel_;
};

using WebSocketHandler = std::function<Task<void>(const Request &, WebSocket &)>;
using SubprotocolSelector =
    std::function<std::string(const std::vector<std::string> &)>;

class Server {
public:
  explicit Server(ServerOptions options = {});
  ~Server();
  Server(Server &&) noexcept;
  Server &operator=(Server &&) noexcept;
  Server(const Server &) = delete;
  Server &operator=(const Server &) = delete;

  Server &route(std::string method, std::string pattern, Handler handler);
  Server &route_async(std::string method, std::string pattern,
                      AsyncHandler handler);
  Server &route_stream(std::string method, std::string pattern,
                       StreamRequestHandler handler,
                       StreamRouteOptions options = {});
  Server &get(std::string pattern, Handler handler);
  Server &post(std::string pattern, Handler handler);
  Server &put(std::string pattern, Handler handler);
  Server &patch(std::string pattern, Handler handler);
  Server &del(std::string pattern, Handler handler);
  Server &options(std::string pattern, Handler handler);
  Server &head(std::string pattern, Handler handler);
  Server &get_async(std::string pattern, AsyncHandler handler);
  Server &post_async(std::string pattern, AsyncHandler handler);
  Server &post_stream(std::string pattern, StreamRequestHandler handler,
                      StreamRouteOptions options = {});
  Server &put_stream(std::string pattern, StreamRequestHandler handler,
                     StreamRouteOptions options = {});
  Server &patch_stream(std::string pattern, StreamRequestHandler handler,
                       StreamRouteOptions options = {});
  Server &websocket(std::string pattern, WebSocketHandler handler,
                    SubprotocolSelector selector = {});
  Server &mount(std::string url_prefix, std::filesystem::path directory,
                Headers default_headers = {});
  Server &set_pre_routing_handler(Middleware handler);
  Server &set_post_routing_handler(Middleware handler);
  Server &set_error_handler(ErrorHandler handler);
  Server &set_exception_handler(ExceptionHandler handler);
  Server &set_logger(Logger logger);

  bool start(std::string host = "0.0.0.0", std::uint16_t port = 8080);
  bool listen(std::string host = "0.0.0.0", std::uint16_t port = 8080);
  void wait();
  void stop();
  [[nodiscard]] bool running() const noexcept;
  [[nodiscard]] std::uint16_t port() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

struct ProxyOptions {
  std::string url;
  std::string username;
  std::string password;
};

struct TlsClientOptions {
  bool verify_peer{true};
  bool use_system_certificates{true};
  std::filesystem::path ca_file;
  std::filesystem::path ca_directory;
  std::filesystem::path certificate_file;
  std::filesystem::path private_key_file;
  std::string certificate_pem;
  std::string private_key_pem;
  std::string server_name;
  std::string ciphers;
};

enum class AuthenticationType { none, basic, bearer, digest };

struct AuthenticationOptions {
  AuthenticationType type{AuthenticationType::none};
  std::string username;
  std::string password;
  std::string token;
};

struct ClientOptions {
  std::chrono::milliseconds connect_timeout{10s};
  std::chrono::milliseconds read_timeout{60s};
  std::chrono::milliseconds write_timeout{60s};
  std::size_t max_response_body_size{128 * 1024 * 1024};
  std::size_t max_redirects{10};
  // Maximum number of retained idle connections for each origin/proxy pair.
  std::size_t connection_pool_size{8};
  // Maximum active plus idle connections for each origin/proxy pair. Requests
  // wait asynchronously when the limit is reached; zero disables the limit.
  std::size_t max_connections_per_origin{64};
  bool follow_redirects{true};
  bool keep_alive{true};
  bool tcp_no_delay{true};
  bool auto_decompress{true};
  Headers default_headers;
  ProxyOptions proxy;
  TlsClientOptions tls;
  AuthenticationOptions authentication;
};

struct RequestOptions {
  // Runs before any body callback. Returning false rejects the response and
  // closes its connection. Synchronous callbacks run on the client's libuv
  // I/O thread and therefore must return quickly.
  std::function<bool(const ResponseHead &)> on_response_head;
  std::function<bool(std::string_view)> on_data;
  // An awaited consumer provides real backpressure without blocking the I/O
  // thread. Configure either on_data or on_data_async, never both.
  std::function<Task<bool>(std::string_view)> on_data_async;
  ProgressHandler on_upload_progress;
  ProgressHandler on_download_progress;
  // Compatibility alias for on_download_progress. A total of zero means the
  // response length is unknown. Configure only one download progress callback.
  std::function<bool(std::uint64_t current, std::uint64_t total)> on_progress;
  // Kept for source compatibility. It is observed at protocol boundaries but
  // cannot wake a blocked socket read; prefer stop_token for active cancel.
  std::shared_ptr<std::atomic_bool> cancellation;
  std::stop_token stop_token;

  // Per-request overrides. Unset phase timeouts inherit ClientOptions. A
  // deadline and total_timeout may both be supplied; the earlier limit wins.
  std::optional<std::chrono::steady_clock::time_point> deadline;
  std::optional<std::chrono::milliseconds> total_timeout;
  std::optional<std::chrono::milliseconds> connect_timeout;
  std::optional<std::chrono::milliseconds> read_timeout;
  std::optional<std::chrono::milliseconds> write_timeout;
  std::optional<std::chrono::milliseconds> header_timeout;
  std::optional<std::chrono::milliseconds> first_body_byte_timeout;
  std::optional<std::chrono::milliseconds> idle_timeout;
  std::optional<std::size_t> max_response_body_size;
  std::optional<bool> auto_decompress;
};

class AsyncClient {
public:
  explicit AsyncClient(std::string base_url, ClientOptions options = {});
  ~AsyncClient();
  AsyncClient(AsyncClient &&) noexcept;
  AsyncClient &operator=(AsyncClient &&) noexcept;
  AsyncClient(const AsyncClient &) = delete;
  AsyncClient &operator=(const AsyncClient &) = delete;

  Task<ResponseResult> request(Request request, RequestOptions options = {});
  Task<ResponseResult> get(std::string target, Headers headers = {},
                           RequestOptions options = {});
  Task<ResponseResult> head(std::string target, Headers headers = {},
                            RequestOptions options = {});
  Task<ResponseResult> post(std::string target, std::string body,
                            std::string content_type, Headers headers = {},
                            RequestOptions options = {});
  Task<ResponseResult> put(std::string target, std::string body,
                           std::string content_type, Headers headers = {},
                           RequestOptions options = {});
  Task<ResponseResult> patch(std::string target, std::string body,
                             std::string content_type, Headers headers = {},
                             RequestOptions options = {});
  Task<ResponseResult> del(std::string target, Headers headers = {},
                           RequestOptions options = {});
  void cancel();

private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

class Client {
public:
  explicit Client(std::string base_url, ClientOptions options = {});
  ~Client();
  Client(Client &&) noexcept;
  Client &operator=(Client &&) noexcept;
  Client(const Client &) = delete;
  Client &operator=(const Client &) = delete;

  ResponseResult request(Request request, RequestOptions options = {});
  ResponseResult get(std::string target, Headers headers = {},
                     RequestOptions options = {});
  ResponseResult head(std::string target, Headers headers = {},
                      RequestOptions options = {});
  ResponseResult post(std::string target, std::string body,
                      std::string content_type,
                      Headers headers = {}, RequestOptions options = {});
  ResponseResult put(std::string target, std::string body,
                     std::string content_type,
                     Headers headers = {}, RequestOptions options = {});
  ResponseResult patch(std::string target, std::string body,
                       std::string content_type,
                       Headers headers = {}, RequestOptions options = {});
  ResponseResult del(std::string target, Headers headers = {},
                     RequestOptions options = {});
  void cancel();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

struct SseClientOptions {
  bool reconnect{true};
  std::chrono::milliseconds initial_retry{3s};
  std::chrono::milliseconds max_retry{30s};
  std::string last_event_id;
};

struct SseParserOptions {
  std::size_t max_line_size{1024 * 1024};
  std::size_t max_event_size{8 * 1024 * 1024};
  std::string last_event_id;
};

// A request-method-agnostic incremental WHATWG event-stream parser. feed()
// accepts arbitrarily split transport chunks; finish() discards an incomplete
// final event, as required when the stream reaches EOF without a blank line.
class SseParser {
public:
  using MessageHandler = std::function<void(const SseEvent &)>;

  explicit SseParser(SseParserOptions options = {});
  ~SseParser();
  SseParser(SseParser &&) noexcept;
  SseParser &operator=(SseParser &&) noexcept;
  SseParser(const SseParser &) = delete;
  SseParser &operator=(const SseParser &) = delete;

  SseParser &on_message(MessageHandler handler);
  SseParser &on_event(std::string event, MessageHandler handler);
  ErrorInfo feed(std::string_view bytes);
  ErrorInfo finish();
  void reset();
  [[nodiscard]] std::string last_event_id() const;
  [[nodiscard]] std::optional<std::chrono::milliseconds> retry() const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class SseClient {
public:
  using MessageHandler = std::function<void(const SseEvent &)>;
  using ErrorCallback = std::function<void(const ErrorInfo &)>;
  using OpenHandler = std::function<void(const ResponseHead &)>;

  SseClient(AsyncClient &client, std::string target,
            Headers headers = {}, SseClientOptions options = {});
  SseClient(AsyncClient &client, Request request,
            SseClientOptions options = {});
  ~SseClient();
  SseClient(SseClient &&) noexcept;
  SseClient &operator=(SseClient &&) noexcept;
  SseClient(const SseClient &) = delete;
  SseClient &operator=(const SseClient &) = delete;

  SseClient &on_message(MessageHandler handler);
  SseClient &on_event(std::string event, MessageHandler handler);
  SseClient &on_open(OpenHandler handler);
  SseClient &on_error(ErrorCallback handler);
  Task<ErrorInfo> connect();
  void stop();
  [[nodiscard]] bool running() const noexcept;

private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

class AsyncWebSocketClient {
public:
  static Task<Result<std::shared_ptr<WebSocket>>>
  connect(std::string url, Headers headers = {}, ClientOptions options = {});
};

std::string url_encode(std::string_view value, bool space_as_plus = false);
Result<std::string> url_decode(std::string_view value,
                               bool plus_as_space = false);
Params parse_query(std::string_view query);
std::string make_query(const Params &params);
Result<MultipartForm> parse_multipart(std::string_view body,
                                      std::string_view content_type,
                                      std::size_t max_parts = 1024);
std::pair<std::string, std::string> make_multipart(const MultipartForm &parts,
                                                   std::string boundary = {});
std::string mime_type(const std::filesystem::path &path);
std::string status_reason(int status);
struct ContentRange {
  std::string unit;
  std::optional<std::uint64_t> first;
  std::optional<std::uint64_t> last;
  std::optional<std::uint64_t> total;

  [[nodiscard]] bool satisfied() const noexcept {
    return first.has_value() && last.has_value();
  }
};
Result<ContentRange> parse_content_range(std::string_view value);
Result<std::string>
format_byte_range(std::uint64_t first,
                  std::optional<std::uint64_t> last = std::nullopt);
Result<std::string>
format_content_range(std::uint64_t first, std::uint64_t last,
                     std::optional<std::uint64_t> total = std::nullopt);
Result<void> set_byte_range(Headers &headers, std::uint64_t first,
                            std::optional<std::uint64_t> last = std::nullopt,
                            std::string_view if_range = {});
std::string basic_auth(std::string_view username, std::string_view password);
std::string bearer_auth(std::string_view token);
struct DigestChallenge {
  std::string realm;
  std::string nonce;
  std::string opaque;
  std::string algorithm{"MD5"};
  std::string qop;
  bool stale{false};
};
Result<DigestChallenge> parse_digest_challenge(std::string_view value);
Result<std::string> digest_auth(
    std::string_view method, std::string_view uri, std::string_view username,
    std::string_view password, const DigestChallenge &challenge,
    std::uint32_t nonce_count = 1, std::string cnonce = {});
std::string format_sse(const SseEvent &event);

} // namespace chhttp
