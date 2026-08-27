#pragma once

#include <atomic>
#include <array>
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
#include <stop_token>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace chhttp {

using namespace std::chrono_literals;

template <class T> class Task;

namespace detail {

template <class T> struct TaskState {
  std::mutex mutex;
  std::condition_variable completed_cv;
  std::coroutine_handle<> continuation{};
  std::optional<T> value;
  std::exception_ptr exception;
  bool completed{false};

  void complete() noexcept {
    std::coroutine_handle<> pending_continuation;
    {
      std::lock_guard lock(mutex);
      completed = true;
      pending_continuation = std::exchange(continuation, {});
    }
    completed_cv.notify_all();
    if (pending_continuation) pending_continuation.resume();
  }
};

template <> struct TaskState<void> {
  std::mutex mutex;
  std::condition_variable completed_cv;
  std::coroutine_handle<> continuation{};
  std::exception_ptr exception;
  bool completed{false};

  void complete() noexcept {
    std::coroutine_handle<> pending_continuation;
    {
      std::lock_guard lock(mutex);
      completed = true;
      pending_continuation = std::exchange(continuation, {});
    }
    completed_cv.notify_all();
    if (pending_continuation) pending_continuation.resume();
  }
};

} // namespace detail

// A small executor-neutral C++20 coroutine result. Network operations resume
// on the libuv loop that owns them; get() may be used by blocking callers.
template <class T> class [[nodiscard]] Task {
public:
  using state_type = detail::TaskState<T>;

  struct promise_type {
    std::shared_ptr<state_type> state = std::make_shared<state_type>();

    Task get_return_object() noexcept { return Task(state); }
    std::suspend_never initial_suspend() const noexcept { return {}; }
    std::suspend_never final_suspend() const noexcept { return {}; }
    ~promise_type() { state->complete(); }
    template <class U> void return_value(U &&result) {
      state->value.emplace(std::forward<U>(result));
    }
    void unhandled_exception() noexcept {
      state->exception = std::current_exception();
    }
  };

  Task() = default;
  Task(Task &&) noexcept = default;
  Task &operator=(Task &&) noexcept = default;
  Task(const Task &) = delete;
  Task &operator=(const Task &) = delete;
  ~Task() = default;

  [[nodiscard]] bool ready() const noexcept {
    if (!state_) return true;
    std::lock_guard lock(state_->mutex);
    return state_->completed;
  }

  T get() {
    if (!state_) throw std::logic_error("Empty chhttp::Task");
    auto state = std::exchange(state_, {});
    {
      std::unique_lock lock(state->mutex);
      state->completed_cv.wait(lock, [&] { return state->completed; });
    }
    if (state->exception) std::rethrow_exception(state->exception);
    return std::move(*state->value);
  }

  struct Awaiter {
    std::shared_ptr<state_type> state;
    bool await_ready() const noexcept {
      if (!state) return true;
      std::lock_guard lock(state->mutex);
      return state->completed;
    }
    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      if (!state) return false;
      std::lock_guard lock(state->mutex);
      if (state->completed) return false;
      state->continuation = continuation;
      return true;
    }
    T await_resume() {
      if (!state) throw std::logic_error("Empty chhttp::Task");
      auto completed_state = std::exchange(state, {});
      if (completed_state->exception)
        std::rethrow_exception(completed_state->exception);
      return std::move(*completed_state->value);
    }
  };

  Awaiter operator co_await() && noexcept {
    return Awaiter{std::exchange(state_, {})};
  }

private:
  explicit Task(std::shared_ptr<state_type> state) noexcept
      : state_(std::move(state)) {}
  std::shared_ptr<state_type> state_;
};

template <> class [[nodiscard]] Task<void> {
public:
  using state_type = detail::TaskState<void>;

  struct promise_type {
    std::shared_ptr<state_type> state = std::make_shared<state_type>();

    Task get_return_object() noexcept { return Task(state); }
    std::suspend_never initial_suspend() const noexcept { return {}; }
    std::suspend_never final_suspend() const noexcept { return {}; }
    ~promise_type() { state->complete(); }
    void return_void() const noexcept {}
    void unhandled_exception() noexcept {
      state->exception = std::current_exception();
    }
  };

  Task() = default;
  Task(Task &&) noexcept = default;
  Task &operator=(Task &&) noexcept = default;
  Task(const Task &) = delete;
  Task &operator=(const Task &) = delete;
  ~Task() = default;

  [[nodiscard]] bool ready() const noexcept {
    if (!state_) return true;
    std::lock_guard lock(state_->mutex);
    return state_->completed;
  }
  void get() {
    if (!state_) throw std::logic_error("Empty chhttp::Task");
    auto state = std::exchange(state_, {});
    {
      std::unique_lock lock(state->mutex);
      state->completed_cv.wait(lock, [&] { return state->completed; });
    }
    if (state->exception) std::rethrow_exception(state->exception);
  }
  struct Awaiter {
    std::shared_ptr<state_type> state;
    bool await_ready() const noexcept {
      if (!state) return true;
      std::lock_guard lock(state->mutex);
      return state->completed;
    }
    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      if (!state) return false;
      std::lock_guard lock(state->mutex);
      if (state->completed) return false;
      state->continuation = continuation;
      return true;
    }
    void await_resume() {
      if (!state) throw std::logic_error("Empty chhttp::Task");
      auto completed_state = std::exchange(state, {});
      if (completed_state->exception)
        std::rethrow_exception(completed_state->exception);
    }
  };
  Awaiter operator co_await() && noexcept {
    return Awaiter{std::exchange(state_, {})};
  }

private:
  explicit Task(std::shared_ptr<state_type> state) noexcept
      : state_(std::move(state)) {}
  std::shared_ptr<state_type> state_;
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

struct ErrorInfo {
  Error code{Error::none};
  std::string message;
  int system_code{0};
  long tls_code{0};

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

class StreamWriter;
using StreamHandler = std::function<Task<void>(StreamWriter &)>;

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
  void set_stream_body(StreamHandler handler,
                       std::optional<std::uint64_t> content_length = std::nullopt);
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

using AsyncBodyConsumer =
    std::function<Task<bool>(std::string_view)>;
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
  MultipartWriter &add_stream(std::string name, std::string filename,
                              std::string content_type,
                              StreamHandler producer,
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
std::pair<std::string, std::string>
make_multipart(const MultipartForm &parts, std::string boundary = {});
std::string mime_type(const std::filesystem::path &path);
std::string status_reason(int status);
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
