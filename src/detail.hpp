#pragma once

#include <chhttp/chhttp.hpp>

#include <uv.h>

#ifdef CHHTTP_HAS_TLS
#include <openssl/ssl.h>
#endif

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <deque>
#include <future>
#include <random>
#include <sstream>
#include <thread>

namespace chhttp::detail {

// Consuming protocol bytes advances a cursor. Compact only when appending
// requires space, so many tiny chunks/frames do not repeatedly move the tail.
class ReadBuffer {
public:
  ReadBuffer() = default;
  ReadBuffer(const ReadBuffer &) = delete;
  ReadBuffer &operator=(const ReadBuffer &) = delete;
  ReadBuffer(ReadBuffer &&other) noexcept
      : storage_(std::move(other.storage_)),
        offset_(std::exchange(other.offset_, 0)) {}
  ReadBuffer &operator=(ReadBuffer &&other) noexcept {
    if (this != &other) {
      storage_ = std::move(other.storage_);
      offset_ = std::exchange(other.offset_, 0);
    }
    return *this;
  }
  [[nodiscard]] std::string_view view() const noexcept {
    return std::string_view(storage_).substr(offset_);
  }
  [[nodiscard]] std::size_t size() const noexcept { return storage_.size() - offset_; }
  [[nodiscard]] bool empty() const noexcept { return size() == 0; }
  [[nodiscard]] const char *data() const noexcept { return storage_.data() + offset_; }
  char operator[](std::size_t index) const noexcept { return data()[index]; }
  std::size_t find(std::string_view text, std::size_t start = 0) const noexcept {
    return view().find(text, start);
  }
  std::size_t find(char ch, std::size_t start = 0) const noexcept {
    return view().find(ch, start);
  }
  std::string substr(std::size_t start, std::size_t count) const {
    return std::string(view().substr(start, count));
  }
  void consume(std::size_t count) noexcept {
    offset_ += count;
    if (offset_ == storage_.size()) clear();
  }
  void clear() noexcept {
    offset_ = 0;
    // Large WebSocket frames must not pin their entire allocation while idle.
    if (storage_.capacity() > 64 * 1024) std::string{}.swap(storage_);
    else storage_.clear();
  }
  void append(std::string bytes) {
    if (empty()) {
      storage_ = std::move(bytes);
      offset_ = 0;
      return;
    }
    if (offset_ >= storage_.size() / 2 ||
        bytes.size() > storage_.capacity() - storage_.size()) {
      storage_.erase(0, offset_);
      offset_ = 0;
    }
    storage_.append(bytes);
  }
  // Copy a borrowed input directly into reusable storage (no temporary string).
  void append_view(std::string_view bytes) {
    if (offset_ != 0 && (offset_ >= storage_.size() / 2 ||
        bytes.size() > storage_.capacity() - storage_.size())) {
      storage_.erase(0, offset_);
      offset_ = 0;
    }
    storage_.append(bytes);
  }

private:
  std::string storage_;
  std::size_t offset_{0};
};

class StreamError final : public std::runtime_error {
public:
  explicit StreamError(ErrorInfo error)
      : std::runtime_error(error.message), error_(std::move(error)) {}
  [[nodiscard]] const ErrorInfo &error() const noexcept { return error_; }
  [[nodiscard]] const char *what() const noexcept override {
    return error_.message.c_str();
  }

private:
  ErrorInfo error_;
};

struct ParsedUrl {
  std::string scheme;
  std::string host;
  std::uint16_t port{0};
  std::string target{"/"};
  bool secure{false};

  [[nodiscard]] std::string authority() const;
  [[nodiscard]] std::string origin() const;
};

Result<ParsedUrl> parse_url(std::string_view input, std::string_view base = {});
Result<std::string> resolve_url(std::string_view base,
                                std::string_view reference);
bool iequals(std::string_view lhs, std::string_view rhs) noexcept;
bool istarts_with(std::string_view value, std::string_view prefix) noexcept;
std::string lower(std::string_view value);
std::string trim(std::string_view value);
// Borrows the stored value so internal callers do not materialize a std::string
// for every header they merely inspect.
const std::string *find_header(const Headers &headers,
                               std::string_view name) noexcept;
std::vector<std::string> split_tokens(std::string_view value, char separator);
std::string random_boundary();
bool valid_header_name(std::string_view value) noexcept;
bool valid_header_value(std::string_view value) noexcept;
// Matches one token inside a comma-separated field value. Shared by has_token()
// and by callers that already hold the single value they need to inspect.
bool header_value_has_token(std::string_view value,
                            std::string_view token) noexcept;
bool has_token(const Headers &headers, std::string_view name,
               std::string_view token);

Result<std::string> compress(std::string_view input,
                             std::string_view encoding);
Result<std::string> decompress(std::string_view input,
                               std::string_view encoding,
                               std::size_t max_output);
std::string select_encoding(std::string_view accept_encoding);
// Borrowed form of the public status_reason() so the response serializer can
// append the reason phrase without materialising a temporary string.
std::string_view status_reason_view(int status) noexcept;

std::string base64_encode(std::string_view value);
Result<std::string> base64_decode(std::string_view value);
std::string sha1_base64(std::string_view value);
std::string websocket_accept(std::string_view key);

ErrorInfo make_error(Error code, std::string message, int system_code = 0,
                     long tls_code = 0);
bool path_is_within(const std::filesystem::path &root,
                    const std::filesystem::path &candidate);
// Variant for roots that are already canonical, which avoids resolving the same
// root again for every candidate checked against it.
bool path_is_within_canonical(const std::filesystem::path &canonical_root,
                              const std::filesystem::path &candidate);

class DetachedTask {
public:
  struct promise_type {
    DetachedTask get_return_object() const noexcept { return {}; }
    std::suspend_never initial_suspend() const noexcept { return {}; }
    std::suspend_never final_suspend() const noexcept { return {}; }
    void return_void() const noexcept {}
    void unhandled_exception() const noexcept {}
  };
};

inline DetachedTask detach_task(Task<void> task) {
  co_await std::move(task);
}

class Runtime : public std::enable_shared_from_this<Runtime> {
public:
  Runtime();
  ~Runtime();
  Runtime(const Runtime &) = delete;
  Runtime &operator=(const Runtime &) = delete;

  // Callbacks that are already running on this runtime's loop are invoked
  // directly, so the common on-loop path never builds a type-erased
  // std::function or touches the cross-thread queue.
  template <class Callback> void post(Callback &&callback) {
    if (on_loop_thread()) {
      std::forward<Callback>(callback)();
      return;
    }
    post_remote(std::function<void()>(std::forward<Callback>(callback)));
  }
  void stop();
  [[nodiscard]] bool on_loop_thread() const noexcept;
  [[nodiscard]] uv_loop_t *loop() noexcept { return &loop_; }

private:
  static void async_callback(uv_async_t *handle);
  void drain();
  void post_remote(std::function<void()> callback);

  uv_loop_t loop_{};
  uv_async_t async_{};
  std::mutex queue_mutex_;
  std::deque<std::function<void()>> queue_;
  // Loop-thread-only scratch for the batch currently being drained.
  std::deque<std::function<void()>> drain_scratch_;
  std::thread thread_;
  std::thread::id thread_id_{};
  std::promise<void> ready_;
  std::atomic_bool stopping_{false};
};

Runtime *current_runtime() noexcept;
void stop_runtime(std::shared_ptr<Runtime> runtime) noexcept;

// Hands the current coroutine back to a runtime's loop. This is a plain awaiter
// rather than a Task so that the common no-op case (the coroutine already runs
// on the target loop) costs neither a coroutine frame nor a TaskState
// allocation.
class ResumeOnAwaiter {
public:
  explicit ResumeOnAwaiter(std::shared_ptr<Runtime> runtime)
      : runtime_(std::move(runtime)) {}
  [[nodiscard]] bool await_ready() const noexcept {
    return runtime_->on_loop_thread();
  }
  void await_suspend(std::coroutine_handle<> continuation) {
    runtime_->post([continuation] { continuation.resume(); });
  }
  void await_resume() const noexcept {}

private:
  std::shared_ptr<Runtime> runtime_;
};

inline ResumeOnAwaiter resume_on(std::shared_ptr<Runtime> runtime) {
  return ResumeOnAwaiter(std::move(runtime));
}

// Run blocking filesystem work on a bounded pool, then return to the caller's
// I/O loop when invoked from one. Exceptions propagate through the Task.
Task<void> run_file_io(std::function<void()> operation);

#ifdef CHHTTP_HAS_TLS
SSL_CTX *create_client_tls_context(const TlsClientOptions &options,
                                   ErrorInfo &error);
SSL_CTX *create_server_tls_context(const TlsServerOptions &options,
                                   ErrorInfo &error);
#endif

class Connection : public std::enable_shared_from_this<Connection> {
public:
  // A received byte range that owns its storage. The transport hands over the
  // block the kernel filled, so consumers that only view the bytes pay no
  // payload copy and the bytes stay valid after the read callback returns.
  class ReadChunk {
  public:
    ReadChunk() = default;
    ReadChunk(std::unique_ptr<char[]> storage, std::size_t size)
        : storage_(std::move(storage)), size_(size) {}
    ReadChunk(ReadChunk &&) noexcept = default;
    ReadChunk &operator=(ReadChunk &&) noexcept = default;
    ReadChunk(const ReadChunk &) = delete;
    ReadChunk &operator=(const ReadChunk &) = delete;

    // The transport reached end of stream, so no storage is involved.
    [[nodiscard]] static ReadChunk closed() {
      ReadChunk chunk;
      chunk.eof = true;
      return chunk;
    }
    // Copies a borrowed range into owned storage; used where the transport
    // cannot hand its own block over (TLS decrypts into a stack buffer).
    [[nodiscard]] static ReadChunk copy_of(std::string_view bytes) {
      ReadChunk chunk;
      if (!bytes.empty()) {
        chunk.storage_.reset(new char[bytes.size()]);
        std::memcpy(chunk.storage_.get(), bytes.data(), bytes.size());
      }
      chunk.size_ = bytes.size();
      return chunk;
    }

    [[nodiscard]] std::string_view view() const noexcept {
      return std::string_view(storage_.get(), size_);
    }
    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
    [[nodiscard]] const char *data() const noexcept { return storage_.get(); }

    bool eof{false};

  private:
    std::unique_ptr<char[]> storage_;
    std::size_t size_{0};
  };

  static Task<Result<std::shared_ptr<Connection>>>
  connect(std::shared_ptr<Runtime> runtime, std::string host,
          std::uint16_t port, std::chrono::milliseconds timeout,
          std::function<void(const std::shared_ptr<Connection> &)> on_created = {});
  static std::shared_ptr<Connection>
  accept(std::shared_ptr<Runtime> runtime, uv_stream_t *listener);

  ~Connection();
  Connection(const Connection &) = delete;
  Connection &operator=(const Connection &) = delete;

#ifdef CHHTTP_HAS_TLS
  ErrorInfo enable_tls(SSL_CTX *context, bool server,
                       std::string_view server_name = {});
  Task<ErrorInfo> handshake(std::chrono::milliseconds timeout);
#endif

  Task<Result<ReadChunk>> read(std::chrono::milliseconds timeout);
  Task<ErrorInfo> write(std::string data, std::chrono::milliseconds timeout);
  ErrorInfo set_no_delay(bool enabled);
  void close();
  [[nodiscard]] bool open() const noexcept;
  [[nodiscard]] bool secure() const noexcept;
  [[nodiscard]] std::string remote_address() const;
  [[nodiscard]] std::uint16_t remote_port() const noexcept;
  [[nodiscard]] std::shared_ptr<Runtime> runtime() const { return runtime_; }

public: // Internal transport implementation; not exposed by the public API.
  explicit Connection(std::shared_ptr<Runtime> runtime);
  struct PendingRead;
  struct PendingWrite;
  struct PendingConnect;

  Task<Result<ReadChunk>> raw_read(std::chrono::milliseconds timeout);
  Task<ErrorInfo> raw_write(std::string data,
                            std::chrono::milliseconds timeout);
  void close_on_loop();
  void finish_connect(Result<std::shared_ptr<Connection>> result);
  void finish_read(Result<ReadChunk> result);
  void finish_write(ErrorInfo error);

  static void allocate(uv_handle_t *, std::size_t, uv_buf_t *);
  static void read_callback(uv_stream_t *, ssize_t, const uv_buf_t *);
  static void write_callback(uv_write_t *, int);
  static void close_callback(uv_handle_t *);
  static void read_timeout(uv_timer_t *);
  static void write_timeout(uv_timer_t *);

  std::shared_ptr<Runtime> runtime_;
  std::shared_ptr<Connection> self_keep_;
  uv_tcp_t tcp_{};
  uv_timer_t read_timer_{};
  uv_timer_t write_timer_{};
  bool tcp_initialized_{false};
  bool read_timer_initialized_{false};
  bool write_timer_initialized_{false};
  bool closing_{false};
  std::atomic_bool open_{false};
  bool read_started_{false};
  int closing_handles_{0};
  std::shared_ptr<PendingConnect> pending_connect_;
  std::shared_ptr<PendingRead> pending_read_;
  std::shared_ptr<PendingWrite> pending_write_;
  std::string remote_address_;
  std::uint16_t remote_port_{0};

#ifdef CHHTTP_HAS_TLS
  // Scratch blocks for the TLS paths. Keeping them on the connection instead of
  // in a coroutine frame matters a lot: a frame that holds a local live across a
  // suspension reserves that space for the whole suspension, and the read path is
  // suspended whenever the peer is quiet. They are allocated only for connections
  // that actually enable TLS.
  static constexpr std::size_t tls_scratch_size = 16 * 1024;
  SSL *ssl_{nullptr};
  bool tls_server_{false};
  std::unique_ptr<char[]> tls_read_scratch_;
  std::unique_ptr<char[]> tls_flush_scratch_;
  [[nodiscard]] char *tls_read_scratch() noexcept {
    return tls_read_scratch_.get();
  }
  [[nodiscard]] char *tls_flush_scratch() noexcept {
    return tls_flush_scratch_.get();
  }
  Task<ErrorInfo> flush_tls(std::chrono::milliseconds timeout);
#endif
};

class Listener : public std::enable_shared_from_this<Listener> {
public:
  using AcceptCallback = std::function<void(std::shared_ptr<Connection>)>;

  static Result<std::shared_ptr<Listener>>
  create(std::shared_ptr<Runtime> runtime, std::string host, std::uint16_t port,
         int backlog, AcceptCallback callback, bool reuse_address);
  ~Listener();
  void close();
  [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

private:
  explicit Listener(std::shared_ptr<Runtime> runtime);
  static void connection_callback(uv_stream_t *, int);
  static void close_callback(uv_handle_t *);

  std::shared_ptr<Runtime> runtime_;
  std::shared_ptr<Listener> self_keep_;
  uv_tcp_t tcp_{};
  AcceptCallback callback_;
  std::uint16_t port_{0};
  bool initialized_{false};
  bool closing_{false};
};

struct HttpReadOptions {
  std::size_t max_header_size{64 * 1024};
  std::size_t max_body_size{128 * 1024 * 1024};
  std::chrono::milliseconds read_timeout{60s};
  std::optional<std::chrono::milliseconds> header_timeout;
  std::optional<std::chrono::milliseconds> first_body_byte_timeout;
  std::optional<std::chrono::milliseconds> idle_timeout;
  std::optional<std::chrono::steady_clock::time_point> deadline;
  bool auto_decompress{false};
  std::function<bool()> cancelled;
  std::function<bool(const ResponseHead &)> on_response_head;
  std::function<bool(std::string_view)> on_data;
  std::function<Task<bool>(std::string_view)> on_data_async;
  ProgressHandler on_progress;
};

struct RequestBodyState {
  Headers headers;
  std::optional<std::uint64_t> content_length;
  bool chunked{false};
  bool expect_continue{false};
  bool continue_sent{false};
};

struct RequestHeadResult {
  Request request;
  RequestBodyState body;
};

// `duplicate_headers` mirrors the parsed fields onto `Request::headers`. Callers
// that go on to take ownership of `RequestBodyState::headers` can pass false and
// avoid a second copy of every field.
Task<Result<RequestHeadResult>>
read_request_head(const std::shared_ptr<Connection> &connection,
                  ReadBuffer &buffer, const HttpReadOptions &options,
                  bool duplicate_headers = true);
Task<Result<std::string>>
read_request_body(const std::shared_ptr<Connection> &connection,
                  ReadBuffer &buffer, RequestBodyState &state,
                  const HttpReadOptions &options);

Task<Result<Request>> read_request(const std::shared_ptr<Connection> &connection,
                                   ReadBuffer &buffer,
                                   const HttpReadOptions &options);
Task<ResponseResult> read_response(
    const std::shared_ptr<Connection> &connection, ReadBuffer &buffer,
    std::string_view request_method, const HttpReadOptions &options);
Task<ErrorInfo> write_request(const std::shared_ptr<Connection> &connection,
                              const Request &request,
                              std::string_view wire_target,
                              std::chrono::milliseconds timeout,
                              ProgressHandler on_upload_progress = {});
Task<ErrorInfo> write_response(const std::shared_ptr<Connection> &connection,
                               const Request &request, const Response &response,
                               std::chrono::milliseconds timeout);
Task<ErrorInfo>
write_response_head(const std::shared_ptr<Connection> &connection,
                    const Request &request, const Response &response,
                    bool chunked, std::optional<std::uint64_t> content_length,
                    std::chrono::milliseconds timeout);
Task<ErrorInfo> write_chunk(const std::shared_ptr<Connection> &connection,
                            std::string_view data,
                            std::chrono::milliseconds timeout);
Task<ErrorInfo> write_last_chunk(
    const std::shared_ptr<Connection> &connection,
    std::chrono::milliseconds timeout);

bool is_websocket_upgrade(const Request &request);
Task<ErrorInfo> websocket_server_handshake(
    const std::shared_ptr<Connection> &connection, const Request &request,
    std::string_view subprotocol, std::chrono::milliseconds timeout);
Task<Result<std::shared_ptr<WebSocket>>> websocket_client_connect(
    std::shared_ptr<Runtime> runtime, std::string url, Headers headers,
    ClientOptions options);
std::shared_ptr<WebSocket::Channel> make_websocket_channel(
    std::shared_ptr<Connection> connection, ReadBuffer buffered,
    bool client_side, std::string subprotocol,
    std::chrono::milliseconds timeout);

} // namespace chhttp::detail

namespace chhttp {
struct ServerAccess {
  static std::optional<std::filesystem::path> &file(Response &response) {
    return response.file_path_;
  }
  static StreamHandler &stream(Response &response) {
    return response.stream_handler_;
  }
};
} // namespace chhttp
