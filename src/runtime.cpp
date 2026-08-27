#include "detail.hpp"

#ifdef CHHTTP_HAS_TLS
#include <openssl/err.h>
#include <openssl/x509v3.h>
#endif

#include <cerrno>
#include <cstring>

namespace chhttp::detail {
namespace {

thread_local Runtime *active_runtime = nullptr;

std::uint64_t timeout_ms(std::chrono::milliseconds value) {
  return static_cast<std::uint64_t>(std::max<std::int64_t>(1, value.count()));
}

std::string uv_message(int status) {
  const char *message = uv_strerror(status);
  return message ? std::string(message) : std::string("libuv error");
}

} // namespace

Runtime::Runtime() {
  const int loop_status = uv_loop_init(&loop_);
  if (loop_status != 0)
    throw std::runtime_error("uv_loop_init failed: " + uv_message(loop_status));
  async_.data = this;
  const int async_status = uv_async_init(&loop_, &async_, &Runtime::async_callback);
  if (async_status != 0) {
    uv_loop_close(&loop_);
    throw std::runtime_error("uv_async_init failed: " + uv_message(async_status));
  }
  auto ready = ready_.get_future();
  thread_ = std::thread([this] {
    thread_id_ = std::this_thread::get_id();
    active_runtime = this;
    ready_.set_value();
    uv_run(&loop_, UV_RUN_DEFAULT);
    active_runtime = nullptr;
  });
  ready.wait();
}

Runtime::~Runtime() { stop(); }

void Runtime::post(std::function<void()> callback) {
  if (!callback) return;
  if (on_loop_thread()) {
    callback();
    return;
  }
  {
    std::lock_guard lock(queue_mutex_);
    if (stopping_) return;
    queue_.push_back(std::move(callback));
  }
  uv_async_send(&async_);
}

void Runtime::async_callback(uv_async_t *handle) {
  static_cast<Runtime *>(handle->data)->drain();
}

void Runtime::drain() {
  std::deque<std::function<void()>> callbacks;
  {
    std::lock_guard lock(queue_mutex_);
    callbacks.swap(queue_);
  }
  for (auto &callback : callbacks) {
    try {
      callback();
    } catch (...) {
    }
  }
}

void Runtime::stop() {
  if (stopping_.exchange(true)) {
    if (thread_.joinable() && !on_loop_thread()) thread_.join();
    return;
  }
  {
    std::lock_guard lock(queue_mutex_);
    queue_.push_back([this] {
      if (!uv_is_closing(reinterpret_cast<uv_handle_t *>(&async_)))
        uv_close(reinterpret_cast<uv_handle_t *>(&async_), nullptr);
    });
  }
  uv_async_send(&async_);
  if (thread_.joinable() && !on_loop_thread()) thread_.join();
  uv_loop_close(&loop_);
}

bool Runtime::on_loop_thread() const noexcept {
  return thread_id_ == std::this_thread::get_id();
}

Runtime *current_runtime() noexcept { return active_runtime; }

void stop_runtime(std::shared_ptr<Runtime> runtime) noexcept {
  if (!runtime) return;
  if (!runtime->on_loop_thread()) {
    runtime->stop();
    return;
  }
  try {
    auto shutdown_runtime = runtime;
    std::thread([shutdown_runtime = std::move(shutdown_runtime)] {
      shutdown_runtime->stop();
    }).detach();
    runtime.reset();
  } catch (...) {
    // Retain the runtime if the shutdown handoff cannot allocate a thread. This
    // is preferable to destroying a live libuv loop from its own callback.
    static std::mutex retained_mutex;
    static std::vector<std::shared_ptr<Runtime>> retained;
    std::lock_guard lock(retained_mutex);
    retained.push_back(std::move(runtime));
  }
}

struct Connection::PendingRead {
  std::coroutine_handle<> continuation;
  Result<ReadChunk> result{ErrorInfo{Error::internal, "Read not completed"}};
  bool completed{false};
};

struct Connection::PendingWrite {
  uv_write_t request{};
  std::shared_ptr<PendingWrite> self_keep;
  std::coroutine_handle<> continuation;
  std::string data;
  ErrorInfo result{Error::internal, "Write not completed"};
  bool completed{false};
  Connection *connection{nullptr};
};

struct Connection::PendingConnect {
  uv_getaddrinfo_t resolver{};
  uv_connect_t connector{};
  std::shared_ptr<PendingConnect> self_keep;
  std::shared_ptr<Connection> connection;
  std::coroutine_handle<> continuation;
  Result<std::shared_ptr<Connection>> result{
      ErrorInfo{Error::internal, "Connect not completed"}};
  std::string host;
  std::string service;
  bool completed{false};
  bool resolving{true};
};

Connection::Connection(std::shared_ptr<Runtime> runtime)
    : runtime_(std::move(runtime)) {}

Connection::~Connection() {
#ifdef CHHTTP_HAS_TLS
  if (ssl_) SSL_free(ssl_);
#endif
}

namespace {

struct ConnectAwaiter {
  std::shared_ptr<Connection> connection;
  std::shared_ptr<Connection::PendingConnect> pending;
  std::chrono::milliseconds timeout;

  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> continuation) {
    pending->continuation = continuation;
    auto connection_copy = connection;
    auto pending_copy = pending;
    connection->runtime()->post([connection_copy, pending_copy,
                                 timeout = timeout] {
      auto *connection = connection_copy.get();
      connection->self_keep_ = connection_copy;
      int status = uv_tcp_init(connection->runtime_->loop(), &connection->tcp_);
      if (status == 0) {
        connection->tcp_initialized_ = true;
        connection->tcp_.data = connection;
        status = uv_timer_init(connection->runtime_->loop(),
                               &connection->read_timer_);
      }
      if (status == 0) {
        connection->read_timer_initialized_ = true;
        connection->read_timer_.data = connection;
        status = uv_timer_init(connection->runtime_->loop(),
                               &connection->write_timer_);
      }
      if (status == 0) {
        connection->write_timer_initialized_ = true;
        connection->write_timer_.data = connection;
      }
      if (status != 0) {
        connection->pending_connect_ = pending_copy;
        connection->finish_connect(ErrorInfo{Error::connect,
                                              "Unable to initialize TCP: " +
                                                  uv_message(status),
                                              status});
        return;
      }
      connection->pending_connect_ = pending_copy;
      pending_copy->self_keep = pending_copy;
      pending_copy->resolver.data = pending_copy.get();
      pending_copy->connector.data = pending_copy.get();
      uv_timer_start(&connection->read_timer_, &Connection::read_timeout,
                     timeout_ms(timeout), 0);

      addrinfo hints{};
      hints.ai_family = AF_UNSPEC;
      hints.ai_socktype = SOCK_STREAM;
      hints.ai_protocol = IPPROTO_TCP;
      const int resolve_status = uv_getaddrinfo(
          connection->runtime_->loop(), &pending_copy->resolver,
          [](uv_getaddrinfo_t *request, int resolve_status, addrinfo *addresses) {
            auto *pending = static_cast<Connection::PendingConnect *>(request->data);
            auto self = pending->self_keep;
            auto connection = pending->connection;
            pending->resolving = false;
            if (pending->completed) {
              if (addresses) uv_freeaddrinfo(addresses);
              pending->self_keep.reset();
              return;
            }
            if (resolve_status != 0 || !addresses) {
              if (addresses) uv_freeaddrinfo(addresses);
              connection->finish_connect(ErrorInfo{
                  Error::resolve,
                  "Unable to resolve " + pending->host + ": " +
                      uv_message(resolve_status),
                  resolve_status});
              pending->self_keep.reset();
              return;
            }
            const int connect_status = uv_tcp_connect(
                &pending->connector, &connection->tcp_, addresses->ai_addr,
                [](uv_connect_t *request, int status) {
                  auto *pending =
                      static_cast<Connection::PendingConnect *>(request->data);
                  auto self = pending->self_keep;
                  auto connection = pending->connection;
                  if (!pending->completed) {
                    if (status == 0)
                      connection->finish_connect(connection);
                    else
                      connection->finish_connect(ErrorInfo{
                          Error::connect,
                          "Unable to connect to " + pending->host + ": " +
                              uv_message(status),
                          status});
                  }
                  pending->self_keep.reset();
                });
            uv_freeaddrinfo(addresses);
            if (connect_status != 0) {
              connection->finish_connect(ErrorInfo{
                  Error::connect,
                  "Unable to start connection: " + uv_message(connect_status),
                  connect_status});
              pending->self_keep.reset();
            }
          },
          pending_copy->host.c_str(), pending_copy->service.c_str(), &hints);
      if (resolve_status != 0) {
        connection->finish_connect(ErrorInfo{
            Error::resolve,
            "Unable to start DNS resolution: " + uv_message(resolve_status),
            resolve_status});
        pending_copy->self_keep.reset();
      }
    });
  }
  Result<std::shared_ptr<Connection>> await_resume() {
    return std::move(pending->result);
  }
};

} // namespace

Task<Result<std::shared_ptr<Connection>>>
Connection::connect(std::shared_ptr<Runtime> runtime, std::string host,
                    std::uint16_t port,
                    std::chrono::milliseconds timeout) {
  auto connection = std::shared_ptr<Connection>(new Connection(std::move(runtime)));
  auto pending = std::make_shared<PendingConnect>();
  pending->connection = connection;
  pending->host = std::move(host);
  pending->service = std::to_string(port);
  co_return co_await ConnectAwaiter{connection, pending, timeout};
}

void Connection::finish_connect(Result<std::shared_ptr<Connection>> result) {
  auto pending = std::exchange(pending_connect_, {});
  if (!pending || pending->completed) return;
  pending->completed = true;
  if (read_timer_initialized_) uv_timer_stop(&read_timer_);
  pending->result = std::move(result);
  auto continuation = pending->continuation;
  if (pending->result)
    open_.store(true, std::memory_order_release);
  else
    close_on_loop();
  continuation.resume();
}

std::shared_ptr<Connection>
Connection::accept(std::shared_ptr<Runtime> runtime, uv_stream_t *listener) {
  auto connection = std::shared_ptr<Connection>(new Connection(std::move(runtime)));
  connection->self_keep_ = connection;
  int status = uv_tcp_init(connection->runtime_->loop(), &connection->tcp_);
  if (status == 0) {
    connection->tcp_initialized_ = true;
    connection->tcp_.data = connection.get();
    status = uv_timer_init(connection->runtime_->loop(), &connection->read_timer_);
  }
  if (status == 0) {
    connection->read_timer_initialized_ = true;
    connection->read_timer_.data = connection.get();
    status = uv_timer_init(connection->runtime_->loop(), &connection->write_timer_);
  }
  if (status == 0) {
    connection->write_timer_initialized_ = true;
    connection->write_timer_.data = connection.get();
  }
  if (status != 0) {
    connection->close_on_loop();
    return {};
  }
  if (uv_accept(listener, reinterpret_cast<uv_stream_t *>(&connection->tcp_)) !=
      0) {
    connection->close_on_loop();
    return {};
  }
  connection->open_.store(true, std::memory_order_release);
  sockaddr_storage peer{};
  int peer_size = sizeof(peer);
  if (uv_tcp_getpeername(&connection->tcp_, reinterpret_cast<sockaddr *>(&peer),
                         &peer_size) == 0) {
    char address[INET6_ADDRSTRLEN]{};
    if (peer.ss_family == AF_INET) {
      auto *value = reinterpret_cast<sockaddr_in *>(&peer);
      uv_ip4_name(value, address, sizeof(address));
      connection->remote_port_ = ntohs(value->sin_port);
    } else if (peer.ss_family == AF_INET6) {
      auto *value = reinterpret_cast<sockaddr_in6 *>(&peer);
      uv_ip6_name(value, address, sizeof(address));
      connection->remote_port_ = ntohs(value->sin6_port);
    }
    connection->remote_address_ = address;
  }
  return connection;
}

namespace {

struct ReadAwaiter {
  std::shared_ptr<Connection> connection;
  std::shared_ptr<Connection::PendingRead> pending;
  std::chrono::milliseconds timeout;
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> continuation) {
    pending->continuation = continuation;
    auto connection_copy = connection;
    auto pending_copy = pending;
    connection->runtime()->post([connection_copy, pending_copy,
                                 timeout = timeout] {
      auto *connection = connection_copy.get();
      if (!connection->open()) {
        pending_copy->result = Connection::ReadChunk{{}, true};
        pending_copy->completed = true;
        pending_copy->continuation.resume();
        return;
      }
      if (connection->pending_read_) {
        pending_copy->result = ErrorInfo{Error::read,
                                         "Concurrent reads are not supported"};
        pending_copy->completed = true;
        pending_copy->continuation.resume();
        return;
      }
      connection->pending_read_ = pending_copy;
      uv_timer_start(&connection->read_timer_, &Connection::read_timeout,
                     timeout_ms(timeout), 0);
      if (!connection->read_started_) {
        const int status = uv_read_start(
            reinterpret_cast<uv_stream_t *>(&connection->tcp_),
            &Connection::allocate, &Connection::read_callback);
        if (status != 0) {
          connection->finish_read(ErrorInfo{
              Error::read, "Unable to start read: " + uv_message(status), status});
          return;
        }
        connection->read_started_ = true;
      }
    });
  }
  Result<Connection::ReadChunk> await_resume() {
    return std::move(pending->result);
  }
};

struct WriteAwaiter {
  std::shared_ptr<Connection> connection;
  std::shared_ptr<Connection::PendingWrite> pending;
  std::chrono::milliseconds timeout;
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> continuation) {
    pending->continuation = continuation;
    auto connection_copy = connection;
    auto pending_copy = pending;
    connection->runtime()->post([connection_copy, pending_copy,
                                 timeout = timeout] {
      auto *connection = connection_copy.get();
      if (!connection->open()) {
        pending_copy->result = ErrorInfo{Error::write, "Connection is closed"};
        pending_copy->completed = true;
        pending_copy->continuation.resume();
        return;
      }
      if (connection->pending_write_) {
        pending_copy->result = ErrorInfo{Error::write,
                                         "Concurrent writes are not supported"};
        pending_copy->completed = true;
        pending_copy->continuation.resume();
        return;
      }
      connection->pending_write_ = pending_copy;
      pending_copy->connection = connection;
      pending_copy->self_keep = pending_copy;
      pending_copy->request.data = pending_copy.get();
      uv_buf_t buffer = uv_buf_init(pending_copy->data.data(),
                                    static_cast<unsigned int>(pending_copy->data.size()));
      uv_timer_start(&connection->write_timer_, &Connection::write_timeout,
                     timeout_ms(timeout), 0);
      const int status = uv_write(
          &pending_copy->request,
          reinterpret_cast<uv_stream_t *>(&connection->tcp_), &buffer, 1,
          &Connection::write_callback);
      if (status != 0) {
        connection->finish_write(ErrorInfo{
            Error::write, "Unable to start write: " + uv_message(status), status});
        pending_copy->self_keep.reset();
      }
    });
  }
  ErrorInfo await_resume() { return std::move(pending->result); }
};

} // namespace

Task<Result<Connection::ReadChunk>>
Connection::raw_read(std::chrono::milliseconds timeout) {
  auto pending = std::make_shared<PendingRead>();
  co_return co_await ReadAwaiter{shared_from_this(), pending, timeout};
}

Task<ErrorInfo> Connection::raw_write(std::string data,
                                      std::chrono::milliseconds timeout) {
  if (data.empty()) co_return ErrorInfo{};
  auto pending = std::make_shared<PendingWrite>();
  pending->data = std::move(data);
  co_return co_await WriteAwaiter{shared_from_this(), pending, timeout};
}

void Connection::allocate(uv_handle_t *, std::size_t suggested, uv_buf_t *buf) {
  const auto size = std::max<std::size_t>(suggested, 16 * 1024);
  buf->base = static_cast<char *>(std::malloc(size));
  buf->len = static_cast<decltype(buf->len)>(size);
}

void Connection::read_callback(uv_stream_t *stream, ssize_t count,
                               const uv_buf_t *buffer) {
  auto *connection = static_cast<Connection *>(stream->data);
  std::unique_ptr<char, decltype(&std::free)> storage(buffer->base, &std::free);
  if (count > 0) {
    connection->finish_read(ReadChunk{
        std::string(buffer->base, static_cast<std::size_t>(count)), false});
  } else if (count == UV_EOF) {
    connection->finish_read(ReadChunk{{}, true});
  } else if (count < 0) {
    connection->finish_read(ErrorInfo{
        Error::read, "Socket read failed: " + uv_message(static_cast<int>(count)),
        static_cast<int>(count)});
  }
}

void Connection::finish_read(Result<ReadChunk> result) {
  auto pending = std::exchange(pending_read_, {});
  if (!pending || pending->completed) return;
  pending->completed = true;
  uv_timer_stop(&read_timer_);
  if (read_started_) {
    uv_read_stop(reinterpret_cast<uv_stream_t *>(&tcp_));
    read_started_ = false;
  }
  pending->result = std::move(result);
  pending->continuation.resume();
}

void Connection::write_callback(uv_write_t *request, int status) {
  auto *pending = static_cast<PendingWrite *>(request->data);
  auto self = pending->self_keep;
  if (!pending->completed) {
    if (status == 0)
      pending->connection->finish_write(ErrorInfo{});
    else
      pending->connection->finish_write(ErrorInfo{
          Error::write, "Socket write failed: " + uv_message(status), status});
  }
  pending->self_keep.reset();
}

void Connection::finish_write(ErrorInfo error) {
  auto pending = std::exchange(pending_write_, {});
  if (!pending || pending->completed) return;
  pending->completed = true;
  uv_timer_stop(&write_timer_);
  pending->result = std::move(error);
  pending->continuation.resume();
}

void Connection::read_timeout(uv_timer_t *timer) {
  auto *connection = static_cast<Connection *>(timer->data);
  if (connection->pending_connect_) {
    auto pending = connection->pending_connect_;
    if (pending->resolving) uv_cancel(reinterpret_cast<uv_req_t *>(&pending->resolver));
    connection->finish_connect(
        ErrorInfo{Error::timeout, "Connection attempt timed out", UV_ETIMEDOUT});
  } else {
    connection->finish_read(
        ErrorInfo{Error::timeout, "Socket read timed out", UV_ETIMEDOUT});
  }
  connection->close_on_loop();
}

void Connection::write_timeout(uv_timer_t *timer) {
  auto *connection = static_cast<Connection *>(timer->data);
  connection->finish_write(
      ErrorInfo{Error::timeout, "Socket write timed out", UV_ETIMEDOUT});
  connection->close_on_loop();
}

Task<Result<Connection::ReadChunk>>
Connection::read(std::chrono::milliseconds timeout) {
#ifdef CHHTTP_HAS_TLS
  if (ssl_) {
    for (;;) {
      std::array<char, 16 * 1024> output{};
      std::size_t count = 0;
      const int status = SSL_read_ex(ssl_, output.data(), output.size(), &count);
      const int ssl_error =
          status == 1 ? SSL_ERROR_NONE : SSL_get_error(ssl_, status);
      const auto tls_code =
          ssl_error != SSL_ERROR_NONE && ssl_error != SSL_ERROR_WANT_READ &&
                  ssl_error != SSL_ERROR_WANT_WRITE &&
                  ssl_error != SSL_ERROR_ZERO_RETURN
              ? ERR_get_error()
              : 0;
      auto flushed = co_await flush_tls(timeout);
      if (flushed) co_return flushed;
      if (status == 1 && count > 0)
        co_return ReadChunk{std::string(output.data(), count), false};
      if (ssl_error == SSL_ERROR_ZERO_RETURN) co_return ReadChunk{{}, true};
      if (ssl_error != SSL_ERROR_WANT_READ && ssl_error != SSL_ERROR_WANT_WRITE) {
        co_return ErrorInfo{Error::read, "TLS read failed", ssl_error,
                            static_cast<long>(tls_code)};
      }
      if (ssl_error == SSL_ERROR_WANT_READ) {
        auto encrypted = co_await raw_read(timeout);
        if (!encrypted) co_return encrypted.error();
        if (encrypted->eof) co_return ReadChunk{{}, true};
        if (BIO_write(SSL_get_rbio(ssl_), encrypted->data.data(),
                      static_cast<int>(encrypted->data.size())) <= 0)
          co_return ErrorInfo{Error::read, "Unable to feed TLS input"};
      }
    }
  }
#endif
  co_return co_await raw_read(timeout);
}

Task<ErrorInfo> Connection::write(std::string data,
                                  std::chrono::milliseconds timeout) {
#ifdef CHHTTP_HAS_TLS
  if (ssl_) {
    std::size_t offset = 0;
    while (offset < data.size()) {
      std::size_t count = 0;
      const int status = SSL_write_ex(ssl_, data.data() + offset,
                                      data.size() - offset, &count);
      const int ssl_error =
          status == 1 ? SSL_ERROR_NONE : SSL_get_error(ssl_, status);
      const auto tls_code =
          ssl_error != SSL_ERROR_NONE && ssl_error != SSL_ERROR_WANT_READ &&
                  ssl_error != SSL_ERROR_WANT_WRITE
              ? ERR_get_error()
              : 0;
      auto flushed = co_await flush_tls(timeout);
      if (flushed) co_return flushed;
      if (status == 1) {
        offset += count;
        continue;
      }
      if (ssl_error == SSL_ERROR_WANT_READ) {
        auto encrypted = co_await raw_read(timeout);
        if (!encrypted) co_return encrypted.error();
        if (encrypted->eof)
          co_return ErrorInfo{Error::write, "TLS peer closed during write"};
        if (BIO_write(SSL_get_rbio(ssl_), encrypted->data.data(),
                      static_cast<int>(encrypted->data.size())) <= 0)
          co_return ErrorInfo{Error::write, "Unable to feed TLS input"};
        continue;
      }
      if (ssl_error == SSL_ERROR_WANT_WRITE) continue;
      co_return ErrorInfo{Error::write, "TLS write failed", ssl_error,
                          static_cast<long>(tls_code)};
    }
    co_return co_await flush_tls(timeout);
  }
#endif
  co_return co_await raw_write(std::move(data), timeout);
}

ErrorInfo Connection::set_no_delay(bool enabled) {
  if (!runtime_->on_loop_thread())
    return ErrorInfo{Error::internal,
                     "TCP options must be set on the owning libuv loop"};
  const int status = uv_tcp_nodelay(&tcp_, enabled ? 1 : 0);
  if (status != 0)
    return ErrorInfo{Error::connect,
                     "Unable to configure TCP_NODELAY: " + uv_message(status),
                     status};
  return {};
}

#ifdef CHHTTP_HAS_TLS
ErrorInfo Connection::enable_tls(SSL_CTX *context, bool server,
                                 std::string_view server_name) {
  if (!runtime_->on_loop_thread())
    return ErrorInfo{Error::tls_configuration,
                     "TLS must be initialized on the owning libuv loop"};
  ssl_ = SSL_new(context);
  if (!ssl_)
    return ErrorInfo{Error::tls_configuration, "Unable to allocate TLS state",
                     0, static_cast<long>(ERR_get_error())};
  BIO *input = BIO_new(BIO_s_mem());
  BIO *output = BIO_new(BIO_s_mem());
  if (!input || !output) {
    if (input) BIO_free(input);
    if (output) BIO_free(output);
    SSL_free(ssl_);
    ssl_ = nullptr;
    return ErrorInfo{Error::tls_configuration, "Unable to allocate TLS BIO"};
  }
  BIO_set_mem_eof_return(input, -1);
  BIO_set_mem_eof_return(output, -1);
  SSL_set_bio(ssl_, input, output);
  tls_server_ = server;
  if (server) {
    SSL_set_accept_state(ssl_);
  } else {
    SSL_set_connect_state(ssl_);
    if (!server_name.empty()) {
      const std::string name(server_name);
      std::array<unsigned char, 16> address{};
      const bool is_ip_address =
          uv_inet_pton(AF_INET, name.c_str(), address.data()) == 0 ||
          uv_inet_pton(AF_INET6, name.c_str(), address.data()) == 0;
      if (!is_ip_address &&
          SSL_set_tlsext_host_name(ssl_, name.c_str()) != 1)
        return ErrorInfo{Error::tls_configuration, "Unable to configure SNI"};
      X509_VERIFY_PARAM *parameters = SSL_get0_param(ssl_);
      const int configured =
          is_ip_address
              ? X509_VERIFY_PARAM_set1_ip_asc(parameters, name.c_str())
              : X509_VERIFY_PARAM_set1_host(parameters, name.c_str(), name.size());
      if (configured != 1)
        return ErrorInfo{Error::tls_configuration,
                         "Unable to configure TLS hostname verification"};
    }
  }
  return {};
}

Task<ErrorInfo> Connection::flush_tls(std::chrono::milliseconds timeout) {
  BIO *output = SSL_get_wbio(ssl_);
  while (BIO_ctrl_pending(output) > 0) {
    std::array<char, 16 * 1024> buffer{};
    const int count =
        BIO_read(output, buffer.data(), static_cast<int>(buffer.size()));
    if (count <= 0) break;
    auto error = co_await raw_write(std::string(buffer.data(), count), timeout);
    if (error) co_return error;
  }
  co_return ErrorInfo{};
}

Task<ErrorInfo> Connection::handshake(std::chrono::milliseconds timeout) {
  if (!ssl_)
    co_return ErrorInfo{Error::tls_configuration, "TLS is not initialized"};
  for (;;) {
    const int status = SSL_do_handshake(ssl_);
    const int ssl_error =
        status == 1 ? SSL_ERROR_NONE : SSL_get_error(ssl_, status);
    const auto tls_code =
        ssl_error != SSL_ERROR_NONE && ssl_error != SSL_ERROR_WANT_READ &&
                ssl_error != SSL_ERROR_WANT_WRITE
            ? ERR_get_error()
            : 0;
    auto flushed = co_await flush_tls(timeout);
    if (flushed) co_return flushed;
    if (status == 1) co_return ErrorInfo{};
    if (ssl_error == SSL_ERROR_WANT_WRITE) continue;
    if (ssl_error == SSL_ERROR_WANT_READ) {
      auto encrypted = co_await raw_read(timeout);
      if (!encrypted) co_return encrypted.error();
      if (encrypted->eof)
        co_return ErrorInfo{Error::tls_handshake,
                            "TLS peer closed during handshake"};
      if (BIO_write(SSL_get_rbio(ssl_), encrypted->data.data(),
                    static_cast<int>(encrypted->data.size())) <= 0)
        co_return ErrorInfo{Error::tls_handshake, "Unable to feed TLS handshake"};
      continue;
    }
    const long verification = SSL_get_verify_result(ssl_);
    if (!tls_server_ && verification != X509_V_OK)
      co_return ErrorInfo{Error::tls_verification,
                          X509_verify_cert_error_string(verification), ssl_error,
                          verification};
    co_return ErrorInfo{Error::tls_handshake, "TLS handshake failed", ssl_error,
                        static_cast<long>(tls_code)};
  }
}
#endif

void Connection::close() {
  if (!runtime_) return;
  open_.store(false, std::memory_order_release);
  auto self = shared_from_this();
  runtime_->post([self] { self->close_on_loop(); });
}

void Connection::close_on_loop() {
  if (closing_) return;
  closing_ = true;
  open_.store(false, std::memory_order_release);
  if (pending_read_)
    finish_read(ErrorInfo{Error::cancelled, "Connection closed"});
  if (pending_write_)
    finish_write(ErrorInfo{Error::cancelled, "Connection closed"});
  if (pending_connect_)
    finish_connect(ErrorInfo{Error::cancelled, "Connection closed"});
  closing_handles_ = 0;
  const auto close_handle = [this](uv_handle_t *handle) {
    if (!uv_is_closing(handle)) {
      ++closing_handles_;
      uv_close(handle, &Connection::close_callback);
    }
  };
  if (tcp_initialized_)
    close_handle(reinterpret_cast<uv_handle_t *>(&tcp_));
  if (read_timer_initialized_)
    close_handle(reinterpret_cast<uv_handle_t *>(&read_timer_));
  if (write_timer_initialized_)
    close_handle(reinterpret_cast<uv_handle_t *>(&write_timer_));
  if (closing_handles_ == 0) self_keep_.reset();
}

void Connection::close_callback(uv_handle_t *handle) {
  auto *connection = static_cast<Connection *>(handle->data);
  if (connection && --connection->closing_handles_ == 0)
    connection->self_keep_.reset();
}

bool Connection::open() const noexcept {
  return open_.load(std::memory_order_acquire);
}

bool Connection::secure() const noexcept {
#ifdef CHHTTP_HAS_TLS
  return ssl_ != nullptr;
#else
  return false;
#endif
}

std::string Connection::remote_address() const { return remote_address_; }
std::uint16_t Connection::remote_port() const noexcept { return remote_port_; }

Listener::Listener(std::shared_ptr<Runtime> runtime)
    : runtime_(std::move(runtime)) {}

Listener::~Listener() = default;

Result<std::shared_ptr<Listener>>
Listener::create(std::shared_ptr<Runtime> runtime, std::string host,
                 std::uint16_t port, int backlog, AcceptCallback callback,
                 bool reuse_address) {
  auto listener = std::shared_ptr<Listener>(new Listener(std::move(runtime)));
  std::promise<ErrorInfo> completed;
  auto future = completed.get_future();
  listener->runtime_->post([listener, host = std::move(host), port, backlog,
                            callback = std::move(callback),
                            reuse_address, &completed]() mutable {
    listener->self_keep_ = listener;
    sockaddr_storage address{};
    int status = host.find(':') != std::string::npos
                     ? uv_ip6_addr(host.c_str(), port,
                                   reinterpret_cast<sockaddr_in6 *>(&address))
                     : uv_ip4_addr(host.c_str(), port,
                                   reinterpret_cast<sockaddr_in *>(&address));
    if (status != 0) {
      listener->self_keep_.reset();
      completed.set_value(ErrorInfo{Error::connect,
                                    "Invalid listen address " + host + ": " +
                                        uv_message(status),
                                    status});
      return;
    }
    status = uv_tcp_init_ex(listener->runtime_->loop(), &listener->tcp_,
                            address.ss_family);
    if (status != 0) {
      listener->self_keep_.reset();
      completed.set_value(ErrorInfo{Error::internal,
                                    "Unable to initialize listener: " +
                                        uv_message(status),
                                    status});
      return;
    }
    listener->initialized_ = true;
    listener->tcp_.data = listener.get();
    if (reuse_address) {
      uv_os_fd_t descriptor{};
      status = uv_fileno(
          reinterpret_cast<const uv_handle_t *>(&listener->tcp_), &descriptor);
      if (status == 0) {
        const int enabled = 1;
#ifdef _WIN32
        if (setsockopt(reinterpret_cast<SOCKET>(descriptor), SOL_SOCKET,
                       SO_REUSEADDR,
                       reinterpret_cast<const char *>(&enabled),
                       sizeof(enabled)) != 0)
          status = uv_translate_sys_error(WSAGetLastError());
#else
        if (setsockopt(static_cast<int>(descriptor), SOL_SOCKET, SO_REUSEADDR,
                       &enabled, sizeof(enabled)) != 0)
          status = uv_translate_sys_error(errno);
#endif
      }
    }
    if (status == 0)
      status = uv_tcp_bind(&listener->tcp_,
                           reinterpret_cast<const sockaddr *>(&address), 0);
    if (status == 0) {
      listener->callback_ = std::move(callback);
      status = uv_listen(reinterpret_cast<uv_stream_t *>(&listener->tcp_), backlog,
                         &Listener::connection_callback);
    }
    if (status == 0) {
      sockaddr_storage bound{};
      int size = sizeof(bound);
      if (uv_tcp_getsockname(&listener->tcp_,
                             reinterpret_cast<sockaddr *>(&bound), &size) == 0) {
        listener->port_ = bound.ss_family == AF_INET
                              ? ntohs(reinterpret_cast<sockaddr_in *>(&bound)->sin_port)
                              : ntohs(reinterpret_cast<sockaddr_in6 *>(&bound)->sin6_port);
      }
      completed.set_value({});
    } else {
      completed.set_value(ErrorInfo{Error::connect,
                                    "Unable to listen on " + host + ": " +
                                        uv_message(status),
                                    status});
      listener->close();
    }
  });
  auto error = future.get();
  if (error) return error;
  return listener;
}

void Listener::connection_callback(uv_stream_t *server, int status) {
  if (status != 0) return;
  auto *listener = static_cast<Listener *>(server->data);
  auto connection = Connection::accept(listener->runtime_, server);
  if (connection && listener->callback_) listener->callback_(std::move(connection));
}

void Listener::close() {
  if (!runtime_) return;
  auto self = shared_from_this();
  runtime_->post([self] {
    if (!self->initialized_ || self->closing_) return;
    self->closing_ = true;
    uv_close(reinterpret_cast<uv_handle_t *>(&self->tcp_),
             &Listener::close_callback);
  });
}

void Listener::close_callback(uv_handle_t *handle) {
  auto *listener = static_cast<Listener *>(handle->data);
  listener->self_keep_.reset();
}

} // namespace chhttp::detail

namespace chhttp {
namespace {

struct SleepState {
  uv_timer_t timer{};
  std::shared_ptr<SleepState> self_keep;
  std::coroutine_handle<> continuation;
};

struct SleepAwaiter {
  std::chrono::milliseconds duration;
  std::shared_ptr<SleepState> state{std::make_shared<SleepState>()};
  bool await_ready() const noexcept { return duration.count() <= 0; }
  void await_suspend(std::coroutine_handle<> continuation) {
    state->continuation = continuation;
    if (auto *runtime = detail::current_runtime()) {
      state->self_keep = state;
      state->timer.data = state.get();
      uv_timer_init(runtime->loop(), &state->timer);
      uv_timer_start(
          &state->timer,
          [](uv_timer_t *timer) {
            uv_timer_stop(timer);
            uv_close(reinterpret_cast<uv_handle_t *>(timer), [](uv_handle_t *handle) {
              auto *state = static_cast<SleepState *>(handle->data);
              auto self = state->self_keep;
              state->self_keep.reset();
              state->continuation.resume();
            });
          },
          static_cast<std::uint64_t>(std::max<std::int64_t>(1, duration.count())),
          0);
    } else {
      auto state_copy = state;
      std::thread([state_copy, duration = duration] {
        std::this_thread::sleep_for(duration);
        state_copy->continuation.resume();
      }).detach();
    }
  }
  void await_resume() const noexcept {}
};

} // namespace

Task<void> sleep_for(std::chrono::milliseconds duration) {
  co_await SleepAwaiter{duration};
}

} // namespace chhttp
