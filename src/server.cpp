#include "detail.hpp"

#ifdef CHHTTP_HAS_TLS
#include <openssl/err.h>
#endif

#include <charconv>
#include <condition_variable>
#include <fstream>
#include <queue>
#include <regex>
#include <shared_mutex>

namespace chhttp {
namespace {

struct CompiledPattern {
  std::regex expression;
  std::vector<std::string> names;
};

bool regex_special(char ch) {
  constexpr std::string_view special = R"(.^$|()[]{}+?\)";
  return special.find(ch) != std::string_view::npos;
}

CompiledPattern compile_pattern(std::string_view pattern) {
  if (pattern.starts_with("regex:"))
    return {std::regex(std::string(pattern.substr(6))), {}};
  if (pattern.starts_with('^'))
    return {std::regex(std::string(pattern)), {}};
  std::string expression = "^";
  std::vector<std::string> names;
  for (std::size_t index = 0; index < pattern.size();) {
    if (pattern[index] == '{') {
      const auto end = pattern.find('}', index + 1);
      if (end == std::string_view::npos)
        throw std::invalid_argument("Unclosed route parameter");
      names.emplace_back(pattern.substr(index + 1, end - index - 1));
      if (names.back().empty())
        throw std::invalid_argument("Route parameter name cannot be empty");
      expression += "([^/]+)";
      index = end + 1;
    } else if (pattern[index] == ':' &&
               (index == 0 || pattern[index - 1] == '/')) {
      auto end = index + 1;
      while (end < pattern.size() && pattern[end] != '/') ++end;
      names.emplace_back(pattern.substr(index + 1, end - index - 1));
      if (names.back().empty())
        throw std::invalid_argument("Route parameter name cannot be empty");
      expression += "([^/]+)";
      index = end;
    } else if (pattern[index] == '*') {
      names.emplace_back("wildcard");
      expression += "(.*)";
      ++index;
    } else {
      if (regex_special(pattern[index])) expression.push_back('\\');
      expression.push_back(pattern[index++]);
    }
  }
  expression += '$';
  return {std::regex(expression), std::move(names)};
}

bool match_pattern(const CompiledPattern &pattern, std::string_view path,
                   PathParams &parameters) {
  std::match_results<std::string_view::const_iterator> matches;
  if (!std::regex_match(path.begin(), path.end(), matches, pattern.expression))
    return false;
  for (std::size_t index = 0;
       index < pattern.names.size() && index + 1 < matches.size(); ++index) {
    const std::string encoded(matches[index + 1].first,
                              matches[index + 1].second);
    auto decoded = url_decode(encoded);
    parameters[pattern.names[index]] = decoded ? std::move(*decoded) : encoded;
  }
  return true;
}

struct ByteRange {
  std::uint64_t first{0};
  std::uint64_t last{0};
};

std::optional<ByteRange> parse_range(std::string_view value,
                                     std::uint64_t size) {
  if (!value.starts_with("bytes=") || value.find(',') != std::string_view::npos ||
      size == 0)
    return std::nullopt;
  value.remove_prefix(6);
  const auto dash = value.find('-');
  if (dash == std::string_view::npos) return std::nullopt;
  const auto parse_number = [](std::string_view text,
                               std::uint64_t &result) {
    if (text.empty()) return false;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), result);
    return parsed.ec == std::errc{} &&
           parsed.ptr == text.data() + text.size();
  };
  ByteRange range;
  if (dash == 0) {
    std::uint64_t suffix = 0;
    if (!parse_number(value.substr(1), suffix) || suffix == 0) return std::nullopt;
    suffix = std::min(suffix, size);
    range.first = size - suffix;
    range.last = size - 1;
  } else {
    if (!parse_number(value.substr(0, dash), range.first) || range.first >= size)
      return std::nullopt;
    if (dash + 1 == value.size()) {
      range.last = size - 1;
    } else if (!parse_number(value.substr(dash + 1), range.last) ||
               range.last < range.first) {
      return std::nullopt;
    }
    range.last = std::min(range.last, size - 1);
  }
  return range;
}

class WorkerPool {
public:
  WorkerPool(std::shared_ptr<detail::Runtime> runtime, std::size_t count)
      : runtime_(std::move(runtime)) {
    count = std::max<std::size_t>(1, count);
    threads_.reserve(count);
    for (std::size_t index = 0; index < count; ++index)
      threads_.emplace_back([this] { worker(); });
  }
  ~WorkerPool() {
    {
      std::lock_guard lock(mutex_);
      stopping_ = true;
    }
    ready_.notify_all();
    for (auto &thread : threads_)
      if (thread.joinable()) thread.join();
  }

  Task<void> execute(std::function<void()> function) {
    struct State {
      std::coroutine_handle<> continuation;
      std::exception_ptr exception;
    };
    struct Awaiter {
      WorkerPool *pool;
      std::function<void()> function;
      std::shared_ptr<State> state{std::make_shared<State>()};
      bool await_ready() const noexcept { return false; }
      void await_suspend(std::coroutine_handle<> continuation) {
        state->continuation = continuation;
        auto state_copy = state;
        pool->enqueue([pool = pool, state_copy,
                       function = std::move(function)]() mutable {
          try {
            function();
          } catch (...) {
            state_copy->exception = std::current_exception();
          }
          pool->runtime_->post(
              [state_copy] { state_copy->continuation.resume(); });
        });
      }
      void await_resume() {
        if (state->exception) std::rethrow_exception(state->exception);
      }
    };
    co_await Awaiter{this, std::move(function)};
  }

private:
  void enqueue(std::function<void()> function) {
    {
      std::lock_guard lock(mutex_);
      queue_.push(std::move(function));
    }
    ready_.notify_one();
  }
  void worker() {
    for (;;) {
      std::function<void()> function;
      {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
        if (stopping_ && queue_.empty()) return;
        function = std::move(queue_.front());
        queue_.pop();
      }
      function();
    }
  }

  std::shared_ptr<detail::Runtime> runtime_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::queue<std::function<void()>> queue_;
  std::vector<std::thread> threads_;
  bool stopping_{false};
};

class ChunkSink final : public StreamWriter::Sink {
public:
  ChunkSink(std::shared_ptr<detail::Connection> connection,
            std::chrono::milliseconds timeout, bool chunked)
      : connection_(std::move(connection)), timeout_(timeout),
        chunked_(chunked) {}

  Task<bool> write(std::string data) override {
    if (!open_) co_return false;
    if (data.empty()) co_return true;
    auto error = chunked_
                     ? co_await detail::write_chunk(connection_, data, timeout_)
                     : co_await connection_->write(std::move(data), timeout_);
    if (error) open_ = false;
    co_return !error;
  }
  Task<bool> flush() override { co_return open(); }
  bool open() const noexcept override {
    return open_ && connection_ && connection_->open();
  }
  void close() noexcept { open_ = false; }

private:
  std::shared_ptr<detail::Connection> connection_;
  std::chrono::milliseconds timeout_;
  bool chunked_{true};
  std::atomic_bool open_{true};
};

} // namespace

class ServerRequestBodySource final : public RequestBodyStream::Source {
public:
  ServerRequestBodySource(std::shared_ptr<detail::Connection> connection,
                          std::shared_ptr<std::string> buffer,
                          detail::RequestBodyState state,
                          detail::HttpReadOptions options, Request *request)
      : connection_(std::move(connection)), buffer_(std::move(buffer)),
        state_(std::move(state)), options_(std::move(options)),
        request_(request) {
    if (!state_.chunked && state_.content_length.value_or(0) == 0) {
      complete_ = true;
    }
  }

  Task<ErrorInfo> consume(AsyncBodyConsumer consumer) override {
    if (consumed_.exchange(true))
      co_return complete_
                    ? ErrorInfo{Error::invalid_argument,
                                "Request body stream was already consumed"}
                    : ErrorInfo{Error::cancelled,
                                "Request body stream is no longer available"};
    if (complete_) co_return ErrorInfo{};
    if (cancelled_)
      co_return ErrorInfo{Error::cancelled, "Request body stream cancelled"};
    if (state_.content_length &&
        *state_.content_length > options_.max_body_size) {
      error_ = {Error::body_too_large,
                "Request body exceeds the stream route limit"};
      co_return error_;
    }
    options_.cancelled = [this] { return cancelled_.load(); };
    options_.on_data_async =
        [this, consumer = std::move(consumer)](
            std::string_view data) mutable -> Task<bool> {
      if (cancelled_) co_return false;
      received_ += data.size();
      const bool accepted = co_await consumer(data);
      co_return accepted;
    };
    auto body = co_await detail::read_request_body(connection_, *buffer_, state_,
                                                   options_);
    if (!body) {
      error_ = body.error();
      co_return error_;
    }
    complete_ = true;
    if (request_) request_->headers = state_.headers;
    co_return ErrorInfo{};
  }

  void cancel() noexcept override {
    cancelled_ = true;
    if (connection_) connection_->close();
  }

  [[nodiscard]] std::optional<std::uint64_t>
  content_length() const noexcept override {
    return state_.content_length;
  }
  [[nodiscard]] std::uint64_t received() const noexcept override {
    return received_.load();
  }
  [[nodiscard]] bool consumed() const noexcept override { return consumed_; }
  [[nodiscard]] bool complete() const noexcept override { return complete_; }
  [[nodiscard]] const Headers &headers() const noexcept { return state_.headers; }
  [[nodiscard]] const ErrorInfo &error() const noexcept { return error_; }
  void detach_request() noexcept { request_ = nullptr; }

private:
  std::shared_ptr<detail::Connection> connection_;
  std::shared_ptr<std::string> buffer_;
  detail::RequestBodyState state_;
  detail::HttpReadOptions options_;
  Request *request_{nullptr};
  std::atomic_uint64_t received_{0};
  std::atomic_bool consumed_{false};
  std::atomic_bool complete_{false};
  std::atomic_bool cancelled_{false};
  ErrorInfo error_;
};

class Server::Impl {
public:
  struct Route {
    std::string method;
    CompiledPattern pattern;
    Handler sync;
    AsyncHandler async;
  };
  struct WsRoute {
    CompiledPattern pattern;
    WebSocketHandler handler;
    SubprotocolSelector selector;
  };
  struct StreamRoute {
    std::string method;
    CompiledPattern pattern;
    StreamRequestHandler handler;
    StreamRouteOptions options;
  };
  struct Mount {
    std::string prefix;
    std::filesystem::path root;
    Headers headers;
  };
  struct Session {
    std::shared_ptr<detail::Connection> connection;
    std::atomic_bool in_request{false};
  };

  explicit Impl(ServerOptions server_options)
      : options(std::move(server_options)) {
    if (options.worker_threads == 0)
      options.worker_threads = std::clamp<std::size_t>(
          std::thread::hardware_concurrency(), 1, 8);
    default_error_handler = [](const Request &, Response &response) {
      response.set_content(response.status == 404 ? "Not Found"
                                                  : status_reason(response.status));
    };
    default_exception_handler = [](const Request &, Response &response,
                                   std::exception_ptr) {
      response.status = 500;
      response.set_content("Internal Server Error");
    };
  }

  ~Impl() { stop(); }

  bool start(std::string host, std::uint16_t requested_port) {
    std::lock_guard state_lock(state_mutex);
    if (started) return false;
    runtime = std::make_shared<detail::Runtime>();
    workers = std::make_unique<WorkerPool>(runtime, options.worker_threads);
#ifdef CHHTTP_HAS_TLS
    if (options.tls) {
      ErrorInfo error;
      tls_context = detail::create_server_tls_context(*options.tls, error);
      if (!tls_context) {
        last_start_error = error.message;
        workers.reset();
        runtime->stop();
        runtime.reset();
        return false;
      }
    }
#else
    if (options.tls) {
      last_start_error = "TLS support is disabled";
      workers.reset();
      runtime->stop();
      runtime.reset();
      return false;
    }
#endif
    auto weak = std::weak_ptr<detail::Runtime>(runtime);
    auto created = detail::Listener::create(
        runtime, std::move(host), requested_port, 1024,
        [this, weak](std::shared_ptr<detail::Connection> connection) {
          if (stopping || weak.expired()) {
            connection->close();
            return;
          }
          if (auto error = connection->set_no_delay(options.tcp_no_delay)) {
            connection->close();
            return;
          }
          auto session = std::make_shared<Session>();
          session->connection = std::move(connection);
          {
            std::lock_guard lock(sessions_mutex);
            sessions.push_back(session);
          }
          ++active_sessions;
          detail::detach_task(serve(std::move(session)));
        },
        options.reuse_address);
    if (!created) {
      last_start_error = created.error().message;
#ifdef CHHTTP_HAS_TLS
      if (tls_context) {
        SSL_CTX_free(tls_context);
        tls_context = nullptr;
      }
#endif
      workers.reset();
      runtime->stop();
      runtime.reset();
      return false;
    }
    listener = *created;
    bound_port = listener->port();
    stopping = false;
    started = true;
    return true;
  }

  void stop() {
    bool expected = false;
    if (!started || !stopping.compare_exchange_strong(expected, true)) return;
    if (listener) listener->close();
    {
      std::lock_guard lock(sessions_mutex);
      for (const auto &session : sessions)
        if (session && !session->in_request) session->connection->close();
    }
    {
      std::unique_lock lock(state_mutex);
      sessions_cv.wait_for(lock, options.shutdown_timeout,
                           [&] { return active_sessions.load() == 0; });
    }
    if (active_sessions != 0) {
      std::lock_guard lock(sessions_mutex);
      for (const auto &session : sessions)
        if (session) session->connection->close();
    }
    {
      std::unique_lock lock(state_mutex);
      sessions_cv.wait_for(lock, 2s,
                           [&] { return active_sessions.load() == 0; });
    }
    listener.reset();
    workers.reset();
    if (runtime) runtime->stop();
    runtime.reset();
#ifdef CHHTTP_HAS_TLS
    if (tls_context) {
      SSL_CTX_free(tls_context);
      tls_context = nullptr;
    }
#endif
    started = false;
    bound_port = 0;
    state_cv.notify_all();
  }

  Task<void> serve(std::shared_ptr<Session> session) {
    auto connection = session->connection;
#ifdef CHHTTP_HAS_TLS
    if (tls_context) {
      auto error = connection->enable_tls(tls_context, true);
      if (!error)
        error = co_await connection->handshake(options.request_timeout);
      if (error) {
        connection->close();
        session_finished(session);
        co_return;
      }
    }
#endif
    auto buffer = std::make_shared<std::string>();
    std::size_t request_count = 0;
    while (connection->open() &&
           request_count < options.keep_alive_max_requests) {
      session->in_request = false;
      if (stopping) break;
      const auto timeout = request_count == 0 ? options.request_timeout
                                              : options.keep_alive_timeout;
      auto parsed = co_await detail::read_request_head(
          connection, *buffer,
          {.max_header_size = options.max_header_size,
           .max_body_size = options.max_body_size,
           .read_timeout = timeout});
      if (!parsed) {
        if (parsed.error().code == Error::body_too_large ||
            parsed.error().code == Error::protocol) {
          Request invalid;
          invalid.keep_alive = false;
          Response response;
          response.status = parsed.error().code == Error::body_too_large ? 413 : 400;
          response.keep_alive = false;
          response.set_content(status_reason(response.status));
          co_await detail::write_response(connection, invalid, response,
                                           options.request_timeout);
        }
        break;
      }
      session->in_request = true;
      Request request = std::move(parsed->request);
      request.remote_address = connection->remote_address();
      request.remote_port = connection->remote_port();
      if (const auto route = find_stream_route(request)) {
        auto &entry = stream_routes[*route];
        const auto body_limit = entry.options.max_body_size == 0
                                    ? options.max_body_size
                                    : entry.options.max_body_size;
        if (parsed->body.content_length &&
            *parsed->body.content_length > body_limit) {
          Response response;
          response.status = 413;
          response.keep_alive = false;
          response.set_content(status_reason(response.status));
          co_await detail::write_response(connection, request, response,
                                           options.request_timeout);
          break;
        }
        detail::HttpReadOptions read_options{
            .max_header_size = options.max_header_size,
            .max_body_size = body_limit,
            .read_timeout = options.request_timeout,
            .first_body_byte_timeout = entry.options.first_body_byte_timeout,
            .idle_timeout = entry.options.idle_timeout,
            .auto_decompress = options.auto_decompress_request &&
                               entry.options.auto_decompress};
        if (entry.options.total_timeout)
          read_options.deadline = std::chrono::steady_clock::now() +
                                  *entry.options.total_timeout;
        auto source = std::make_shared<ServerRequestBodySource>(
            connection, buffer, std::move(parsed->body),
            std::move(read_options), &request);
        RequestBodyStream body(source);
        Response response;
        response.version = request.version;
        response.keep_alive = request.keep_alive;
        co_await dispatch_stream(entry, request, body, response);
        source->detach_request();
        request.headers = source->headers();
        if (source->error() && response.status == 200 && response.body.empty() &&
            !response.is_streaming()) {
          response.status = source->error().code == Error::body_too_large
                                ? 413
                                : source->error().code == Error::timeout ? 408
                                                                         : 400;
          response.set_content(source->error().message);
        }
        if (!source->complete()) response.keep_alive = false;
        if (request_count + 1 >= options.keep_alive_max_requests || stopping)
          response.keep_alive = false;
        const bool reusable = response.keep_alive && request.keep_alive &&
                              source->complete();
        auto error = co_await send_response(connection, request, response);
        if (logger) {
          try {
            logger(request, response);
          } catch (...) {
          }
        }
        ++request_count;
        session->in_request = false;
        if (error || !reusable) break;
        continue;
      }

      auto request_body = co_await detail::read_request_body(
          connection, *buffer, parsed->body,
          {.max_header_size = options.max_header_size,
           .max_body_size = options.max_body_size,
           .read_timeout = options.request_timeout,
           .auto_decompress = options.auto_decompress_request});
      if (!request_body) {
        Response response;
        response.status = request_body.error().code == Error::body_too_large
                              ? 413
                              : request_body.error().code == Error::timeout ? 408
                                                                             : 400;
        response.keep_alive = false;
        response.set_content(request_body.error().message);
        co_await detail::write_response(connection, request, response,
                                         options.request_timeout);
        break;
      }
      request.body = std::move(*request_body);
      request.headers = std::move(parsed->body.headers);
      const auto content_type = request.headers.get("Content-Type");
      if (detail::lower(content_type).starts_with("multipart/form-data")) {
        auto files = parse_multipart(request.body, content_type);
        if (files) request.files = std::move(*files);
      }

      if (detail::is_websocket_upgrade(request)) {
        const auto route = find_ws_route(request);
        if (!route) {
          Response response;
          response.status = 404;
          response.keep_alive = false;
          default_error_handler(request, response);
          co_await detail::write_response(connection, request, response,
                                           options.request_timeout);
          break;
        }
        auto &entry = websocket_routes[*route];
        std::vector<std::string> offered;
        for (const auto &value :
             request.headers.get_all("Sec-WebSocket-Protocol")) {
          auto protocols = detail::split_tokens(value, ',');
          offered.insert(offered.end(), protocols.begin(), protocols.end());
        }
        std::string selected = entry.selector ? entry.selector(offered) : "";
        if (!selected.empty() &&
            (!detail::valid_header_name(selected) ||
             std::ranges::find(offered, selected) == offered.end())) {
          selected.clear();
        }
        auto handshake = co_await detail::websocket_server_handshake(
            connection, request, selected, options.request_timeout);
        if (handshake) break;
        auto channel = detail::make_websocket_channel(
            connection, std::move(*buffer), false, selected,
            options.request_timeout);
        WebSocket socket(channel);
        bool handler_failed = false;
        try {
          co_await entry.handler(request, socket);
        } catch (...) {
          handler_failed = true;
        }
        if (socket.open())
          co_await socket.close(handler_failed ? 1011 : 1000,
                                handler_failed ? "Handler error" : "");
        session_finished(session);
        co_return;
      }

      Response response;
      response.version = request.version;
      response.keep_alive = request.keep_alive;
      co_await dispatch(request, response);
      if (request_count + 1 >= options.keep_alive_max_requests || stopping)
        response.keep_alive = false;
      const bool reusable = response.keep_alive && request.keep_alive;
      auto error = co_await send_response(connection, request, response);
      if (logger) {
        try {
          logger(request, response);
        } catch (...) {
        }
      }
      ++request_count;
      session->in_request = false;
      if (error || !reusable) break;
    }
    connection->close();
    session_finished(session);
  }

  void session_finished(const std::shared_ptr<Session> &session) {
    {
      std::lock_guard lock(sessions_mutex);
      std::erase(sessions, session);
    }
    --active_sessions;
    sessions_cv.notify_all();
  }

  std::optional<std::size_t> find_route(Request &request) const {
    for (std::size_t index = 0; index < routes.size(); ++index) {
      if (!detail::iequals(routes[index].method, request.method) &&
          !(detail::iequals(request.method, "HEAD") &&
            detail::iequals(routes[index].method, "GET")))
        continue;
      PathParams parameters;
      if (match_pattern(routes[index].pattern, request.path, parameters)) {
        request.path_params = std::move(parameters);
        return index;
      }
    }
    return std::nullopt;
  }

  std::optional<std::size_t> find_stream_route(Request &request) const {
    for (std::size_t index = 0; index < stream_routes.size(); ++index) {
      if (!detail::iequals(stream_routes[index].method, request.method))
        continue;
      PathParams parameters;
      if (match_pattern(stream_routes[index].pattern, request.path,
                        parameters)) {
        request.path_params = std::move(parameters);
        return index;
      }
    }
    return std::nullopt;
  }

  std::optional<std::size_t> find_ws_route(Request &request) const {
    for (std::size_t index = 0; index < websocket_routes.size(); ++index) {
      PathParams parameters;
      if (match_pattern(websocket_routes[index].pattern, request.path,
                        parameters)) {
        request.path_params = std::move(parameters);
        return index;
      }
    }
    return std::nullopt;
  }

  bool serve_static(const Request &request, Response &response) const {
    if (!detail::iequals(request.method, "GET") &&
        !detail::iequals(request.method, "HEAD"))
      return false;
    for (const auto &mount : mounts) {
      if (!request.path.starts_with(mount.prefix)) continue;
      auto relative = request.path.substr(mount.prefix.size());
      while (!relative.empty() && relative.front() == '/') relative.erase(0, 1);
      auto path = mount.root / std::filesystem::path(relative);
      std::error_code ignored;
      if (std::filesystem::is_directory(path, ignored)) path /= "index.html";
      if (!std::filesystem::is_regular_file(path, ignored) ||
          !detail::path_is_within(mount.root, path))
        return false;
      response.set_file(path);
      for (const auto &[name, value] : mount.headers)
        response.headers.set(name, value);
      response.headers.set("Accept-Ranges", "bytes");
      return true;
    }
    return false;
  }

  Task<void> dispatch(Request &request, Response &response) {
    try {
      if (pre_routing && pre_routing(request, response)) co_return;
      if (const auto route = find_route(request)) {
        auto &entry = routes[*route];
        if (entry.async)
          co_await entry.async(request, response);
        else
          co_await workers->execute(
              [&entry, &request, &response] { entry.sync(request, response); });
      } else if (!serve_static(request, response)) {
        response.status = 404;
        (error_handler ? error_handler : default_error_handler)(request, response);
      }
      if (post_routing) post_routing(request, response);
    } catch (...) {
      (exception_handler ? exception_handler : default_exception_handler)(
          request, response, std::current_exception());
    }
  }

  Task<void> dispatch_stream(StreamRoute &route, Request &request,
                             RequestBodyStream &body, Response &response) {
    try {
      if (pre_routing && pre_routing(request, response)) co_return;
      co_await route.handler(request, body, response);
      if (post_routing) post_routing(request, response);
    } catch (...) {
      (exception_handler ? exception_handler : default_exception_handler)(
          request, response, std::current_exception());
    }
  }

  Task<ErrorInfo> send_file(const std::shared_ptr<detail::Connection> &connection,
                            const Request &request, Response &response) {
    const auto &path = *ServerAccess::file(response);
    std::error_code filesystem_error;
    const auto size = std::filesystem::file_size(path, filesystem_error);
    if (filesystem_error)
      co_return ErrorInfo{Error::read, "Unable to stat static file"};
    std::uint64_t first = 0;
    std::uint64_t last = size == 0 ? 0 : size - 1;
    if (request.headers.contains("Range")) {
      auto range = parse_range(request.headers.get("Range"), size);
      if (!range) {
        response.status = 416;
        response.headers.set("Content-Range", "bytes */" + std::to_string(size));
        response.body.clear();
        co_return co_await detail::write_response(connection, request, response,
                                                   options.request_timeout);
      }
      first = range->first;
      last = range->last;
      response.status = 206;
      response.headers.set("Content-Range",
                           "bytes " + std::to_string(first) + "-" +
                               std::to_string(last) + "/" + std::to_string(size));
    }
    const auto length = size == 0 ? 0 : last - first + 1;
    auto error = co_await detail::write_response_head(
        connection, request, response, false, length, options.request_timeout);
    if (error || detail::iequals(request.method, "HEAD") || length == 0)
      co_return error;
    std::ifstream input(path, std::ios::binary);
    if (!input) co_return ErrorInfo{Error::read, "Unable to open static file"};
    input.seekg(static_cast<std::streamoff>(first));
    std::array<char, 64 * 1024> buffer{};
    std::uint64_t remaining = length;
    while (remaining > 0) {
      const auto count = static_cast<std::streamsize>(
          std::min<std::uint64_t>(remaining, buffer.size()));
      input.read(buffer.data(), count);
      const auto received = input.gcount();
      if (received <= 0)
        co_return ErrorInfo{Error::read, "Static file read was truncated"};
      error = co_await connection->write(
          std::string(buffer.data(), static_cast<std::size_t>(received)),
          options.request_timeout);
      if (error) co_return error;
      remaining -= static_cast<std::uint64_t>(received);
    }
    co_return ErrorInfo{};
  }

  Task<ErrorInfo> send_response(
      const std::shared_ptr<detail::Connection> &connection,
      const Request &request, Response &response) {
    if (ServerAccess::file(response))
      co_return co_await send_file(connection, request, response);
    if (response.is_streaming()) {
      const bool omit_body = detail::iequals(request.method, "HEAD") ||
                             (response.status >= 100 && response.status < 200) ||
                             response.status == 204 || response.status == 205 ||
                             response.status == 304;
      const bool chunked = request.version >= 11;
      if (!chunked) response.keep_alive = false;
      auto error = co_await detail::write_response_head(
          connection, request, response, chunked,
          chunked ? std::optional<std::uint64_t>{0} : std::nullopt,
          options.request_timeout);
      if (error || omit_body) co_return error;
      auto sink = std::make_shared<ChunkSink>(connection, options.request_timeout,
                                              chunked);
      StreamWriter writer(sink);
      try {
        co_await ServerAccess::stream(response)(writer);
      } catch (...) {
        sink->close();
        co_return ErrorInfo{Error::write, "Streaming response handler failed"};
      }
      if (!sink->open())
        co_return ErrorInfo{Error::write, "Streaming connection closed"};
      if (chunked)
        error = co_await detail::write_last_chunk(connection,
                                                  options.request_timeout);
      sink->close();
      co_return error;
    }
    if (options.auto_compress_response &&
        response.body.size() >= options.compression_threshold &&
        !response.headers.contains("Content-Encoding") &&
        !detail::iequals(request.method, "HEAD")) {
      const auto encoding =
          detail::select_encoding(request.headers.get("Accept-Encoding"));
      if (!encoding.empty()) {
        auto compressed = detail::compress(response.body, encoding);
        if (compressed) {
          response.body = std::move(*compressed);
          response.headers.set("Content-Encoding", encoding);
          response.headers.set("Vary", "Accept-Encoding");
        }
      }
    }
    co_return co_await detail::write_response(connection, request, response,
                                               options.request_timeout);
  }

  ServerOptions options;
  std::shared_ptr<detail::Runtime> runtime;
  std::unique_ptr<WorkerPool> workers;
  std::shared_ptr<detail::Listener> listener;
#ifdef CHHTTP_HAS_TLS
  SSL_CTX *tls_context{nullptr};
#endif
  std::vector<Route> routes;
  std::vector<StreamRoute> stream_routes;
  std::vector<WsRoute> websocket_routes;
  std::vector<Mount> mounts;
  Middleware pre_routing;
  Middleware post_routing;
  ErrorHandler error_handler;
  ErrorHandler default_error_handler;
  ExceptionHandler exception_handler;
  ExceptionHandler default_exception_handler;
  Logger logger;
  std::atomic_bool started{false};
  std::atomic_bool stopping{false};
  std::atomic_size_t active_sessions{0};
  std::mutex sessions_mutex;
  std::vector<std::shared_ptr<Session>> sessions;
  std::mutex state_mutex;
  std::condition_variable state_cv;
  std::condition_variable sessions_cv;
  std::atomic_uint16_t bound_port{0};
  std::string last_start_error;
};

Server::Server(ServerOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}
Server::~Server() = default;
Server::Server(Server &&) noexcept = default;
Server &Server::operator=(Server &&) noexcept = default;

Server &Server::route(std::string method, std::string pattern,
                      Handler handler) {
  if (impl_->started) throw std::logic_error("Routes cannot change while running");
  impl_->routes.push_back({detail::lower(method), compile_pattern(pattern),
                           std::move(handler), {}});
  return *this;
}

Server &Server::route_async(std::string method, std::string pattern,
                            AsyncHandler handler) {
  if (impl_->started) throw std::logic_error("Routes cannot change while running");
  impl_->routes.push_back({detail::lower(method), compile_pattern(pattern), {},
                           std::move(handler)});
  return *this;
}

Server &Server::route_stream(std::string method, std::string pattern,
                             StreamRequestHandler handler,
                             StreamRouteOptions options) {
  if (impl_->started) throw std::logic_error("Routes cannot change while running");
  if (!handler) throw std::invalid_argument("Stream route handler is empty");
  impl_->stream_routes.push_back({detail::lower(method),
                                  compile_pattern(pattern), std::move(handler),
                                  std::move(options)});
  return *this;
}

Server &Server::get(std::string pattern, Handler handler) {
  return route("GET", std::move(pattern), std::move(handler));
}
Server &Server::post(std::string pattern, Handler handler) {
  return route("POST", std::move(pattern), std::move(handler));
}
Server &Server::put(std::string pattern, Handler handler) {
  return route("PUT", std::move(pattern), std::move(handler));
}
Server &Server::patch(std::string pattern, Handler handler) {
  return route("PATCH", std::move(pattern), std::move(handler));
}
Server &Server::del(std::string pattern, Handler handler) {
  return route("DELETE", std::move(pattern), std::move(handler));
}
Server &Server::options(std::string pattern, Handler handler) {
  return route("OPTIONS", std::move(pattern), std::move(handler));
}
Server &Server::head(std::string pattern, Handler handler) {
  return route("HEAD", std::move(pattern), std::move(handler));
}
Server &Server::get_async(std::string pattern, AsyncHandler handler) {
  return route_async("GET", std::move(pattern), std::move(handler));
}
Server &Server::post_async(std::string pattern, AsyncHandler handler) {
  return route_async("POST", std::move(pattern), std::move(handler));
}
Server &Server::post_stream(std::string pattern,
                            StreamRequestHandler handler,
                            StreamRouteOptions options) {
  return route_stream("POST", std::move(pattern), std::move(handler),
                      std::move(options));
}
Server &Server::put_stream(std::string pattern,
                           StreamRequestHandler handler,
                           StreamRouteOptions options) {
  return route_stream("PUT", std::move(pattern), std::move(handler),
                      std::move(options));
}
Server &Server::patch_stream(std::string pattern,
                             StreamRequestHandler handler,
                             StreamRouteOptions options) {
  return route_stream("PATCH", std::move(pattern), std::move(handler),
                      std::move(options));
}
Server &Server::websocket(std::string pattern, WebSocketHandler handler,
                          SubprotocolSelector selector) {
  if (impl_->started) throw std::logic_error("Routes cannot change while running");
  impl_->websocket_routes.push_back(
      {compile_pattern(pattern), std::move(handler), std::move(selector)});
  return *this;
}
Server &Server::mount(std::string prefix, std::filesystem::path directory,
                      Headers headers) {
  if (impl_->started) throw std::logic_error("Mounts cannot change while running");
  if (prefix.empty() || prefix.front() != '/')
    throw std::invalid_argument("Mount prefix must start with '/'");
  impl_->mounts.push_back({std::move(prefix),
                           std::filesystem::weakly_canonical(directory),
                           std::move(headers)});
  return *this;
}
Server &Server::set_pre_routing_handler(Middleware handler) {
  impl_->pre_routing = std::move(handler);
  return *this;
}
Server &Server::set_post_routing_handler(Middleware handler) {
  impl_->post_routing = std::move(handler);
  return *this;
}
Server &Server::set_error_handler(ErrorHandler handler) {
  impl_->error_handler = std::move(handler);
  return *this;
}
Server &Server::set_exception_handler(ExceptionHandler handler) {
  impl_->exception_handler = std::move(handler);
  return *this;
}
Server &Server::set_logger(Logger logger) {
  impl_->logger = std::move(logger);
  return *this;
}
bool Server::start(std::string host, std::uint16_t port) {
  return impl_->start(std::move(host), port);
}
bool Server::listen(std::string host, std::uint16_t port) {
  if (!start(std::move(host), port)) return false;
  wait();
  return true;
}
void Server::wait() {
  std::unique_lock lock(impl_->state_mutex);
  impl_->state_cv.wait(lock, [this] { return !impl_->started; });
}
void Server::stop() { impl_->stop(); }
bool Server::running() const noexcept { return impl_->started; }
std::uint16_t Server::port() const noexcept { return impl_->bound_port; }

} // namespace chhttp
