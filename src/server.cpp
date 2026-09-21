#include "detail.hpp"

#ifdef CHHTTP_HAS_TLS
#include <openssl/err.h>
#endif

#include <charconv>
#include <condition_variable>
#include <fstream>
#include <map>
#include <queue>
#include <regex>
#include <shared_mutex>

namespace chhttp {
namespace {

// A compiled route pattern is matched by a small hand-written matcher whenever
// the pattern stays within the documented mini-language. Only patterns that ask
// for a real regular expression (or place a wildcard somewhere other than the
// end) fall back to std::regex, which keeps matching independent of the standard
// library's regex implementation and avoids its per-match overhead.
struct PatternSegment {
  std::string literal;
  std::string name;
  bool parameter{false};
  bool wildcard{false};
};

struct CompiledPattern {
  std::optional<std::regex> expression;
  std::vector<std::string> names;
  std::string literal;
  std::vector<PatternSegment> segments;
  bool literal_only{false};
  bool linear{false};
};

using PatternCaptures = std::vector<std::pair<std::string_view, std::string_view>>;

bool regex_special(char ch) {
  constexpr std::string_view special = R"(.^$|()[]{}+?\)";
  return special.find(ch) != std::string_view::npos;
}

CompiledPattern compile_pattern(std::string_view pattern) {
  CompiledPattern compiled;
  if (pattern.starts_with("regex:")) {
    compiled.expression.emplace(std::string(pattern.substr(6)));
    return compiled;
  }
  if (pattern.starts_with('^')) {
    compiled.expression.emplace(std::string(pattern));
    return compiled;
  }
  if (pattern.find_first_of("{*:") == std::string_view::npos) {
    compiled.literal_only = true;
    compiled.literal = std::string(pattern);
    return compiled;
  }
  std::string expression = "^";
  std::vector<std::string> names;
  std::vector<PatternSegment> segments;
  std::string pending_literal;
  std::size_t wildcards = 0;
  const auto flush_literal = [&] {
    if (pending_literal.empty()) return;
    PatternSegment segment;
    segment.literal = std::move(pending_literal);
    pending_literal.clear();
    segments.push_back(std::move(segment));
  };
  const auto add_parameter = [&](std::string_view name) {
    if (name.empty())
      throw std::invalid_argument("Route parameter name cannot be empty");
    names.emplace_back(name);
    flush_literal();
    PatternSegment segment;
    segment.parameter = true;
    segment.name = std::string(name);
    segments.push_back(std::move(segment));
  };
  for (std::size_t index = 0; index < pattern.size();) {
    if (pattern[index] == '{') {
      const auto end = pattern.find('}', index + 1);
      if (end == std::string_view::npos)
        throw std::invalid_argument("Unclosed route parameter");
      add_parameter(pattern.substr(index + 1, end - index - 1));
      expression += "([^/]+)";
      index = end + 1;
    } else if (pattern[index] == ':' &&
               (index == 0 || pattern[index - 1] == '/')) {
      auto end = index + 1;
      while (end < pattern.size() && pattern[end] != '/') ++end;
      add_parameter(pattern.substr(index + 1, end - index - 1));
      expression += "([^/]+)";
      index = end;
    } else if (pattern[index] == '*') {
      names.emplace_back("wildcard");
      flush_literal();
      PatternSegment segment;
      segment.wildcard = true;
      segment.name = "wildcard";
      segments.push_back(std::move(segment));
      expression += "(.*)";
      ++wildcards;
      ++index;
    } else {
      if (regex_special(pattern[index])) expression.push_back('\\');
      expression.push_back(pattern[index]);
      pending_literal.push_back(pattern[index++]);
    }
  }
  flush_literal();
  expression += '$';
  compiled.names = std::move(names);
  // A trailing wildcard is the only form the linear matcher can resolve without
  // backtracking across the whole remaining path.
  if (wildcards == 0 || (wildcards == 1 && !segments.empty() &&
                         segments.back().wildcard)) {
    compiled.linear = true;
    compiled.segments = std::move(segments);
  } else {
    compiled.expression.emplace(std::move(expression));
  }
  return compiled;
}

// Greedy leftmost match with backtracking, mirroring the ECMAScript semantics of
// the equivalent generated regular expression. Restores `captures` on failure so
// a rejected candidate cannot leak partial bindings.
bool match_segments(const std::vector<PatternSegment> &segments,
                    std::size_t index, std::string_view path,
                    PatternCaptures &captures) {
  const std::size_t entry = captures.size();
  if (index == segments.size()) {
    if (path.empty()) return true;
    captures.resize(entry);
    return false;
  }
  const PatternSegment &segment = segments[index];
  if (segment.wildcard) {
    if (index + 1 != segments.size()) {
      captures.resize(entry);
      return false;
    }
    captures.emplace_back(segment.name, path);
    return true;
  }
  if (!segment.parameter) {
    if (path.starts_with(segment.literal) &&
        match_segments(segments, index + 1,
                       path.substr(segment.literal.size()), captures))
      return true;
    captures.resize(entry);
    return false;
  }
  // "[^/]+" is greedy but cannot cross a separator, so candidate lengths are
  // bounded by the next slash.
  const auto slash = path.find('/');
  const std::size_t limit = slash == std::string_view::npos ? path.size() : slash;
  for (std::size_t length = limit; length >= 1; --length) {
    captures.emplace_back(segment.name, path.substr(0, length));
    if (match_segments(segments, index + 1, path.substr(length), captures))
      return true;
    captures.resize(entry);
  }
  captures.resize(entry);
  return false;
}

bool match_pattern(const CompiledPattern &pattern, std::string_view path,
                   PathParams &parameters, PatternCaptures &captures) {
  if (pattern.literal_only) return path == pattern.literal;
  if (pattern.linear) {
    captures.clear();
    if (!match_segments(pattern.segments, 0, path, captures)) return false;
    for (const auto &[name, value] : captures) {
      auto decoded = url_decode(value);
      parameters[std::string(name)] =
          decoded ? std::move(*decoded) : std::string(value);
    }
    return true;
  }
  std::match_results<std::string_view::const_iterator> matches;
  if (!std::regex_match(path.begin(), path.end(), matches, *pattern.expression))
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
  if (!detail::iequals(value.substr(0, 6), "bytes=") || value.find(',') != std::string_view::npos ||
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

  // The handler callable is stored inside the shared completion state so the
  // queued closures stay within std::function's small-buffer limit. Dispatching a
  // handler therefore costs one allocation (the state) instead of two.
  template <class Function> Task<void> execute(Function function) {
    struct State {
      std::coroutine_handle<> continuation{};
      std::exception_ptr exception;
      Function function;
      explicit State(Function value) : function(std::move(value)) {}
    };
    struct Awaiter {
      WorkerPool *pool;
      std::shared_ptr<State> state;
      Awaiter(WorkerPool *pool, Function function)
          : pool(pool),
            state(std::make_shared<State>(std::move(function))) {}
      bool await_ready() const noexcept { return false; }
      void await_suspend(std::coroutine_handle<> continuation) {
        state->continuation = continuation;
        auto state_copy = state;
        pool->enqueue([pool = pool, state_copy] {
          try {
            state_copy->function();
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
    bool wake = false;
    {
      std::lock_guard lock(mutex_);
      // `sleeping_` is only modified while the mutex is held, and a worker
      // increments it before atomically releasing the lock inside wait(). So a
      // zero count guarantees every worker is either running a job or on its way
      // back to the queue, and no signal is needed. Busy intervals that push many
      // jobs therefore cost one wake-up instead of one per request.
      wake = sleeping_ != 0;
      queue_.push(std::move(function));
    }
    if (wake) ready_.notify_one();
  }
  void worker() {
    for (;;) {
      std::function<void()> function;
      {
        std::unique_lock lock(mutex_);
        for (;;) {
          if (!queue_.empty()) {
            function = std::move(queue_.front());
            queue_.pop();
            break;
          }
          if (stopping_) return;
          ++sleeping_;
          ready_.wait(lock);
          --sleeping_;
        }
      }
      function();
    }
  }

  std::shared_ptr<detail::Runtime> runtime_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::queue<std::function<void()>> queue_;
  std::vector<std::thread> threads_;
  std::size_t sleeping_{0};
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
                          std::shared_ptr<detail::ReadBuffer> buffer,
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
  std::shared_ptr<detail::ReadBuffer> buffer_;
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
    build_dispatch_index();
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
    auto buffer = std::make_shared<detail::ReadBuffer>();
    std::size_t request_count = 0;
    while (connection->open() &&
           request_count < options.keep_alive_max_requests) {
      session->in_request = false;
      if (stopping) break;
      const auto timeout = request_count == 0 ? options.request_timeout
                                              : options.keep_alive_timeout;
      // The session takes ownership of the parsed fields below, so the head is
      // read without duplicating them onto Request::headers.
      auto parsed = co_await detail::read_request_head(
          connection, *buffer,
          {.max_header_size = options.max_header_size,
           .max_body_size = options.max_body_size,
           .read_timeout = timeout},
          false);
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
        // Stream handlers observe the fields before the body is consumed.
        request.headers = parsed->body.headers;
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
        if (!runtime->on_loop_thread()) co_await detail::resume_on(runtime);
        source->detach_request();
        if (!connection->open()) break;
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
      // Borrowed lookup: the common non-multipart body path must not allocate a
      // Content-Type copy just to compare a prefix.
      const auto *content_type =
          detail::find_header(request.headers, "Content-Type");
      if (content_type &&
          detail::istarts_with(*content_type, "multipart/form-data")) {
        auto files = parse_multipart(request.body, *content_type);
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
      if (!runtime->on_loop_thread()) co_await detail::resume_on(runtime);
      if (!connection->open()) break;
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

  // Dispatch index. Route patterns cannot change once the server is running, so
  // the bucket layout is built once and then only read. Literal routes resolve
  // through a hash lookup, and only the patterned routes that precede the
  // matched literal are actually evaluated, which preserves registration order.
  // Ordered with a transparent comparator so a std::string_view can be looked up
  // without constructing a temporary key on the request path.
  struct MethodRoutes {
    std::vector<std::size_t> patterned;
    std::map<std::string, std::size_t, std::less<>> literal_first;
  };
  using MethodRoutesMap = std::map<std::string, MethodRoutes, std::less<>>;

  // Lower-cases the request method without allocating for the usual short verbs.
  class MethodKey {
  public:
    explicit MethodKey(std::string_view method) : length_(method.size()) {
      if (length_ <= buffer_.size()) {
        for (std::size_t index = 0; index < length_; ++index) {
          const auto ch = static_cast<unsigned char>(method[index]);
          buffer_[index] = ch >= 'A' && ch <= 'Z' ? static_cast<char>(ch + 32)
                                                  : static_cast<char>(ch);
        }
      } else {
        fallback_ = detail::lower(method);
      }
    }
    [[nodiscard]] std::string_view view() const noexcept {
      return fallback_.empty() ? std::string_view(buffer_.data(), length_)
                               : std::string_view(fallback_);
    }

  private:
    std::array<char, 32> buffer_{};
    std::string fallback_;
    std::size_t length_{0};
  };

  void build_dispatch_index() {
    routes_by_method.clear();
    stream_routes_by_method.clear();
    for (std::size_t index = 0; index < routes.size(); ++index) {
      const auto &route = routes[index];
      auto &bucket = routes_by_method[route.method];
      if (route.pattern.literal_only)
        bucket.literal_first.try_emplace(route.pattern.literal, index);
      else
        bucket.patterned.push_back(index);
    }
    for (std::size_t index = 0; index < stream_routes.size(); ++index) {
      const auto &route = stream_routes[index];
      auto &bucket = stream_routes_by_method[route.method];
      if (route.pattern.literal_only)
        bucket.literal_first.try_emplace(route.pattern.literal, index);
      else
        bucket.patterned.push_back(index);
    }
  }

  // Walks at most two method buckets (HEAD also consults GET) in registration
  // order, stopping as soon as the matched literal route would be reached.
  template <class Table>
  std::optional<std::size_t>
  locate_route(const MethodRoutes *primary, const MethodRoutes *secondary,
               std::string_view path, PathParams &parameters,
               PatternCaptures &captures, const Table &table) const {
    const MethodRoutes *buckets[2]{primary, secondary};
    constexpr std::size_t none = (std::numeric_limits<std::size_t>::max)();
    std::size_t literal_index = none;
    for (const auto *bucket : buckets) {
      if (bucket == nullptr) continue;
      const auto found = bucket->literal_first.find(path);
      if (found != bucket->literal_first.end() && found->second < literal_index)
        literal_index = found->second;
    }
    std::size_t positions[2]{0, 0};
    for (;;) {
      std::size_t next = none;
      for (std::size_t slot = 0; slot < 2; ++slot) {
        const auto *bucket = buckets[slot];
        if (bucket == nullptr || positions[slot] >= bucket->patterned.size())
          continue;
        next = std::min(next, bucket->patterned[positions[slot]]);
      }
      if (next == none || next >= literal_index) break;
      if (match_pattern(table[next].pattern, path, parameters, captures))
        return next;
      for (std::size_t slot = 0; slot < 2; ++slot) {
        const auto *bucket = buckets[slot];
        if (bucket == nullptr) continue;
        while (positions[slot] < bucket->patterned.size() &&
               bucket->patterned[positions[slot]] == next)
          ++positions[slot];
      }
    }
    if (literal_index != none) return literal_index;
    return std::nullopt;
  }

  const MethodRoutes *bucket_for(const MethodRoutesMap &map,
                                 std::string_view method) const {
    if (method.empty()) return nullptr;
    const auto found = map.find(method);
    return found == map.end() ? nullptr : &found->second;
  }

  std::optional<std::size_t> find_route(Request &request) const {
    const MethodKey key(request.method);
    const MethodRoutes *secondary = nullptr;
    if (key.view() == "head") {
      const auto found = routes_by_method.find("get");
      if (found != routes_by_method.end()) secondary = &found->second;
    }
    PathParams parameters;
    PatternCaptures captures;
    const auto matched = locate_route(bucket_for(routes_by_method, key.view()),
                                      secondary, request.path, parameters,
                                      captures, routes);
    if (!matched) return std::nullopt;
    request.path_params = std::move(parameters);
    return matched;
  }

  std::optional<std::size_t> find_stream_route(Request &request) const {
    const MethodKey key(request.method);
    PathParams parameters;
    PatternCaptures captures;
    const auto matched =
        locate_route(bucket_for(stream_routes_by_method, key.view()), nullptr,
                     request.path, parameters, captures, stream_routes);
    if (!matched) return std::nullopt;
    request.path_params = std::move(parameters);
    return matched;
  }

  std::optional<std::size_t> find_ws_route(Request &request) const {
    PathParams parameters;
    PatternCaptures captures;
    for (std::size_t index = 0; index < websocket_routes.size(); ++index) {
      if (match_pattern(websocket_routes[index].pattern, request.path,
                        parameters, captures)) {
        request.path_params = std::move(parameters);
        return index;
      }
      parameters.clear();
    }
    return std::nullopt;
  }

  bool serve_static(const Request &request, Response &response) const {
    if (!detail::iequals(request.method, "GET") &&
        !detail::iequals(request.method, "HEAD"))
      return false;
    for (const auto &mount : mounts) {
      if (!request.path.starts_with(mount.prefix)) continue;
      std::size_t begin = mount.prefix.size();
      while (begin < request.path.size() && request.path[begin] == '/') ++begin;
      auto path = mount.root /
                  std::filesystem::path(std::string_view(request.path).substr(begin));
      std::error_code ignored;
      if (std::filesystem::is_directory(path, ignored)) path /= "index.html";
      // The mount root was canonicalised at registration time, so only the
      // candidate needs resolving here.
      if (!std::filesystem::is_regular_file(path, ignored) ||
          !detail::path_is_within_canonical(mount.root, path))
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
      } else {
        bool served = false;
        if (!mounts.empty())
          co_await detail::run_file_io([&] { served = serve_static(request, response); });
        if (!served) {
          response.status = 404;
          (error_handler ? error_handler : default_error_handler)(request, response);
        }
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
    std::ifstream input;
    std::uint64_t size = 0;
    bool opened = false;
    co_await detail::run_file_io([&] {
      input.open(path, std::ios::binary | std::ios::ate);
      if (!input) return;
      const auto end = input.tellg();
      if (end < 0) { input.close(); return; }
      size = static_cast<std::uint64_t>(end);
      opened = true;
    });
    if (!opened) {
      response.status = 500;
      response.keep_alive = false;
      response.set_content("Unable to open response file");
      co_return co_await detail::write_response(connection, request, response,
                                                options.request_timeout);
    }
    // Keep cleanup outside the transfer coroutine so every read/write failure
    // closes the descriptor on the file pool before returning to the session.
    auto transfer = [&]() -> Task<ErrorInfo> {
      std::uint64_t first = 0;
      std::uint64_t last = size == 0 ? 0 : size - 1;
      // Borrow the request and response fields: a conditional file request must
      // not materialise three string copies just to compare them.
      const auto *requested_range = detail::find_header(request.headers, "Range");
      const auto *validator = detail::find_header(request.headers, "If-Range");
      const auto *etag = detail::find_header(response.headers, "ETag");
      // A missing/stale/weak validator must not splice bytes into an older
      // representation. Date validators conservatively use the full response.
      const bool validator_matches =
          validator == nullptr ||
          (validator->size() >= 2 && validator->front() == '"' &&
           validator->back() == '"' && etag != nullptr && *validator == *etag);
      if (response.status == 200 && detail::iequals(request.method, "GET") &&
          requested_range != nullptr &&
          detail::istarts_with(*requested_range, "bytes=") && validator_matches) {
        auto range = parse_range(*requested_range, size);
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
      if (error || detail::iequals(request.method, "HEAD") || length == 0 ||
          (response.status >= 100 && response.status < 200) ||
          response.status == 204 || response.status == 205 || response.status == 304)
        co_return error;
      std::uint64_t remaining = length;
      bool seek = true;
      // Each iteration costs a filesystem-pool hop plus a socket write, so larger
      // transfers are read in blocks that amortise both.
      constexpr std::uint64_t block_size = 256 * 1024;
      while (remaining > 0) {
        if (!connection->open())
          co_return ErrorInfo{Error::write, "File transfer connection closed"};
        std::string chunk;
        bool read_failed = false;
        co_await detail::run_file_io([&] {
          if (seek) {
            input.seekg(static_cast<std::streamoff>(first));
            seek = false;
          }
          chunk.resize(static_cast<std::size_t>(
              std::min<std::uint64_t>(remaining, block_size)));
          input.read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
          read_failed = input.bad() ||
              input.gcount() != static_cast<std::streamsize>(chunk.size());
        });
        if (read_failed)
          co_return ErrorInfo{Error::read, "Static file read was truncated"};
        remaining -= chunk.size();
        error = co_await connection->write(std::move(chunk), options.request_timeout);
        if (error) co_return error;
      }
      co_return ErrorInfo{};
    };
    ErrorInfo error;
    std::exception_ptr exception;
    try {
      error = co_await transfer();
    } catch (...) {
      exception = std::current_exception();
    }
    co_await detail::run_file_io([&] { input.close(); });
    if (exception) std::rethrow_exception(exception);
    co_return error;
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
        !detail::iequals(request.method, "HEAD") &&
        !(response.status >= 100 && response.status < 200) &&
        response.status != 204 && response.status != 205 && response.status != 304) {
      const auto *accept_encoding =
          detail::find_header(request.headers, "Accept-Encoding");
      const auto encoding =
          detail::select_encoding(accept_encoding ? *accept_encoding
                                                  : std::string_view{});
      if (!encoding.empty()) {
        Result<std::string> compressed{ErrorInfo{Error::internal, "Compression pending"}};
        co_await workers->execute([&] {
          compressed = detail::compress(response.body, encoding);
        });
        if (compressed) {
          response.body = std::move(*compressed);
          response.headers.set("Content-Encoding", encoding);
          if (!detail::has_token(response.headers, "Vary", "Accept-Encoding") &&
              !detail::has_token(response.headers, "Vary", "*"))
            response.headers.add("Vary", "Accept-Encoding");
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
  MethodRoutesMap routes_by_method;
  MethodRoutesMap stream_routes_by_method;
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
