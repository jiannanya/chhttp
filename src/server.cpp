#include "detail.hpp"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#ifdef CHHTTP_HAS_TLS
#include <boost/asio/ssl.hpp>
#include <boost/beast/ssl.hpp>
#include <openssl/ssl.h>
#endif

#include <charconv>
#include <condition_variable>
#include <fstream>
#include <limits>
#include <mutex>
#include <shared_mutex>

namespace chhttp {
namespace {

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using tcp = asio::ip::tcp;

struct CompiledPattern {
  std::regex expression;
  std::vector<std::string> names;
};

bool regex_special(char ch) {
  constexpr std::string_view special = R"(.^$|()[]{}+?\)";
  return special.find(ch) != std::string_view::npos;
}

CompiledPattern compile_pattern(std::string_view pattern) {
  if (!pattern.empty() && pattern.front() == '^') {
    return {std::regex(std::string(pattern),
                       std::regex::ECMAScript | std::regex::optimize),
            {}};
  }
  std::string output = "^";
  std::vector<std::string> names;
  for (std::size_t index = 0; index < pattern.size();) {
    if (pattern[index] == '{') {
      const auto close = pattern.find('}', index + 1);
      if (close != std::string_view::npos) {
        names.emplace_back(pattern.substr(index + 1, close - index - 1));
        output += "([^/]+)";
        index = close + 1;
        continue;
      }
    }
    if (pattern[index] == ':' &&
        (index == 0 || pattern[index - 1] == '/')) {
      auto end = index + 1;
      while (end < pattern.size() &&
             (std::isalnum(static_cast<unsigned char>(pattern[end])) ||
              pattern[end] == '_')) {
        ++end;
      }
      if (end > index + 1) {
        names.emplace_back(pattern.substr(index + 1, end - index - 1));
        output += "([^/]+)";
        index = end;
        continue;
      }
    }
    if (pattern[index] == '*') {
      names.emplace_back("wildcard");
      output += "(.*)";
    } else {
      if (regex_special(pattern[index])) output.push_back('\\');
      output.push_back(pattern[index]);
    }
    ++index;
  }
  output += "/?$";
  return {std::regex(output, std::regex::ECMAScript | std::regex::optimize),
          std::move(names)};
}

bool match_pattern(const CompiledPattern &pattern, std::string_view path,
                   PathParams &params) {
  std::match_results<std::string_view::const_iterator> matches;
  if (!std::regex_match(path.begin(), path.end(), matches,
                        pattern.expression)) {
    return false;
  }
  for (std::size_t index = 0;
       index < pattern.names.size() && index + 1 < matches.size(); ++index) {
    const std::string encoded(matches[index + 1].first,
                              matches[index + 1].second);
    auto decoded = url_decode(encoded);
    params[pattern.names[index]] = decoded ? std::move(*decoded) : std::string{};
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
      size == 0) {
    return std::nullopt;
  }
  value.remove_prefix(6);
  const auto dash = value.find('-');
  if (dash == std::string_view::npos) return std::nullopt;
  const auto parse_number = [](std::string_view text,
                               std::uint64_t &result) -> bool {
    if (text.empty()) return false;
    const auto converted =
        std::from_chars(text.data(), text.data() + text.size(), result);
    return converted.ec == std::errc{} &&
           converted.ptr == text.data() + text.size();
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

template <class Stream> class ChunkSink final : public StreamWriter::Sink {
public:
  explicit ChunkSink(Stream &stream) : stream_(stream) {}

  asio::awaitable<bool> write(std::string data) override {
    if (!open_) co_return false;
    boost::system::error_code ec;
    co_await asio::async_write(stream_, http::make_chunk(asio::buffer(data)),
                               asio::redirect_error(asio::use_awaitable, ec));
    if (ec) open_ = false;
    co_return !ec;
  }

  asio::awaitable<bool> flush() override { co_return open_; }
  bool open() const noexcept override { return open_; }
  void close() noexcept { open_ = false; }

private:
  Stream &stream_;
  std::atomic_bool open_{true};
};

template <class NextLayer>
class BeastWebSocketChannel final : public WebSocket::Channel {
public:
  explicit BeastWebSocketChannel(NextLayer stream)
      : stream_(std::move(stream)) {
    stream_.read_message_max(64 * 1024 * 1024);
    stream_.set_option(websocket::stream_base::timeout::suggested(
        beast::role_type::server));
  }

  asio::awaitable<bool>
  accept(http::request<http::string_body> request,
         std::string selected_protocol) {
    selected_protocol_ = std::move(selected_protocol);
    if (!selected_protocol_.empty()) {
      stream_.set_option(websocket::stream_base::decorator(
          [selected = selected_protocol_](websocket::response_type &response) {
            response.set(http::field::sec_websocket_protocol, selected);
          }));
    }
    boost::system::error_code ec;
    co_await stream_.async_accept(request,
                                  asio::redirect_error(asio::use_awaitable, ec));
    open_ = !ec;
    co_return !ec;
  }

  asio::awaitable<Result<WebSocket::Message>> read() override {
    if (!open_) {
      co_return ErrorInfo{Error::websocket_closed, "WebSocket is closed"};
    }
    beast::flat_buffer buffer;
    boost::system::error_code ec;
    co_await stream_.async_read(buffer,
                                asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      open_ = false;
      if (ec == websocket::error::closed) {
        co_return ErrorInfo{Error::websocket_closed, "Peer closed WebSocket",
                            ec.value()};
      }
      co_return detail::make_error(Error::read, "WebSocket read failed", ec);
    }
    WebSocket::Message message;
    message.type = stream_.got_text() ? WebSocket::MessageType::text
                                      : WebSocket::MessageType::binary;
    message.data = beast::buffers_to_string(buffer.data());
    co_return message;
  }

  asio::awaitable<bool> send(std::string data, bool binary) override {
    if (!open_) co_return false;
    stream_.binary(binary);
    boost::system::error_code ec;
    co_await stream_.async_write(asio::buffer(data),
                                 asio::redirect_error(asio::use_awaitable, ec));
    if (ec) open_ = false;
    co_return !ec;
  }

  asio::awaitable<void> ping(std::string data) override {
    if (!open_) co_return;
    boost::system::error_code ec;
    co_await stream_.async_ping(websocket::ping_data(std::move(data)),
                                asio::redirect_error(asio::use_awaitable, ec));
    if (ec) open_ = false;
  }

  asio::awaitable<void> close(std::uint16_t code, std::string reason) override {
    if (!open_) co_return;
    websocket::close_reason close_reason;
    close_reason.code = static_cast<websocket::close_code>(code);
    close_reason.reason = std::move(reason);
    boost::system::error_code ec;
    co_await stream_.async_close(close_reason,
                                 asio::redirect_error(asio::use_awaitable, ec));
    open_ = false;
  }

  bool open() const noexcept override { return open_; }
  std::string subprotocol() const override { return selected_protocol_; }

private:
  websocket::stream<NextLayer> stream_;
  std::atomic_bool open_{false};
  std::string selected_protocol_;
};

template <class Stream>
void set_timeout(Stream &stream, std::chrono::seconds timeout) {
  beast::get_lowest_layer(stream).expires_after(timeout);
}

template <class Stream> void shutdown_stream(Stream &stream) {
  boost::system::error_code ignored;
  beast::get_lowest_layer(stream).socket().shutdown(tcp::socket::shutdown_both,
                                                    ignored);
  beast::get_lowest_layer(stream).socket().close(ignored);
}

} // namespace

class Server::Impl {
public:
  struct SessionControl {
    std::atomic_bool in_request{false};
    std::function<void()> cancel_if_idle;
  };

  struct Route {
    std::string method;
    CompiledPattern pattern;
    AsyncHandler handler;
  };
  struct WsRoute {
    CompiledPattern pattern;
    WebSocketHandler handler;
    SubprotocolSelector selector;
  };
  struct Mount {
    std::string prefix;
    std::filesystem::path root;
    Headers headers;
  };

  explicit Impl(ServerOptions server_options)
      : options(std::move(server_options)), io(), acceptor(io)
#ifdef CHHTTP_HAS_TLS
        , ssl_context(asio::ssl::context::tls_server)
#endif
  {
    if (options.worker_threads == 0) {
      options.worker_threads = std::max<std::size_t>(1, std::thread::hardware_concurrency());
    }
    default_error_handler = [](const Request &, Response &response) {
      if (response.status == 404)
        response.set_content("Not Found", "text/plain; charset=utf-8");
      else
        response.set_content(status_reason(response.status),
                             "text/plain; charset=utf-8");
    };
    default_exception_handler = [](const Request &, Response &response,
                                   std::exception_ptr) {
      response.status = 500;
      response.set_content("Internal Server Error",
                           "text/plain; charset=utf-8");
    };
  }

  ~Impl() {
    stop();
    join();
  }

  bool configure_tls() {
    if (!options.tls) return true;
#ifndef CHHTTP_HAS_TLS
    last_start_error = "TLS was disabled at build time";
    return false;
#else
    try {
      auto &tls = *options.tls;
      ssl_context.set_options(asio::ssl::context::default_workarounds |
                              asio::ssl::context::no_sslv2 |
                              asio::ssl::context::no_sslv3 |
                              asio::ssl::context::single_dh_use);
      if (!tls.ciphers.empty() &&
          SSL_CTX_set_cipher_list(ssl_context.native_handle(),
                                  tls.ciphers.c_str()) != 1) {
        last_start_error = "Invalid TLS cipher list";
        return false;
      }
      if (!tls.certificate_pem.empty()) {
        ssl_context.use_certificate_chain(
            asio::buffer(tls.certificate_pem.data(), tls.certificate_pem.size()));
      } else {
        ssl_context.use_certificate_chain_file(tls.certificate_file.string());
      }
      if (!tls.private_key_pem.empty()) {
        ssl_context.use_private_key(
            asio::buffer(tls.private_key_pem.data(), tls.private_key_pem.size()),
            asio::ssl::context::pem);
      } else {
        ssl_context.use_private_key_file(tls.private_key_file.string(),
                                         asio::ssl::context::pem);
      }
      if (!tls.client_ca_file.empty()) {
        ssl_context.load_verify_file(tls.client_ca_file.string());
      }
      if (tls.require_client_certificate) {
        ssl_context.set_verify_mode(asio::ssl::verify_peer |
                                    asio::ssl::verify_fail_if_no_peer_cert);
      }
      return true;
    } catch (const std::exception &exception) {
      last_start_error = exception.what();
      return false;
    }
#endif
  }

  bool start(std::string host, std::uint16_t requested_port) {
    std::lock_guard lock(state_mutex);
    if (started) return false;
    if (!configure_tls()) return false;
    io.restart();
    boost::system::error_code ec;
    const auto address = asio::ip::make_address(host, ec);
    if (ec) {
      last_start_error = ec.message();
      return false;
    }
    tcp::endpoint endpoint(address, requested_port);
    acceptor.open(endpoint.protocol(), ec);
    if (!ec && options.reuse_address)
      acceptor.set_option(asio::socket_base::reuse_address(true), ec);
    if (!ec) acceptor.bind(endpoint, ec);
    if (!ec) acceptor.listen(asio::socket_base::max_listen_connections, ec);
    if (ec) {
      last_start_error = ec.message();
      boost::system::error_code ignored;
      acceptor.close(ignored);
      return false;
    }
    bound_port = acceptor.local_endpoint().port();
    started = true;
    stopping = false;
    session_started();
    asio::co_spawn(io, accept_loop(), asio::detached);
    threads.reserve(options.worker_threads);
    for (std::size_t i = 0; i < options.worker_threads; ++i) {
      threads.emplace_back([this] { io.run(); });
    }
    return true;
  }

  void join() {
    if (io.get_executor().running_in_this_thread()) return;
    for (auto &thread : threads) {
      if (thread.joinable() && thread.get_id() != std::this_thread::get_id())
        thread.join();
    }
    std::erase_if(threads, [](std::thread &thread) { return !thread.joinable(); });
  }

  void stop() {
    bool expected = false;
    if (!stopping.compare_exchange_strong(expected, true)) return;
    if (!started) {
      io.stop();
      return;
    }
    asio::dispatch(io, [this] {
      boost::system::error_code ignored;
      acceptor.cancel(ignored);
      acceptor.close(ignored);
      cancel_idle_sessions();
      started = false;
      state_cv.notify_all();
      if (active_sessions == 0) io.stop();
    });
    if (!io.get_executor().running_in_this_thread()) {
      std::unique_lock lock(state_mutex);
      if (!state_cv.wait_for(lock, options.shutdown_timeout,
                             [this] { return active_sessions == 0; })) {
        io.stop();
      }
    }
  }

  void session_started() noexcept { ++active_sessions; }

  void register_session(const std::shared_ptr<SessionControl> &control) {
    std::lock_guard lock(sessions_mutex);
    sessions.emplace_back(control);
  }

  void unregister_session(const SessionControl *control) {
    std::lock_guard lock(sessions_mutex);
    std::erase_if(sessions, [&](const auto &candidate) {
      const auto locked = candidate.lock();
      return !locked || locked.get() == control;
    });
  }

  void cancel_idle_sessions() {
    std::vector<std::shared_ptr<SessionControl>> controls;
    {
      std::lock_guard lock(sessions_mutex);
      for (auto &candidate : sessions) {
        if (auto control = candidate.lock()) controls.push_back(std::move(control));
      }
    }
    for (const auto &control : controls) {
      if (!control->in_request && control->cancel_if_idle)
        control->cancel_if_idle();
    }
  }

  void session_finished() noexcept {
    if (active_sessions.fetch_sub(1) == 1) {
      state_cv.notify_all();
      if (stopping) io.stop();
    }
  }

  template <class Stream>
  asio::awaitable<void> run_session(Stream stream) {
    auto shared_stream = std::make_shared<Stream>(std::move(stream));
    auto control = std::make_shared<SessionControl>();
    const auto stream_executor = beast::get_lowest_layer(*shared_stream).get_executor();
    std::weak_ptr<Stream> weak_stream = shared_stream;
    std::weak_ptr<SessionControl> weak_control = control;
    control->cancel_if_idle = [weak_stream, weak_control, stream_executor] {
      asio::dispatch(stream_executor, [weak_stream, weak_control] {
        const auto control = weak_control.lock();
        if (!control || control->in_request) return;
        if (auto stream = weak_stream.lock())
          beast::get_lowest_layer(*stream).cancel();
      });
    };
    register_session(control);
    try {
      co_await serve(*shared_stream, control);
    } catch (...) {
    }
    unregister_session(control.get());
    session_finished();
  }

  asio::awaitable<void> accept_loop() {
    while (!stopping) {
      boost::system::error_code ec;
      tcp::socket socket(asio::make_strand(io));
      co_await acceptor.async_accept(socket,
                                     asio::redirect_error(asio::use_awaitable, ec));
      if (ec) {
        if (ec == asio::error::operation_aborted || stopping) break;
        continue;
      }
      if (options.tcp_no_delay) {
        socket.set_option(tcp::no_delay(true), ec);
      }
      beast::tcp_stream tcp_stream(std::move(socket));
      const auto session_executor = tcp_stream.get_executor();
#ifdef CHHTTP_HAS_TLS
      if (options.tls) {
        session_started();
        asio::co_spawn(session_executor, accept_tls(std::move(tcp_stream)),
                       asio::detached);
        continue;
      }
#endif
      session_started();
      asio::co_spawn(session_executor, run_session(std::move(tcp_stream)),
                     asio::detached);
    }
    session_finished();
  }

#ifdef CHHTTP_HAS_TLS
  asio::awaitable<void> accept_tls(beast::tcp_stream tcp_stream) {
    beast::ssl_stream<beast::tcp_stream> stream(std::move(tcp_stream),
                                                ssl_context);
    set_timeout(stream, options.request_timeout);
    boost::system::error_code ec;
    co_await stream.async_handshake(
        asio::ssl::stream_base::server,
        asio::redirect_error(asio::use_awaitable, ec));
    if (!ec) {
      co_await run_session(std::move(stream));
      co_return;
    }
    session_finished();
  }
#endif

  std::optional<std::size_t> find_ws_route(Request &request) const {
    for (std::size_t index = 0; index < websocket_routes.size(); ++index) {
      PathParams params;
      if (match_pattern(websocket_routes[index].pattern, request.path, params)) {
        request.path_params = std::move(params);
        return index;
      }
    }
    return std::nullopt;
  }

  std::optional<std::size_t> find_route(Request &request) const {
    for (std::size_t index = 0; index < routes.size(); ++index) {
      if (!detail::iequals(routes[index].method, request.method) &&
          !(detail::iequals(request.method, "HEAD") &&
            detail::iequals(routes[index].method, "GET"))) {
        continue;
      }
      PathParams params;
      if (match_pattern(routes[index].pattern, request.path, params)) {
        request.path_params = std::move(params);
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
      auto decoded = url_decode(relative);
      if (!decoded) return false;
      auto path = mount.root / std::filesystem::path(*decoded);
      if (std::filesystem::is_directory(path)) path /= "index.html";
      if (!std::filesystem::is_regular_file(path) ||
          !detail::path_is_within(mount.root, path))
        return false;
      response.set_file(path);
      for (const auto &[name, value] : mount.headers) response.headers.set(name, value);
      response.headers.set("Accept-Ranges", "bytes");
      return true;
    }
    return false;
  }

  asio::awaitable<void> dispatch(Request &request, Response &response) {
    try {
      if (pre_routing && pre_routing(request, response)) co_return;
      if (const auto route = find_route(request)) {
        co_await routes[*route].handler(request, response);
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

  template <class Stream>
  asio::awaitable<bool> write_file(Stream &stream, const Request &request,
                                   Response &response) {
    auto &path = *ServerAccess::file(response);
    boost::system::error_code ec;
    std::error_code filesystem_error;
    const auto file_size = std::filesystem::file_size(path, filesystem_error);
    if (filesystem_error) {
      response.status = 404;
      ServerAccess::file(response).reset();
      (error_handler ? error_handler : default_error_handler)(request, response);
      co_return co_await write_regular(stream, request, response);
    }
    std::optional<ByteRange> range;
    if (request.headers.contains("Range")) {
      range = parse_range(request.headers.get("Range"), file_size);
      if (!range) {
        http::response<http::empty_body> invalid{
            http::status::range_not_satisfiable, request.version};
        invalid.set(http::field::content_range,
                    "bytes */" + std::to_string(file_size));
        invalid.content_length(0);
        invalid.keep_alive(request.keep_alive);
        co_await http::async_write(stream, invalid,
                                   asio::redirect_error(asio::use_awaitable, ec));
        co_return !ec;
      }
    }
    if (range) {
      const auto length = range->last - range->first + 1;
      http::response<http::vector_body<char>> partial{
          http::status::partial_content, request.version};
      detail::to_beast_headers(response.headers, partial.base());
      partial.set(http::field::server, "chhttp/0.1");
      partial.set(http::field::content_range,
                  "bytes " + std::to_string(range->first) + "-" +
                      std::to_string(range->last) + "/" +
                      std::to_string(file_size));
      partial.keep_alive(response.keep_alive && request.keep_alive);
      if (!detail::iequals(request.method, "HEAD")) {
        std::ifstream input(path, std::ios::binary);
        if (!input) co_return false;
        input.seekg(static_cast<std::streamoff>(range->first));
        partial.body().resize(static_cast<std::size_t>(length));
        input.read(partial.body().data(), static_cast<std::streamsize>(length));
        if (static_cast<std::uint64_t>(input.gcount()) != length) co_return false;
      }
      partial.content_length(length);
      co_await http::async_write(stream, partial,
                                 asio::redirect_error(asio::use_awaitable, ec));
      co_return !ec;
    }
    http::response<http::file_body> output{
        static_cast<http::status>(response.status),
        request.version};
    detail::to_beast_headers(response.headers, output.base());
    output.set(http::field::server, "chhttp/0.1");
    output.keep_alive(response.keep_alive && request.keep_alive);
    output.body().open(path.string().c_str(), beast::file_mode::scan, ec);
    if (ec) co_return false;
    output.content_length(file_size);
    if (detail::iequals(request.method, "HEAD")) {
      http::response<http::empty_body> head{
          static_cast<http::status>(output.result_int()), request.version};
      head.base() = output.base();
      head.content_length(file_size);
      co_await http::async_write(stream, head,
                                 asio::redirect_error(asio::use_awaitable, ec));
    } else {
      co_await http::async_write(stream, output,
                                 asio::redirect_error(asio::use_awaitable, ec));
    }
    co_return !ec;
  }

  template <class Stream>
  asio::awaitable<bool> write_stream(Stream &stream, const Request &request,
                                     Response &response) {
    boost::system::error_code ec;
    http::response<http::empty_body> output{
        static_cast<http::status>(response.status), request.version};
    detail::to_beast_headers(response.headers, output.base());
    output.set(http::field::server, "chhttp/0.1");
    output.chunked(true);
    output.keep_alive(response.keep_alive && request.keep_alive);
    http::response_serializer<http::empty_body> serializer(output);
    co_await http::async_write_header(
        stream, serializer, asio::redirect_error(asio::use_awaitable, ec));
    if (ec || detail::iequals(request.method, "HEAD")) co_return !ec;
    auto sink = std::make_shared<ChunkSink<Stream>>(stream);
    StreamWriter writer(sink);
    try {
      co_await ServerAccess::stream(response)(writer);
    } catch (...) {
      sink->close();
      co_return false;
    }
    if (!sink->open()) co_return false;
    co_await asio::async_write(stream, http::make_chunk_last(),
                               asio::redirect_error(asio::use_awaitable, ec));
    sink->close();
    co_return !ec;
  }

  template <class Stream>
  asio::awaitable<bool> write_regular(Stream &stream, const Request &request,
                                      Response &response) {
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
    http::response<http::string_body> output{
        static_cast<http::status>(response.status), request.version};
    detail::to_beast_headers(response.headers, output.base());
    output.set(http::field::server, "chhttp/0.1");
    output.keep_alive(response.keep_alive && request.keep_alive);
    output.body() = detail::iequals(request.method, "HEAD") ? std::string{}
                                                            : response.body;
    if (detail::iequals(request.method, "HEAD"))
      output.content_length(response.body.size());
    else
      output.prepare_payload();
    boost::system::error_code ec;
    co_await http::async_write(stream, output,
                               asio::redirect_error(asio::use_awaitable, ec));
    co_return !ec;
  }

  template <class Stream>
  asio::awaitable<bool> write_response(Stream &stream, const Request &request,
                                       Response &response) {
    if (ServerAccess::file(response))
      co_return co_await write_file(stream, request, response);
    if (response.is_streaming())
      co_return co_await write_stream(stream, request, response);
    co_return co_await write_regular(stream, request, response);
  }

  template <class Stream>
  asio::awaitable<void> handle_websocket(
      Stream stream, http::request<http::string_body> raw_request,
      Request request, std::size_t route_index) {
    auto &route = websocket_routes[route_index];
    std::string selected;
    if (route.selector) {
      selected = route.selector(detail::split_tokens(
          request.headers.get("Sec-WebSocket-Protocol"), ','));
    }
    auto channel =
        std::make_shared<BeastWebSocketChannel<Stream>>(std::move(stream));
    if (!co_await channel->accept(std::move(raw_request), selected)) co_return;
    WebSocket socket(channel);
    bool handler_failed = false;
    try {
      co_await route.handler(request, socket);
    } catch (...) {
      handler_failed = true;
    }
    if (handler_failed) {
      co_await socket.close(1011, "Handler error");
      co_return;
    }
    if (socket.open()) co_await socket.close();
  }

  template <class Stream>
  asio::awaitable<void>
  serve(Stream &stream, const std::shared_ptr<SessionControl> &control) {
    beast::flat_buffer buffer;
    std::size_t requests = 0;
    while (!stopping && requests < options.keep_alive_max_requests) {
      control->in_request = false;
      set_timeout(stream, requests == 0 ? options.request_timeout
                                        : options.keep_alive_timeout);
      http::request_parser<http::string_body> parser;
      parser.body_limit(options.max_body_size);
      parser.header_limit(static_cast<std::uint32_t>(std::min<std::size_t>(
          options.max_header_size, std::numeric_limits<std::uint32_t>::max())));
      boost::system::error_code ec;
      co_await http::async_read(stream, buffer, parser,
                                asio::redirect_error(asio::use_awaitable, ec));
      if (ec == http::error::end_of_stream || ec == asio::error::eof) break;
      if (ec) {
        if (ec == http::error::body_limit) {
          control->in_request = true;
          http::response<http::string_body> too_large{
              http::status::payload_too_large, 11};
          too_large.body() = "Payload Too Large";
          too_large.prepare_payload();
          co_await http::async_write(
              stream, too_large, asio::redirect_error(asio::use_awaitable, ec));
        }
        break;
      }
      auto raw = parser.release();
      control->in_request = true;
      Request request = detail::from_beast_request(raw);
      const auto endpoint = beast::get_lowest_layer(stream).socket().remote_endpoint(ec);
      if (!ec) {
        request.remote_address = endpoint.address().to_string();
        request.remote_port = endpoint.port();
      }
      if (options.auto_decompress_request &&
          request.headers.contains("Content-Encoding")) {
        auto decoded = detail::decompress(request.body,
                                          request.headers.get("Content-Encoding"),
                                          options.max_body_size);
        if (!decoded) {
          Response bad;
          bad.status = decoded.error().code == Error::body_too_large ? 413 : 400;
          bad.set_content(decoded.error().message);
          co_await write_response(stream, request, bad);
          break;
        }
        request.body = std::move(*decoded);
        request.headers.erase("Content-Encoding");
      }
      const auto content_type = request.headers.get("Content-Type");
      if (detail::lower(content_type).starts_with("multipart/form-data")) {
        auto parts = parse_multipart(request.body, content_type);
        if (parts) request.files = std::move(*parts);
      }
      if (websocket::is_upgrade(raw)) {
        if (const auto route = find_ws_route(request)) {
          co_await handle_websocket(std::move(stream), std::move(raw),
                                    std::move(request), *route);
        } else {
          Response not_found;
          not_found.status = 404;
          default_error_handler(request, not_found);
          co_await write_response(stream, request, not_found);
        }
        co_return;
      }
      Response response;
      response.version = request.version;
      response.keep_alive = request.keep_alive;
      co_await dispatch(request, response);
      if (requests + 1 >= options.keep_alive_max_requests)
        response.keep_alive = false;
      const bool keep_alive = response.keep_alive && request.keep_alive;
      if (!co_await write_response(stream, request, response)) break;
      if (logger) {
        try { logger(request, response); } catch (...) {}
      }
      ++requests;
      if (!keep_alive) break;
    }
#ifdef CHHTTP_HAS_TLS
    if constexpr (requires { stream.async_shutdown(asio::use_awaitable); }) {
      if (stopping) {
        shutdown_stream(stream);
        co_return;
      }
      boost::system::error_code ignored;
      co_await stream.async_shutdown(
          asio::redirect_error(asio::use_awaitable, ignored));
    }
#endif
    shutdown_stream(stream);
  }

  ServerOptions options;
  asio::io_context io;
  tcp::acceptor acceptor;
#ifdef CHHTTP_HAS_TLS
  asio::ssl::context ssl_context;
#endif
  std::vector<Route> routes;
  std::vector<WsRoute> websocket_routes;
  std::vector<Mount> mounts;
  Middleware pre_routing;
  Middleware post_routing;
  ErrorHandler error_handler;
  ErrorHandler default_error_handler;
  ExceptionHandler exception_handler;
  ExceptionHandler default_exception_handler;
  Logger logger;
  std::vector<std::thread> threads;
  mutable std::mutex state_mutex;
  std::condition_variable state_cv;
  std::atomic_bool started{false};
  std::atomic_bool stopping{false};
  std::atomic_size_t active_sessions{0};
  std::mutex sessions_mutex;
  std::vector<std::weak_ptr<SessionControl>> sessions;
  std::uint16_t bound_port{0};
  std::string last_start_error;
};

Server::Server(ServerOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}
Server::~Server() = default;
Server::Server(Server &&) noexcept = default;
Server &Server::operator=(Server &&) noexcept = default;

Server &Server::route(std::string method, std::string pattern,
                      Handler handler) {
  return route_async(
      std::move(method), std::move(pattern),
      [handler = std::move(handler)](const Request &request,
                                     Response &response) -> asio::awaitable<void> {
        handler(request, response);
        co_return;
      });
}

Server &Server::route_async(std::string method, std::string pattern,
                            AsyncHandler handler) {
  if (impl_->started) throw std::logic_error("Routes cannot be changed while running");
  impl_->routes.push_back({detail::lower(method), compile_pattern(pattern),
                           std::move(handler)});
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

Server &Server::websocket(std::string pattern, WebSocketHandler handler,
                          SubprotocolSelector selector) {
  if (impl_->started) throw std::logic_error("Routes cannot be changed while running");
  impl_->websocket_routes.push_back(
      {compile_pattern(pattern), std::move(handler), std::move(selector)});
  return *this;
}

Server &Server::mount(std::string url_prefix,
                      std::filesystem::path directory,
                      Headers default_headers) {
  if (impl_->started) throw std::logic_error("Mounts cannot be changed while running");
  if (url_prefix.empty() || url_prefix.front() != '/')
    throw std::invalid_argument("Mount prefix must start with '/'");
  impl_->mounts.push_back({std::move(url_prefix),
                           std::filesystem::weakly_canonical(directory),
                           std::move(default_headers)});
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
Server &Server::set_logger(Logger value) {
  impl_->logger = std::move(value);
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
  lock.unlock();
  impl_->join();
}

void Server::stop() {
  impl_->stop();
  impl_->join();
}

bool Server::running() const noexcept { return impl_->started; }
std::uint16_t Server::port() const noexcept { return impl_->bound_port; }
asio::any_io_executor Server::executor() const { return impl_->io.get_executor(); }

} // namespace chhttp
