#include "detail.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#ifdef CHHTTP_HAS_TLS
#include <boost/asio/ssl.hpp>
#include <boost/beast/ssl.hpp>
#include <openssl/err.h>
#include <openssl/ssl.h>
#ifdef _WIN32
#include <windows.h>
#include <wincrypt.h>
#endif
#endif

#include <array>
#include <mutex>

namespace chhttp {
namespace {

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using tcp = asio::ip::tcp;

std::string origin(const detail::ParsedUrl &url) {
  std::string value = url.scheme + "://";
  if (url.host.find(':') != std::string::npos) value += "[" + url.host + "]";
  else value += url.host;
  if ((!url.secure && url.port != 80) || (url.secure && url.port != 443))
    value += ":" + std::to_string(url.port);
  return value;
}

std::string host_header(const detail::ParsedUrl &url) {
  std::string value = url.host;
  if ((!url.secure && url.port != 80) || (url.secure && url.port != 443))
    value += ":" + std::to_string(url.port);
  return value;
}

Error map_network_error(const boost::system::error_code &ec,
                        Error fallback) {
  if (ec == asio::error::operation_aborted) return Error::cancelled;
  if (ec == beast::error::timeout || ec == asio::error::timed_out)
    return Error::timeout;
  if (ec == http::error::body_limit) return Error::body_too_large;
  return fallback;
}

template <class Stream>
void set_timeout(Stream &stream, std::chrono::milliseconds timeout) {
  beast::get_lowest_layer(stream).expires_after(timeout);
}

template <class Stream>
asio::awaitable<ResponseResult>
exchange(Stream &stream, http::request<http::string_body> outgoing,
         const ClientOptions &client_options,
         const RequestOptions &request_options,
         std::uint64_t generation,
         const std::atomic_uint64_t &cancel_generation) {
  if ((request_options.cancellation && *request_options.cancellation) ||
      generation != cancel_generation.load()) {
    co_return ErrorInfo{Error::cancelled, "Request cancelled"};
  }
  boost::system::error_code ec;
  set_timeout(stream, client_options.write_timeout);
  co_await http::async_write(stream, outgoing,
                             asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    co_return detail::make_error(map_network_error(ec, Error::write),
                                 "HTTP request write failed", ec);
  }

  beast::flat_buffer buffer;
  http::response_parser<http::buffer_body> parser;
  parser.body_limit(client_options.max_response_body_size);
  if (outgoing.method() == http::verb::head) parser.skip(true);
  set_timeout(stream, client_options.read_timeout);
  co_await http::async_read_header(
      stream, buffer, parser, asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    co_return detail::make_error(map_network_error(ec, Error::read),
                                 "HTTP response header read failed", ec);
  }

  Response response;
  response.status = parser.get().result_int();
  response.version = parser.get().version();
  response.headers = detail::from_beast_headers(parser.get().base());
  response.keep_alive = parser.get().keep_alive();
  const auto total = parser.content_length().value_or(0);
  std::uint64_t current = 0;
  std::array<char, 32 * 1024> chunk{};
  while (!parser.is_done()) {
    if ((request_options.cancellation && *request_options.cancellation) ||
        generation != cancel_generation.load()) {
      beast::get_lowest_layer(stream).cancel();
      co_return ErrorInfo{Error::cancelled, "Request cancelled"};
    }
    parser.get().body().data = chunk.data();
    parser.get().body().size = chunk.size();
    co_await http::async_read_some(
        stream, buffer, parser, asio::redirect_error(asio::use_awaitable, ec));
    const auto received = chunk.size() - parser.get().body().size;
    current += received;
    if (received != 0) {
      if (request_options.on_data) {
        if (!request_options.on_data(std::string_view(chunk.data(), received))) {
          beast::get_lowest_layer(stream).cancel();
          co_return ErrorInfo{Error::cancelled,
                              "Response body callback stopped the request"};
        }
      } else {
        response.body.append(chunk.data(), received);
      }
    }
    if (request_options.on_progress &&
        !request_options.on_progress(current, total)) {
      beast::get_lowest_layer(stream).cancel();
      co_return ErrorInfo{Error::cancelled,
                          "Progress callback stopped the request"};
    }
    if (ec == http::error::need_buffer) ec.clear();
    if (ec) {
      co_return detail::make_error(map_network_error(ec, Error::read),
                                   "HTTP response body read failed", ec);
    }
  }
  co_return response;
}

#ifdef CHHTTP_HAS_TLS
bool load_windows_root_certificates(asio::ssl::context &context) {
#ifdef _WIN32
  HCERTSTORE store = CertOpenSystemStoreA(0, "ROOT");
  if (!store) return false;
  PCCERT_CONTEXT cert = nullptr;
  X509_STORE *x509_store = SSL_CTX_get_cert_store(context.native_handle());
  while ((cert = CertEnumCertificatesInStore(store, cert)) != nullptr) {
    const unsigned char *encoded = cert->pbCertEncoded;
    X509 *certificate = d2i_X509(nullptr, &encoded, cert->cbCertEncoded);
    if (certificate) {
      X509_STORE_add_cert(x509_store, certificate);
      X509_free(certificate);
      ERR_clear_error();
    }
  }
  CertCloseStore(store, 0);
  return true;
#else
  (void)context;
  return false;
#endif
}

Result<bool> configure_tls_context(asio::ssl::context &context,
                                   const TlsClientOptions &tls) {
  try {
    context.set_options(asio::ssl::context::default_workarounds |
                        asio::ssl::context::no_sslv2 |
                        asio::ssl::context::no_sslv3);
    if (tls.verify_peer) {
      context.set_verify_mode(asio::ssl::verify_peer);
      if (tls.use_system_certificates) {
        boost::system::error_code ignored;
        context.set_default_verify_paths(ignored);
        load_windows_root_certificates(context);
      }
      if (!tls.ca_file.empty()) context.load_verify_file(tls.ca_file.string());
      if (!tls.ca_directory.empty())
        context.add_verify_path(tls.ca_directory.string());
    } else {
      context.set_verify_mode(asio::ssl::verify_none);
    }
    if (!tls.ciphers.empty() &&
        SSL_CTX_set_cipher_list(context.native_handle(), tls.ciphers.c_str()) !=
            1) {
      return ErrorInfo{Error::tls_configuration, "Invalid TLS cipher list",
                       0, static_cast<long>(ERR_get_error())};
    }
    if (!tls.certificate_pem.empty()) {
      context.use_certificate_chain(
          asio::buffer(tls.certificate_pem.data(), tls.certificate_pem.size()));
    } else if (!tls.certificate_file.empty()) {
      context.use_certificate_chain_file(tls.certificate_file.string());
    }
    if (!tls.private_key_pem.empty()) {
      context.use_private_key(
          asio::buffer(tls.private_key_pem.data(), tls.private_key_pem.size()),
          asio::ssl::context::pem);
    } else if (!tls.private_key_file.empty()) {
      context.use_private_key_file(tls.private_key_file.string(),
                                   asio::ssl::context::pem);
    }
    return true;
  } catch (const std::exception &exception) {
    return ErrorInfo{Error::tls_configuration, exception.what(), 0,
                     static_cast<long>(ERR_get_error())};
  }
}

template <class SslStream>
asio::awaitable<ErrorInfo> tls_handshake(SslStream &stream,
                                         const detail::ParsedUrl &url,
                                         const ClientOptions &options) {
  const std::string server_name =
      options.tls.server_name.empty() ? url.host : options.tls.server_name;
  if (SSL_set_tlsext_host_name(stream.native_handle(), server_name.c_str()) !=
      1) {
    co_return ErrorInfo{Error::tls_configuration, "Unable to configure TLS SNI",
                        0, static_cast<long>(ERR_get_error())};
  }
  if (options.tls.verify_peer) {
    stream.set_verify_callback(asio::ssl::host_name_verification(server_name));
  }
  set_timeout(stream, options.connect_timeout);
  boost::system::error_code ec;
  co_await stream.async_handshake(
      asio::ssl::stream_base::client,
      asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    const long verification = SSL_get_verify_result(stream.native_handle());
    const auto error = verification == X509_V_OK ? Error::tls_handshake
                                                  : Error::tls_verification;
    co_return ErrorInfo{error,
                        "TLS handshake failed: " + ec.message(), ec.value(),
                        verification == X509_V_OK
                            ? static_cast<long>(ERR_get_error())
                            : verification};
  }
  co_return ErrorInfo{};
}
#endif

template <class NextLayer>
class ClientWebSocketChannel final : public WebSocket::Channel {
public:
  explicit ClientWebSocketChannel(NextLayer stream)
      : stream_(std::move(stream)) {
    stream_.read_message_max(64 * 1024 * 1024);
    stream_.set_option(websocket::stream_base::timeout::suggested(
        beast::role_type::client));
  }

  asio::awaitable<ErrorInfo> handshake(const detail::ParsedUrl &url,
                                       const Headers &headers) {
    stream_.set_option(websocket::stream_base::decorator(
        [headers](websocket::request_type &request) {
          detail::to_beast_headers(headers, request.base());
          request.set(http::field::user_agent, "chhttp/0.1");
        }));
    websocket::response_type response;
    boost::system::error_code ec;
    co_await stream_.async_handshake(
        response, host_header(url), url.target,
        asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      co_return detail::make_error(Error::websocket_handshake,
                                   "WebSocket handshake failed", ec);
    }
    selected_protocol_ = std::string(response[http::field::sec_websocket_protocol]);
    open_ = true;
    co_return ErrorInfo{};
  }

  asio::awaitable<Result<WebSocket::Message>> read() override {
    if (!open_) co_return ErrorInfo{Error::websocket_closed, "WebSocket is closed"};
    beast::flat_buffer buffer;
    boost::system::error_code ec;
    co_await stream_.async_read(buffer,
                                asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      open_ = false;
      co_return detail::make_error(
          ec == websocket::error::closed ? Error::websocket_closed : Error::read,
          "WebSocket read failed", ec);
    }
    WebSocket::Message result{
        stream_.got_text() ? WebSocket::MessageType::text
                           : WebSocket::MessageType::binary,
        beast::buffers_to_string(buffer.data())};
    co_return result;
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

class PooledConnectionBase {
public:
  explicit PooledConnectionBase(std::string key) : key_(std::move(key)) {}
  virtual ~PooledConnectionBase() = default;
  PooledConnectionBase(const PooledConnectionBase &) = delete;
  PooledConnectionBase &operator=(const PooledConnectionBase &) = delete;

  virtual asio::awaitable<ResponseResult>
  transact(http::request<http::string_body> request,
           const ClientOptions &client_options,
           const RequestOptions &request_options, std::uint64_t generation,
           const std::atomic_uint64_t &cancel_generation) = 0;
  virtual bool open() const noexcept = 0;
  virtual void close() noexcept = 0;
  [[nodiscard]] const std::string &key() const noexcept { return key_; }

private:
  std::string key_;
};

template <class Stream>
class PooledConnection final : public PooledConnectionBase {
public:
  PooledConnection(std::string key, Stream stream)
      : PooledConnectionBase(std::move(key)), stream_(std::move(stream)) {}

  asio::awaitable<ResponseResult>
  transact(http::request<http::string_body> request,
           const ClientOptions &client_options,
           const RequestOptions &request_options, std::uint64_t generation,
           const std::atomic_uint64_t &cancel_generation) override {
    co_return co_await exchange(stream_, std::move(request), client_options,
                                request_options, generation,
                                cancel_generation);
  }

  bool open() const noexcept override {
    return beast::get_lowest_layer(stream_).socket().is_open();
  }

  void close() noexcept override {
    boost::system::error_code ignored;
    beast::get_lowest_layer(stream_).cancel();
    beast::get_lowest_layer(stream_).socket().shutdown(
        tcp::socket::shutdown_both, ignored);
    beast::get_lowest_layer(stream_).socket().close(ignored);
  }

private:
  Stream stream_;
};

} // namespace

class AsyncClient::Impl {
public:
  Impl(asio::any_io_executor value_executor, std::string value_base_url,
       ClientOptions value_options)
      : executor(std::move(value_executor)), base_url(std::move(value_base_url)),
        options(std::move(value_options))
#ifdef CHHTTP_HAS_TLS
        , ssl_context(asio::ssl::context::tls_client)
#endif
  {
    auto parsed = detail::parse_url(base_url);
    if (parsed) {
      base = std::move(*parsed);
      base_url = origin(base);
    } else {
      configuration_error = parsed.error();
    }
#ifdef CHHTTP_HAS_TLS
    if (!configuration_error) {
      auto configured = configure_tls_context(ssl_context, options.tls);
      if (!configured) configuration_error = configured.error();
    }
#endif
  }

  std::shared_ptr<PooledConnectionBase>
  take_idle_connection(const std::string &key) {
    std::lock_guard lock(pool_mutex);
    std::erase_if(idle_connections, [](const auto &connection) {
      return !connection->open();
    });
    const auto found = std::ranges::find_if(
        idle_connections,
        [&](const auto &connection) { return connection->key() == key; });
    if (found == idle_connections.end()) return {};
    auto connection = std::move(*found);
    idle_connections.erase(found);
    return connection;
  }

  void release_connection(std::shared_ptr<PooledConnectionBase> connection) {
    if (options.connection_pool_size == 0 || !connection->open()) {
      connection->close();
      return;
    }
    std::lock_guard lock(pool_mutex);
    const auto same_origin = static_cast<std::size_t>(std::ranges::count_if(
        idle_connections, [&](const auto &candidate) {
          return candidate->key() == connection->key() && candidate->open();
        }));
    if (same_origin >= options.connection_pool_size) {
      connection->close();
      return;
    }
    idle_connections.push_back(std::move(connection));
  }

  void mark_active(const std::shared_ptr<PooledConnectionBase> &connection) {
    std::lock_guard lock(pool_mutex);
    active_connections.emplace_back(connection);
  }

  void unmark_active(const PooledConnectionBase *connection) {
    std::lock_guard lock(pool_mutex);
    std::erase_if(active_connections, [&](const auto &candidate) {
      const auto locked = candidate.lock();
      return !locked || locked.get() == connection;
    });
  }

  asio::awaitable<ResponseResult>
  use_connection(std::shared_ptr<PooledConnectionBase> connection,
                 http::request<http::string_body> outgoing,
                 const RequestOptions &request_options,
                 std::uint64_t generation) {
    mark_active(connection);
    ResponseResult result;
    std::exception_ptr exception;
    try {
      result = co_await connection->transact(
          std::move(outgoing), options, request_options, generation,
          cancel_generation);
    } catch (...) {
      exception = std::current_exception();
    }
    unmark_active(connection.get());
    if (exception) {
      connection->close();
      try {
        std::rethrow_exception(exception);
      } catch (const std::exception &error) {
        co_return ErrorInfo{Error::internal,
                            "Request callback failed: " +
                                std::string(error.what())};
      } catch (...) {
        co_return ErrorInfo{Error::internal, "Request callback failed"};
      }
    }
    if (result && result->keep_alive && options.keep_alive &&
        generation == cancel_generation.load()) {
      release_connection(std::move(connection));
    } else {
      connection->close();
    }
    co_return result;
  }

  asio::awaitable<Result<std::shared_ptr<PooledConnectionBase>>>
  create_connection(const detail::ParsedUrl &url,
                    const detail::ParsedUrl &connect_url, bool use_proxy,
                    const std::string &key) {
    tcp::resolver resolver(executor);
    boost::system::error_code ec;
    const auto endpoints = co_await resolver.async_resolve(
        connect_url.host, std::to_string(connect_url.port),
        asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      co_return detail::make_error(map_network_error(ec, Error::resolve),
                                   "Host resolution failed", ec);
    }
    beast::tcp_stream tcp_stream(executor);
    set_timeout(tcp_stream, options.connect_timeout);
    co_await tcp_stream.async_connect(
        endpoints, asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      co_return detail::make_error(map_network_error(ec, Error::connect),
                                   "Connection failed", ec);
    }
    if (options.tcp_no_delay)
      tcp_stream.socket().set_option(tcp::no_delay(true), ec);

    if (url.secure) {
#ifdef CHHTTP_HAS_TLS
      if (use_proxy) {
        http::request<http::empty_body> tunnel{http::verb::connect,
                                               host_header(url), 11};
        tunnel.set(http::field::host, host_header(url));
        if (!options.proxy.username.empty())
          tunnel.set(http::field::proxy_authorization,
                     basic_auth(options.proxy.username,
                                options.proxy.password));
        co_await http::async_write(
            tcp_stream, tunnel, asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
          co_return detail::make_error(Error::proxy,
                                       "Proxy CONNECT write failed", ec);
        }
        beast::flat_buffer proxy_buffer;
        http::response<http::empty_body> tunnel_response;
        co_await http::async_read(
            tcp_stream, proxy_buffer, tunnel_response,
            asio::redirect_error(asio::use_awaitable, ec));
        if (ec || tunnel_response.result() != http::status::ok) {
          co_return ErrorInfo{Error::proxy, "Proxy rejected CONNECT",
                              ec.value()};
        }
      }
      beast::ssl_stream<beast::tcp_stream> tls_stream(std::move(tcp_stream),
                                                      ssl_context);
      if (auto tls_error = co_await tls_handshake(tls_stream, url, options))
        co_return tls_error;
      co_return std::static_pointer_cast<PooledConnectionBase>(
          std::make_shared<PooledConnection<
              beast::ssl_stream<beast::tcp_stream>>>(key,
                                                     std::move(tls_stream)));
#else
      co_return ErrorInfo{Error::tls_unavailable,
                          "chhttp was built without TLS support"};
#endif
    }
    co_return std::static_pointer_cast<PooledConnectionBase>(
        std::make_shared<PooledConnection<beast::tcp_stream>>(
            key, std::move(tcp_stream)));
  }

  void cancel() {
    ++cancel_generation;
    std::vector<std::shared_ptr<PooledConnectionBase>> connections;
    {
      std::lock_guard lock(pool_mutex);
      connections = std::move(idle_connections);
      for (auto &candidate : active_connections) {
        if (auto connection = candidate.lock())
          connections.push_back(std::move(connection));
      }
      active_connections.clear();
    }
    asio::dispatch(executor, [connections = std::move(connections)] {
      for (const auto &connection : connections) connection->close();
    });
  }

  asio::awaitable<ResponseResult>
  request_once(Request request, const RequestOptions &request_options,
               detail::ParsedUrl url, std::uint64_t generation) {
    if (configuration_error) co_return configuration_error;
    if (url.secure) {
#ifndef CHHTTP_HAS_TLS
      co_return ErrorInfo{Error::tls_unavailable,
                          "chhttp was built without TLS support"};
#endif
    }
    http::request<http::string_body> outgoing;
    outgoing.version(request.version);
    outgoing.method_string(request.method);
    outgoing.target(url.target);
    outgoing.body() = std::move(request.body);
    detail::to_beast_headers(options.default_headers, outgoing.base());
    for (const auto &[name, value] : request.headers) {
      outgoing.set(name, value);
    }
    outgoing.set(http::field::host, host_header(url));
    if (!outgoing.count(http::field::user_agent))
      outgoing.set(http::field::user_agent, "chhttp/0.1");
    if (options.auto_decompress && !outgoing.count(http::field::accept_encoding))
      outgoing.set(http::field::accept_encoding, "br, zstd, gzip, deflate");
    outgoing.keep_alive(options.keep_alive);
    outgoing.prepare_payload();

    detail::ParsedUrl connect_url = url;
    bool use_proxy = false;
    if (!options.proxy.url.empty()) {
      auto proxy = detail::parse_url(options.proxy.url);
      if (!proxy) co_return ErrorInfo{Error::proxy, proxy.error().message};
      connect_url = std::move(*proxy);
      use_proxy = true;
      if (!url.secure && !options.proxy.username.empty()) {
        outgoing.set(http::field::proxy_authorization,
                     basic_auth(options.proxy.username, options.proxy.password));
      }
      if (!url.secure) outgoing.target(origin(url) + url.target);
    }
    const std::string key = origin(url) + "|proxy=" + options.proxy.url;
    const bool retry_safe = detail::iequals(request.method, "GET") ||
                            detail::iequals(request.method, "HEAD") ||
                            detail::iequals(request.method, "OPTIONS");
    for (int attempt = 0; attempt != 2; ++attempt) {
      auto connection = take_idle_connection(key);
      const bool reused = static_cast<bool>(connection);
      if (!connection) {
        auto created =
            co_await create_connection(url, connect_url, use_proxy, key);
        if (!created) co_return created.error();
        connection = std::move(*created);
      }
      auto result = co_await use_connection(connection, outgoing,
                                            request_options, generation);
      if (!result && reused && retry_safe && attempt == 0 &&
          (result.error().code == Error::read ||
           result.error().code == Error::write ||
           result.error().code == Error::connect)) {
        continue;
      }
      co_return result;
    }
    co_return ErrorInfo{Error::internal, "Connection retry failed"};
  }

  asio::awaitable<ResponseResult> request(Request request,
                                           RequestOptions request_options) {
    auto generation = cancel_generation.load();
    Request current = std::move(request);
    if (!current.headers.contains("Authorization")) {
      if (options.authentication.type == AuthenticationType::basic) {
        current.headers.set(
            "Authorization",
            basic_auth(options.authentication.username,
                       options.authentication.password));
      } else if (options.authentication.type == AuthenticationType::bearer) {
        current.headers.set("Authorization",
                            bearer_auth(options.authentication.token));
      }
    }
    detail::ParsedUrl current_url;
    std::size_t redirects = 0;
    std::uint32_t digest_nonce_count = 0;
    bool digest_attempted = false;
    for (;;) {
      auto parsed = detail::parse_url(current.target, base_url);
      if (!parsed) co_return parsed.error();
      current_url = std::move(*parsed);
      current.target = current_url.target;
      auto result = co_await request_once(current, request_options, current_url,
                                          generation);
      if (!result) co_return result;
      if (options.auto_decompress && !request_options.on_data &&
          result->headers.contains("Content-Encoding")) {
        auto decoded = detail::decompress(
            result->body, result->headers.get("Content-Encoding"),
            options.max_response_body_size);
        if (!decoded) co_return decoded.error();
        result->body = std::move(*decoded);
        result->headers.erase("Content-Encoding");
        result->headers.erase("Content-Length");
      }
      if (result->status == 401 && !digest_attempted &&
          options.authentication.type == AuthenticationType::digest) {
        std::optional<DigestChallenge> challenge;
        for (const auto &header :
             result->headers.get_all("WWW-Authenticate")) {
          auto parsed_challenge = parse_digest_challenge(header);
          if (parsed_challenge) {
            challenge = std::move(*parsed_challenge);
            break;
          }
        }
        if (challenge) {
          auto authorization = digest_auth(
              current.method, current_url.target,
              options.authentication.username,
              options.authentication.password, *challenge,
              ++digest_nonce_count);
          if (!authorization) co_return authorization.error();
          current.headers.set("Authorization", std::move(*authorization));
          digest_attempted = true;
          continue;
        }
      }
      const bool redirect =
          result->status == 301 || result->status == 302 ||
          result->status == 303 || result->status == 307 ||
          result->status == 308;
      if (!options.follow_redirects || !redirect ||
          !result->headers.contains("Location"))
        co_return result;
      if (redirects >= options.max_redirects)
        co_return ErrorInfo{Error::redirect_limit,
                            "Maximum redirect count exceeded"};
      ++redirects;
      const auto location = result->headers.get("Location");
      auto next = detail::parse_url(location, origin(current_url));
      if (!next) co_return next.error();
      if (next->host != current_url.host || next->port != current_url.port ||
          next->scheme != current_url.scheme) {
        current.headers.erase("Authorization");
      }
      if (options.authentication.type == AuthenticationType::digest) {
        current.headers.erase("Authorization");
        digest_attempted = false;
      }
      if (result->status == 303 ||
          ((result->status == 301 || result->status == 302) &&
           detail::iequals(current.method, "POST"))) {
        current.method = "GET";
        current.body.clear();
        current.headers.erase("Content-Type");
        current.headers.erase("Content-Length");
      }
      current.target = origin(*next) + next->target;
    }
  }

  asio::any_io_executor executor;
  std::string base_url;
  detail::ParsedUrl base;
  ClientOptions options;
  ErrorInfo configuration_error;
  std::atomic_uint64_t cancel_generation{0};
  std::mutex pool_mutex;
  std::vector<std::shared_ptr<PooledConnectionBase>> idle_connections;
  std::vector<std::weak_ptr<PooledConnectionBase>> active_connections;
#ifdef CHHTTP_HAS_TLS
  asio::ssl::context ssl_context;
#endif
};

AsyncClient::AsyncClient(asio::any_io_executor executor, std::string base_url,
                         ClientOptions options)
    : impl_(std::make_unique<Impl>(std::move(executor), std::move(base_url),
                                   std::move(options))) {}
AsyncClient::~AsyncClient() = default;
AsyncClient::AsyncClient(AsyncClient &&) noexcept = default;
AsyncClient &AsyncClient::operator=(AsyncClient &&) noexcept = default;

asio::awaitable<ResponseResult>
AsyncClient::request(Request request, RequestOptions options) {
  co_return co_await impl_->request(std::move(request), std::move(options));
}

asio::awaitable<ResponseResult> AsyncClient::get(std::string target,
                                                Headers headers,
                                                RequestOptions options) {
  Request request{.method = "GET", .target = std::move(target),
                  .headers = std::move(headers)};
  co_return co_await impl_->request(std::move(request), std::move(options));
}

asio::awaitable<ResponseResult> AsyncClient::head(std::string target,
                                                 Headers headers) {
  Request request{.method = "HEAD", .target = std::move(target),
                  .headers = std::move(headers)};
  co_return co_await impl_->request(std::move(request), {});
}

static Request make_body_request(std::string method, std::string target,
                                 std::string body, std::string content_type,
                                 Headers headers) {
  headers.set("Content-Type", std::move(content_type));
  Request request;
  request.method = std::move(method);
  request.target = std::move(target);
  request.headers = std::move(headers);
  request.body = std::move(body);
  return request;
}

asio::awaitable<ResponseResult>
AsyncClient::post(std::string target, std::string body,
                  std::string content_type, Headers headers,
                  RequestOptions options) {
  co_return co_await impl_->request(
      make_body_request("POST", std::move(target), std::move(body),
                        std::move(content_type), std::move(headers)),
      std::move(options));
}

asio::awaitable<ResponseResult>
AsyncClient::put(std::string target, std::string body,
                 std::string content_type, Headers headers,
                 RequestOptions options) {
  co_return co_await impl_->request(
      make_body_request("PUT", std::move(target), std::move(body),
                        std::move(content_type), std::move(headers)),
      std::move(options));
}

asio::awaitable<ResponseResult>
AsyncClient::patch(std::string target, std::string body,
                   std::string content_type, Headers headers,
                   RequestOptions options) {
  co_return co_await impl_->request(
      make_body_request("PATCH", std::move(target), std::move(body),
                        std::move(content_type), std::move(headers)),
      std::move(options));
}

asio::awaitable<ResponseResult> AsyncClient::del(std::string target,
                                                Headers headers,
                                                RequestOptions options) {
  Request request{.method = "DELETE", .target = std::move(target),
                  .headers = std::move(headers)};
  co_return co_await impl_->request(std::move(request), std::move(options));
}

void AsyncClient::cancel() { impl_->cancel(); }

class Client::Impl {
public:
  Impl(std::string base_url, ClientOptions options)
      : work(asio::make_work_guard(io)),
        client(io.get_executor(), std::move(base_url), std::move(options)),
        thread([this] { io.run(); }) {}

  ~Impl() {
    client.cancel();
    work.reset();
    io.stop();
    if (thread.joinable()) thread.join();
  }

  template <class Operation> ResponseResult run(Operation operation) {
    auto promise = std::make_shared<std::promise<ResponseResult>>();
    auto future = promise->get_future();
    asio::co_spawn(
        io, operation(),
        [promise](std::exception_ptr exception, ResponseResult result) mutable {
          if (exception) {
            try {
              std::rethrow_exception(exception);
            } catch (const std::exception &error) {
              promise->set_value(
                  ErrorInfo{Error::internal, error.what()});
            }
          } else {
            promise->set_value(std::move(result));
          }
        });
    return future.get();
  }

  asio::io_context io;
  asio::executor_work_guard<asio::io_context::executor_type> work;
  AsyncClient client;
  std::thread thread;
};

Client::Client(std::string base_url, ClientOptions options)
    : impl_(std::make_unique<Impl>(std::move(base_url), std::move(options))) {}
Client::~Client() = default;
Client::Client(Client &&) noexcept = default;
Client &Client::operator=(Client &&) noexcept = default;

ResponseResult Client::request(Request request, RequestOptions options) {
  return impl_->run([this, request = std::move(request),
                     options = std::move(options)]() mutable {
    return impl_->client.request(std::move(request), std::move(options));
  });
}
ResponseResult Client::get(std::string target, Headers headers,
                           RequestOptions options) {
  return impl_->run([this, target = std::move(target), headers = std::move(headers),
                     options = std::move(options)]() mutable {
    return impl_->client.get(std::move(target), std::move(headers),
                             std::move(options));
  });
}
ResponseResult Client::head(std::string target, Headers headers) {
  return impl_->run([this, target = std::move(target),
                     headers = std::move(headers)]() mutable {
    return impl_->client.head(std::move(target), std::move(headers));
  });
}
ResponseResult Client::post(std::string target, std::string body,
                            std::string content_type, Headers headers,
                            RequestOptions options) {
  return impl_->run([this, target = std::move(target), body = std::move(body),
                     content_type = std::move(content_type),
                     headers = std::move(headers),
                     options = std::move(options)]() mutable {
    return impl_->client.post(std::move(target), std::move(body),
                              std::move(content_type), std::move(headers),
                              std::move(options));
  });
}
ResponseResult Client::put(std::string target, std::string body,
                           std::string content_type, Headers headers,
                           RequestOptions options) {
  return impl_->run([this, target = std::move(target), body = std::move(body),
                     content_type = std::move(content_type),
                     headers = std::move(headers),
                     options = std::move(options)]() mutable {
    return impl_->client.put(std::move(target), std::move(body),
                             std::move(content_type), std::move(headers),
                             std::move(options));
  });
}
ResponseResult Client::patch(std::string target, std::string body,
                             std::string content_type, Headers headers,
                             RequestOptions options) {
  return impl_->run([this, target = std::move(target), body = std::move(body),
                     content_type = std::move(content_type),
                     headers = std::move(headers),
                     options = std::move(options)]() mutable {
    return impl_->client.patch(std::move(target), std::move(body),
                               std::move(content_type), std::move(headers),
                               std::move(options));
  });
}
ResponseResult Client::del(std::string target, Headers headers,
                           RequestOptions options) {
  return impl_->run([this, target = std::move(target), headers = std::move(headers),
                     options = std::move(options)]() mutable {
    return impl_->client.del(std::move(target), std::move(headers),
                             std::move(options));
  });
}
void Client::cancel() { impl_->client.cancel(); }

asio::awaitable<Result<std::shared_ptr<WebSocket>>>
AsyncWebSocketClient::connect(asio::any_io_executor executor, std::string url,
                              Headers headers, ClientOptions options) {
  auto parsed = detail::parse_url(url);
  if (!parsed) co_return parsed.error();
  if (parsed->scheme != "ws" && parsed->scheme != "wss")
    co_return ErrorInfo{Error::invalid_url,
                        "WebSocket URL must use ws:// or wss://"};
  tcp::resolver resolver(executor);
  boost::system::error_code ec;
  const auto endpoints = co_await resolver.async_resolve(
      parsed->host, std::to_string(parsed->port),
      asio::redirect_error(asio::use_awaitable, ec));
  if (ec) co_return detail::make_error(Error::resolve, "Host resolution failed", ec);
  beast::tcp_stream tcp_stream(executor);
  set_timeout(tcp_stream, options.connect_timeout);
  co_await tcp_stream.async_connect(
      endpoints, asio::redirect_error(asio::use_awaitable, ec));
  if (ec) co_return detail::make_error(Error::connect, "Connection failed", ec);
  if (parsed->secure) {
#ifdef CHHTTP_HAS_TLS
    asio::ssl::context ssl_context(asio::ssl::context::tls_client);
    auto configured = configure_tls_context(ssl_context, options.tls);
    if (!configured) co_return configured.error();
    beast::ssl_stream<beast::tcp_stream> tls_stream(std::move(tcp_stream),
                                                    ssl_context);
    if (auto tls_error = co_await tls_handshake(tls_stream, *parsed, options))
      co_return tls_error;
    auto channel = std::make_shared<
        ClientWebSocketChannel<beast::ssl_stream<beast::tcp_stream>>>(
        std::move(tls_stream));
    if (auto error = co_await channel->handshake(*parsed, headers)) co_return error;
    co_return std::make_shared<WebSocket>(channel);
#else
    co_return ErrorInfo{Error::tls_unavailable,
                        "chhttp was built without TLS support"};
#endif
  }
  auto channel =
      std::make_shared<ClientWebSocketChannel<beast::tcp_stream>>(
          std::move(tcp_stream));
  if (auto error = co_await channel->handshake(*parsed, headers)) co_return error;
  co_return std::make_shared<WebSocket>(channel);
}

} // namespace chhttp
