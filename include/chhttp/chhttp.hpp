#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <regex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace chhttp {

namespace asio = boost::asio;
using namespace std::chrono_literals;

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

struct Request {
  std::string method{"GET"};
  std::string target{"/"};
  std::string path{"/"};
  unsigned version{11};
  Headers headers;
  Params query;
  PathParams path_params;
  std::string body;
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
    virtual asio::awaitable<bool> write(std::string data) = 0;
    virtual asio::awaitable<bool> flush() = 0;
    virtual bool open() const noexcept = 0;
  };

  StreamWriter() = default;
  explicit StreamWriter(std::shared_ptr<Sink> sink) : sink_(std::move(sink)) {}
  asio::awaitable<bool> write(std::string_view data);
  asio::awaitable<bool> write(std::span<const std::byte> data);
  asio::awaitable<bool> flush();
  [[nodiscard]] bool open() const noexcept;

private:
  std::shared_ptr<Sink> sink_;
};

class SseWriter {
public:
  explicit SseWriter(StreamWriter writer) : writer_(std::move(writer)) {}
  asio::awaitable<bool> send(const SseEvent &event);
  asio::awaitable<bool> data(std::string_view value);
  asio::awaitable<bool> comment(std::string_view value = "keep-alive");
  [[nodiscard]] bool open() const noexcept { return writer_.open(); }

private:
  StreamWriter writer_;
};

using StreamHandler = std::function<asio::awaitable<void>(StreamWriter &)>;
using SseHandler = std::function<asio::awaitable<void>(SseWriter &)>;

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
using Handler = std::function<void(const Request &, Response &)>;
using AsyncHandler =
    std::function<asio::awaitable<void>(const Request &, Response &)>;
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
    virtual asio::awaitable<Result<Message>> read() = 0;
    virtual asio::awaitable<bool> send(std::string data, bool binary) = 0;
    virtual asio::awaitable<void> ping(std::string data) = 0;
    virtual asio::awaitable<void> close(std::uint16_t code,
                                        std::string reason) = 0;
    virtual bool open() const noexcept = 0;
    virtual std::string subprotocol() const = 0;
  };

  explicit WebSocket(std::shared_ptr<Channel> channel)
      : channel_(std::move(channel)) {}
  asio::awaitable<Result<Message>> read();
  asio::awaitable<bool> send_text(std::string_view data);
  asio::awaitable<bool> send_binary(std::span<const std::byte> data);
  asio::awaitable<void> ping(std::string_view data = {});
  asio::awaitable<void> close(std::uint16_t code = 1000,
                              std::string_view reason = {});
  [[nodiscard]] bool open() const noexcept;
  [[nodiscard]] std::string subprotocol() const;

private:
  std::shared_ptr<Channel> channel_;
};

using WebSocketHandler =
    std::function<asio::awaitable<void>(const Request &, WebSocket &)>;
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
  Server &get(std::string pattern, Handler handler);
  Server &post(std::string pattern, Handler handler);
  Server &put(std::string pattern, Handler handler);
  Server &patch(std::string pattern, Handler handler);
  Server &del(std::string pattern, Handler handler);
  Server &options(std::string pattern, Handler handler);
  Server &head(std::string pattern, Handler handler);
  Server &get_async(std::string pattern, AsyncHandler handler);
  Server &post_async(std::string pattern, AsyncHandler handler);
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
  [[nodiscard]] asio::any_io_executor executor() const;

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
  std::size_t connection_pool_size{8};
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
  std::function<bool(std::string_view)> on_data;
  std::function<bool(std::uint64_t current, std::uint64_t total)> on_progress;
  std::shared_ptr<std::atomic_bool> cancellation;
};

class AsyncClient {
public:
  AsyncClient(asio::any_io_executor executor, std::string base_url,
              ClientOptions options = {});
  ~AsyncClient();
  AsyncClient(AsyncClient &&) noexcept;
  AsyncClient &operator=(AsyncClient &&) noexcept;
  AsyncClient(const AsyncClient &) = delete;
  AsyncClient &operator=(const AsyncClient &) = delete;

  asio::awaitable<ResponseResult> request(Request request,
                                           RequestOptions options = {});
  asio::awaitable<ResponseResult> get(std::string target,
                                      Headers headers = {},
                                      RequestOptions options = {});
  asio::awaitable<ResponseResult> head(std::string target,
                                       Headers headers = {});
  asio::awaitable<ResponseResult> post(std::string target, std::string body,
                                       std::string content_type,
                                       Headers headers = {},
                                       RequestOptions options = {});
  asio::awaitable<ResponseResult> put(std::string target, std::string body,
                                      std::string content_type,
                                      Headers headers = {},
                                      RequestOptions options = {});
  asio::awaitable<ResponseResult> patch(std::string target, std::string body,
                                        std::string content_type,
                                        Headers headers = {},
                                        RequestOptions options = {});
  asio::awaitable<ResponseResult> del(std::string target,
                                      Headers headers = {},
                                      RequestOptions options = {});
  void cancel();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
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
  ResponseResult head(std::string target, Headers headers = {});
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

class SseClient {
public:
  using MessageHandler = std::function<void(const SseEvent &)>;
  using ErrorCallback = std::function<void(const ErrorInfo &)>;

  SseClient(AsyncClient &client, std::string target,
            Headers headers = {}, SseClientOptions options = {});
  ~SseClient();
  SseClient(SseClient &&) noexcept;
  SseClient &operator=(SseClient &&) noexcept;
  SseClient(const SseClient &) = delete;
  SseClient &operator=(const SseClient &) = delete;

  SseClient &on_message(MessageHandler handler);
  SseClient &on_event(std::string event, MessageHandler handler);
  SseClient &on_error(ErrorCallback handler);
  asio::awaitable<ErrorInfo> connect();
  void stop();
  [[nodiscard]] bool running() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class AsyncWebSocketClient {
public:
  static asio::awaitable<Result<std::shared_ptr<WebSocket>>>
  connect(asio::any_io_executor executor, std::string url,
          Headers headers = {}, ClientOptions options = {});
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
