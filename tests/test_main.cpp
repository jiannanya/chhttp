#include <chhttp/chhttp.hpp>

#ifdef _WIN32
#include <process.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#ifdef CHHTTP_HAS_TLS
#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

std::filesystem::path unique_test_directory(std::string_view prefix) {
  static std::atomic_uint64_t sequence{0};
#ifdef _WIN32
  const auto process_id = static_cast<unsigned long long>(_getpid());
#else
  const auto process_id = static_cast<unsigned long long>(getpid());
#endif
  const auto timestamp = static_cast<unsigned long long>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  for (unsigned attempt = 0; attempt < 100; ++attempt) {
    auto path = std::filesystem::temp_directory_path() /
                (std::string(prefix) + "-" + std::to_string(process_id) + "-" +
                 std::to_string(timestamp) + "-" +
                 std::to_string(sequence.fetch_add(1)));
    std::error_code error;
    if (std::filesystem::create_directory(path, error)) return path;
  }
  throw std::runtime_error("Unable to allocate a unique test directory");
}

#ifdef _WIN32
using raw_socket_t = SOCKET;
constexpr raw_socket_t invalid_raw_socket = INVALID_SOCKET;
void close_raw_socket(raw_socket_t socket) { closesocket(socket); }
#else
using raw_socket_t = int;
constexpr raw_socket_t invalid_raw_socket = -1;
void close_raw_socket(raw_socket_t socket) { close(socket); }
#endif

std::string masked_websocket_frame(unsigned char first_byte,
                                   std::string payload) {
  if (payload.size() > 125)
    throw std::runtime_error("Raw WebSocket test payload is too large");
  constexpr std::array<unsigned char, 4> mask{0x13, 0x37, 0x42, 0x99};
  std::string frame;
  frame.push_back(static_cast<char>(first_byte));
  frame.push_back(static_cast<char>(0x80 | payload.size()));
  frame.append(reinterpret_cast<const char *>(mask.data()), mask.size());
  for (std::size_t index = 0; index < payload.size(); ++index)
    frame.push_back(payload[index] ^ static_cast<char>(mask[index % mask.size()]));
  return frame;
}

std::string raw_http_exchange(std::uint16_t port, std::string_view request,
                              std::size_t send_chunk_size = 0) {
#ifdef _WIN32
  static const bool winsock_ready = [] {
    WSADATA data{};
    return WSAStartup(MAKEWORD(2, 2), &data) == 0;
  }();
  if (!winsock_ready) throw std::runtime_error("WSAStartup failed");
#endif
  raw_socket_t socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket == invalid_raw_socket)
    throw std::runtime_error("Unable to allocate raw test socket");
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
  if (::connect(socket, reinterpret_cast<const sockaddr *>(&address),
                sizeof(address)) != 0) {
    close_raw_socket(socket);
    throw std::runtime_error("Unable to connect raw test socket");
  }
  std::size_t offset = 0;
  while (offset < request.size()) {
    const auto count = send_chunk_size == 0
                           ? request.size() - offset
                           : (std::min)(send_chunk_size, request.size() - offset);
    const int sent = ::send(socket, request.data() + offset,
                            static_cast<int>(count), 0);
    if (sent <= 0) {
      close_raw_socket(socket);
      throw std::runtime_error("Raw test socket send failed");
    }
    offset += static_cast<std::size_t>(sent);
  }
#ifdef _WIN32
  shutdown(socket, SD_SEND);
#else
  shutdown(socket, SHUT_WR);
#endif
  std::string response;
  std::array<char, 4096> buffer{};
  for (;;) {
    const int received =
        ::recv(socket, buffer.data(), static_cast<int>(buffer.size()), 0);
    if (received <= 0) break;
    response.append(buffer.data(), static_cast<std::size_t>(received));
  }
  close_raw_socket(socket);
  return response;
}

class RawResponseServer {
public:
  explicit RawResponseServer(std::string response, bool bytewise = false) {
    listener_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener_ == invalid_raw_socket)
      throw std::runtime_error("Unable to allocate raw response socket");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener_, reinterpret_cast<const sockaddr *>(&address),
               sizeof(address)) != 0 ||
        ::listen(listener_, 1) != 0) {
      close_raw_socket(listener_);
      throw std::runtime_error("Unable to bind raw response socket");
    }
#ifdef _WIN32
    int size = sizeof(address);
#else
    socklen_t size = sizeof(address);
#endif
    getsockname(listener_, reinterpret_cast<sockaddr *>(&address), &size);
    port_ = ntohs(address.sin_port);
    thread_ = std::thread([this, response = std::move(response), bytewise] {
      raw_socket_t client = ::accept(listener_, nullptr, nullptr);
      if (client == invalid_raw_socket) return;
      std::array<char, 2048> request{};
      ::recv(client, request.data(), static_cast<int>(request.size()), 0);
      std::size_t offset = 0;
      while (offset < response.size()) {
        const auto count = bytewise ? std::size_t{1} : response.size() - offset;
        const int sent = ::send(client, response.data() + offset,
                                static_cast<int>(count), 0);
        if (sent <= 0) break;
        offset += static_cast<std::size_t>(sent);
      }
#ifdef _WIN32
      shutdown(client, SD_BOTH);
#else
      shutdown(client, SHUT_RDWR);
#endif
      close_raw_socket(client);
    });
  }
  ~RawResponseServer() {
    close_raw_socket(listener_);
    if (thread_.joinable()) thread_.join();
  }
  std::uint16_t port() const noexcept { return port_; }

private:
  raw_socket_t listener_{invalid_raw_socket};
  std::uint16_t port_{0};
  std::thread thread_;
};

chhttp::ResponseResult fetch_raw_response(std::string response,
                                          bool bytewise = false,
                                          chhttp::ClientOptions options = {}) {
  RawResponseServer server(std::move(response), bytewise);
  chhttp::Client client("http://127.0.0.1:" +
                            std::to_string(server.port()),
                        std::move(options));
  return client.get("/");
}

struct Failure : std::runtime_error {
  using std::runtime_error::runtime_error;
};

#define CHECK(expression)                                                       \
  do {                                                                          \
    if (!(expression))                                                          \
      throw Failure(std::string("CHECK failed: ") + #expression + " at " +     \
                    __FILE__ + ":" + std::to_string(__LINE__));                 \
  } while (false)

struct Test {
  const char *name;
  void (*function)();
};

std::vector<Test> &tests() {
  static std::vector<Test> value;
  return value;
}

struct Registration {
  Registration(const char *name, void (*function)()) {
    tests().push_back({name, function});
  }
};

#define TEST(name)                                                              \
  void name();                                                                  \
  Registration name##_registration(#name, name);                               \
  void name()

class Fixture {
public:
  Fixture() {
    root = unique_test_directory("chhttp-tests-static");
    std::ofstream(root / "asset.txt", std::ios::binary) << "0123456789";
    server.set_pre_routing_handler(
        [](const chhttp::Request &request, chhttp::Response &response) {
          if (request.path == "/blocked") {
            response.status = 403;
            response.set_content("blocked");
            return true;
          }
          return false;
        });
    server.set_post_routing_handler(
        [](const chhttp::Request &, chhttp::Response &response) {
          response.headers.set("X-Post-Route", "yes");
          return false;
        });
    server.set_error_handler([](const chhttp::Request &,
                                chhttp::Response &response) {
      response.set_content("custom-not-found");
    });
    server.get("/hello", [](const chhttp::Request &request,
                             chhttp::Response &response) {
      response.set_content("hello:" + request.get_param("who", "world"));
      response.headers.add("Set-Cookie", "a=1");
      response.headers.add("Set-Cookie", "b=2");
    });
    server.get("/users/{id}", [](const chhttp::Request &request,
                                  chhttp::Response &response) {
      response.set_content(request.path_params.at("id"));
    });
    for (const auto *method : {"POST", "PUT", "PATCH", "DELETE"}) {
      server.route(method, "/echo",
                   [](const chhttp::Request &request,
                      chhttp::Response &response) {
                     response.set_content(request.method + ":" + request.body,
                                          request.get_header("Content-Type"));
                   });
    }
    server.post("/multipart", [](const chhttp::Request &request,
                                  chhttp::Response &response) {
      std::string result;
      for (const auto &part : request.files)
        result += part.name + "=" + part.content + ";";
      response.set_content(result);
    });
    server.get("/redirect", [](const chhttp::Request &,
                                chhttp::Response &response) {
      response.set_redirect("/hello?who=redirected");
    });
    server.get("/redirect-loop", [](const chhttp::Request &,
                                     chhttp::Response &response) {
      response.set_redirect("/redirect-loop");
    });
    server.get("/large", [](const chhttp::Request &,
                             chhttp::Response &response) {
      response.set_content(std::string(128 * 1024, 'x'));
    });
    server.get("/connection", [](const chhttp::Request &request,
                                  chhttp::Response &response) {
      response.set_content(std::to_string(request.remote_port));
    });
    server.get("/authorization", [](const chhttp::Request &request,
                                    chhttp::Response &response) {
      response.set_content(request.get_header("Authorization"));
    });
    server.get("/digest", [](const chhttp::Request &request,
                              chhttp::Response &response) {
      const auto authorization = request.get_header("Authorization");
      if (authorization.empty()) {
        response.status = 401;
        response.headers.set(
            "WWW-Authenticate",
            "Digest realm=\"agents\", nonce=\"nonce42\", "
            "algorithm=SHA-256, qop=\"auth\", opaque=\"opaque42\"");
        response.set_content("authentication required");
      } else {
        response.set_content(authorization);
      }
    });
    server.get_async(
        "/slow", [](const chhttp::Request &,
                     chhttp::Response &response) -> chhttp::Task<void> {
          co_await chhttp::sleep_for(std::chrono::milliseconds(250));
          response.set_content("late");
        });
    server.get("/fail", [](const chhttp::Request &, chhttp::Response &) {
      throw std::runtime_error("boom");
    });
    server.get_async(
        "/async", [](const chhttp::Request &,
                      chhttp::Response &response) -> chhttp::Task<void> {
          co_await chhttp::sleep_for(std::chrono::milliseconds(2));
          response.set_content("async");
        });
    server.get_async(
        "/stream", [](const chhttp::Request &,
                       chhttp::Response &response) -> chhttp::Task<void> {
          response.set_stream(
              "text/plain", [](chhttp::StreamWriter &writer)
                                -> chhttp::Task<void> {
                CHECK(co_await writer.write("one"));
                CHECK(co_await writer.write("two"));
              });
          co_return;
        });
    server.get_async(
        "/sse", [](const chhttp::Request &,
                    chhttp::Response &response) -> chhttp::Task<void> {
          response.set_sse([](chhttp::SseWriter &writer)
                               -> chhttp::Task<void> {
            CHECK(co_await writer.comment());
            for (int index = 0; index != 3; ++index) {
              CHECK(co_await writer.send({.data = "line1\nline2",
                                          .event = "tick",
                                          .id = std::to_string(index),
                                          .retry = std::chrono::milliseconds(10)}));
            }
          });
          co_return;
        });
    server.websocket(
        "/ws",
        [](const chhttp::Request &, chhttp::WebSocket &socket)
            -> chhttp::Task<void> {
          auto message = co_await socket.read();
          if (message &&
              message->type == chhttp::WebSocket::MessageType::ping)
            message = co_await socket.read();
          if (message) co_await socket.send_text("echo:" + message->data);
        },
        [](const std::vector<std::string> &protocols) {
          return std::ranges::find(protocols, "agent.v1") != protocols.end()
                     ? "agent.v1"
                     : "";
        });
    server.mount("/static", root, {{"Cache-Control", "public, max-age=60"}});
    CHECK(server.start("127.0.0.1", 0));
    base_url = "http://127.0.0.1:" + std::to_string(server.port());
  }

  ~Fixture() {
    server.stop();
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
  }

  chhttp::Server server;
  std::filesystem::path root;
  std::string base_url;
};

Fixture &fixture() {
  static Fixture value;
  return value;
}

TEST(headers_preserve_duplicates_and_ignore_case) {
  chhttp::Headers headers{{"Content-Type", "text/plain"},
                          {"Set-Cookie", "a=1"},
                          {"set-cookie", "b=2"}};
  CHECK(headers.contains("content-type"));
  CHECK(headers.get("CONTENT-TYPE") == "text/plain");
  CHECK(headers.get_all("Set-Cookie").size() == 2);
  headers.set("content-type", "application/json");
  CHECK(headers.get("Content-Type") == "application/json");
  CHECK(headers.erase("SET-cookie"));
  CHECK(!headers.contains("set-cookie"));
  CHECK(!headers.erase("missing"));
}

TEST(outbound_request_rejects_invalid_syntax) {
  chhttp::Client client(fixture().base_url);
  chhttp::Request bad_method;
  bad_method.method = "GET\r\nInjected";
  bad_method.target = "/hello";
  auto method_result = client.request(std::move(bad_method));
  CHECK(!method_result &&
        method_result.error().code == chhttp::Error::protocol);

  chhttp::Request bad_header;
  bad_header.target = "/hello";
  bad_header.headers.add("X-Test", "safe\r\nInjected: yes");
  auto header_result = client.request(std::move(bad_header));
  CHECK(!header_result &&
        header_result.error().code == chhttp::Error::protocol);

  auto bad_target = client.get("/hello with-space");
  CHECK(!bad_target && bad_target.error().code == chhttp::Error::invalid_url);
}

TEST(url_and_query_round_trip) {
  const std::string input = "hello 世界 /?";
  const auto encoded = chhttp::url_encode(input);
  const auto decoded = chhttp::url_decode(encoded);
  CHECK(decoded && *decoded == input);
  chhttp::Params params{{"a", "1 2"}, {"utf8", "中文"}, {"a", "3"}};
  const auto parsed = chhttp::parse_query(chhttp::make_query(params));
  CHECK(parsed == params);
  CHECK(!chhttp::url_decode("%XX"));
  CHECK(!chhttp::url_decode("trailing%"));
  CHECK(chhttp::url_decode("a+b", true).value() == "a b");
  CHECK(chhttp::url_decode("a+b", false).value() == "a+b");
  CHECK(chhttp::parse_query("?empty=&flag&bad=%XX") ==
        chhttp::Params({{"empty", ""}, {"flag", ""}}));
}

TEST(invalid_url_authorities_and_ports_are_rejected) {
  for (const auto *url : {"ftp://127.0.0.1/", "http://:80/",
                          "http://user@127.0.0.1/", "http://127.0.0.1:/",
                          "http://127.0.0.1:0/", "http://127.0.0.1:65536/",
                          "http://[::1/", "http://::1/",
                          "http://127.0.0.1/white space"}) {
    chhttp::Client client(url);
    auto result = client.get("/");
    CHECK(!result && result.error().code == chhttp::Error::invalid_url);
  }
}

TEST(multipart_round_trip) {
  chhttp::MultipartForm input{
      {.name = "prompt", .content = "hello"},
      {.name = "fi\"le", .filename = "a;b\\c.txt",
       .content_type = "text/plain",
       .content = std::string("a\0b", 3)}};
  auto [content_type, body] = chhttp::make_multipart(input, "boundary42");
  auto parsed = chhttp::parse_multipart(body, content_type);
  CHECK(parsed && parsed->size() == 2);
  CHECK((*parsed)[1].name == "fi\"le");
  CHECK((*parsed)[1].filename == "a;b\\c.txt");
  CHECK((*parsed)[1].content == std::string("a\0b", 3));
  auto quoted_boundary = chhttp::parse_multipart(
      body, "multipart/form-data; charset=utf-8; boundary=\"boundary42\"");
  CHECK(quoted_boundary && quoted_boundary->size() == 2);
}

TEST(multipart_rejects_malformed_and_excessive_parts) {
  CHECK(!chhttp::parse_multipart("", "text/plain"));
  CHECK(!chhttp::parse_multipart("", "multipart/form-data"));
  CHECK(!chhttp::parse_multipart(
      "", "multipart/form-data; boundary=\"unterminated"));
  CHECK(!chhttp::parse_multipart(
      "--bad boundary --\r\n", "multipart/form-data; boundary=bad boundary "));
  CHECK(!chhttp::parse_multipart(
      "--b\r\nBroken-Header\r\n\r\ndata\r\n--b--\r\n",
      "multipart/form-data; boundary=b"));
  CHECK(!chhttp::parse_multipart(
      "--b\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\ndata",
      "multipart/form-data; boundary=b"));

  chhttp::MultipartForm parts{{.name = "a", .content = "1"},
                              {.name = "b", .content = "2"}};
  auto [content_type, body] = chhttp::make_multipart(parts, "limit-boundary");
  auto excessive = chhttp::parse_multipart(body, content_type, 1);
  CHECK(!excessive && excessive.error().code == chhttp::Error::multipart);
}

TEST(headers_preserve_empty_values_and_insertion_order) {
  chhttp::Headers headers;
  headers.add("X-First", "");
  headers.add("X-Second", "two");
  headers.add("x-first", "three");
  CHECK(headers.size() == 3);
  CHECK(headers.get("X-First").empty());
  CHECK(headers.get_all("x-first") ==
        std::vector<std::string>({"", "three"}));
  auto iterator = headers.begin();
  CHECK(iterator->first == "X-First");
  CHECK((++iterator)->first == "X-Second");
  CHECK(headers.get("missing", "fallback") == "fallback");
}

TEST(headers_set_replaces_every_duplicate_in_place) {
  chhttp::Headers headers{{"X-Value", "one"},
                          {"x-value", "two"},
                          {"X-Other", "kept"}};
  headers.set("X-VALUE", "replacement");
  CHECK(headers.size() == 2);
  CHECK(headers.get_all("x-value") ==
        std::vector<std::string>({"replacement"}));
  CHECK(headers.get("x-other") == "kept");
  headers.set("X-New", "new");
  CHECK(headers.size() == 3 && headers.get("x-new") == "new");
}

TEST(url_encoding_round_trips_every_byte_value) {
  std::string bytes;
  bytes.reserve(256);
  for (int value = 0; value != 256; ++value)
    bytes.push_back(static_cast<char>(value));
  const auto encoded = chhttp::url_encode(bytes);
  const auto decoded = chhttp::url_decode(encoded);
  CHECK(decoded && *decoded == bytes);
  CHECK(encoded.find('\0') == std::string::npos);
  CHECK(encoded.find('%') != std::string::npos);
}

TEST(query_parser_keeps_duplicate_empty_and_flag_fields) {
  const auto query = chhttp::parse_query("?a=1&a=&flag&=value&&plus=a+b");
  CHECK(query == chhttp::Params({{"a", "1"},
                                {"a", ""},
                                {"flag", ""},
                                {"", "value"},
                                {"plus", "a b"}}));
  CHECK(chhttp::make_query(query) ==
        "a=1&a=&flag=&=value&plus=a+b");
}

TEST(url_fragments_are_never_sent_to_the_server) {
  chhttp::Client client(fixture().base_url);
  auto response = client.get("/hello?who=fragment#client-only");
  CHECK(response && response->body == "hello:fragment");
}

TEST(relative_redirects_normalize_dot_segments_and_fragments) {
  chhttp::Server server;
  server.get("/a/b/start", [](const chhttp::Request &,
                               chhttp::Response &response) {
    response.set_redirect("../final?value=ok#ignored");
  });
  server.get("/a/final", [](const chhttp::Request &request,
                             chhttp::Response &response) {
    response.set_content(request.get_param("value") + "|" + request.target);
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto response = client.get("/a/b/start");
  CHECK(response && response->body == "ok|/a/final?value=ok");
  server.stop();
}

TEST(multipart_empty_form_round_trips) {
  auto [content_type, body] = chhttp::make_multipart({}, "empty-boundary");
  CHECK(body == "--empty-boundary--\r\n");
  auto parsed = chhttp::parse_multipart(body, content_type);
  CHECK(parsed && parsed->empty());
}

TEST(multipart_custom_headers_survive_round_trip) {
  chhttp::MultipartPart part{.name = "file",
                             .filename = "agent.bin",
                             .content_type = "application/octet-stream",
                             .content = "payload"};
  part.headers.add("X-Checksum", "abc123");
  part.headers.add("Content-Type", "must-not-duplicate");
  auto [content_type, body] =
      chhttp::make_multipart({part}, "custom-header-boundary");
  auto parsed = chhttp::parse_multipart(body, content_type);
  CHECK(parsed && parsed->size() == 1);
  CHECK((*parsed)[0].headers.get("X-Checksum") == "abc123");
  CHECK((*parsed)[0].headers.get_all("Content-Type").size() == 1);
  CHECK((*parsed)[0].content_type == "application/octet-stream");
}

TEST(multipart_boundary_length_and_character_limits) {
  const std::string maximum(70, 'b');
  auto [content_type, body] = chhttp::make_multipart({}, maximum);
  CHECK(content_type.ends_with(maximum));
  CHECK(chhttp::parse_multipart(body, content_type));

  const std::string excessive(71, 'b');
  auto invalid = chhttp::parse_multipart(
      "--" + excessive + "--\r\n",
      "multipart/form-data; boundary=" + excessive);
  CHECK(!invalid && invalid.error().code == chhttp::Error::multipart);
  CHECK(!chhttp::parse_multipart(
      "--bad@boundary--\r\n",
      "multipart/form-data; boundary=bad@boundary"));
}

TEST(multipart_accepts_preamble_and_epilogue) {
  const std::string body =
      "preamble ignored\r\n--b\r\n"
      "Content-Disposition: form-data; name=\"field\"\r\n\r\n"
      "value\r\n--b--\r\nepilogue ignored";
  auto parsed = chhttp::parse_multipart(
      body, "multipart/form-data; boundary=b");
  CHECK(parsed && parsed->size() == 1);
  CHECK((*parsed)[0].name == "field" && (*parsed)[0].content == "value");
}

TEST(authentication_helpers_and_digest_retry) {
  CHECK(chhttp::basic_auth("Aladdin", "open sesame") ==
        "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==");
  CHECK(chhttp::bearer_auth("token") == "Bearer token");
  auto challenge = chhttp::parse_digest_challenge(
      "Digest realm=\"testrealm@host.com\", "
      "qop=\"auth,auth-int\", algorithm=MD5, "
      "nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\", "
      "opaque=\"5ccc069c403ebaf9f0171e9517f40e41\"");
  CHECK(challenge);
  auto authorization = chhttp::digest_auth(
      "GET", "/dir/index.html", "Mufasa", "Circle Of Life", *challenge, 1,
      "0a4f113b");
  CHECK(authorization);
  CHECK(authorization->find(
            "response=\"6629fae49393a05397450978507c4ef1\"") !=
        std::string::npos);

  chhttp::ClientOptions basic_options;
  basic_options.authentication = {
      .type = chhttp::AuthenticationType::basic,
      .username = "agent",
      .password = "secret"};
  chhttp::Client basic_client(fixture().base_url, std::move(basic_options));
  auto basic = basic_client.get("/authorization");
  CHECK(basic && basic->body == chhttp::basic_auth("agent", "secret"));

  chhttp::ClientOptions digest_options;
  digest_options.authentication = {
      .type = chhttp::AuthenticationType::digest,
      .username = "agent",
      .password = "secret"};
  chhttp::Client digest_client(fixture().base_url, std::move(digest_options));
  auto digest = digest_client.get("/digest");
  CHECK(digest && digest->status == 200);
  CHECK(digest->body.starts_with("Digest username=\"agent\""));
  CHECK(digest->body.find("algorithm=SHA-256") != std::string::npos);
  CHECK(digest->body.find("uri=\"/digest\"") != std::string::npos);
}

TEST(digest_helpers_reject_malformed_and_unsupported_challenges) {
  CHECK(!chhttp::parse_digest_challenge("Basic realm=\"x\""));
  CHECK(!chhttp::parse_digest_challenge("Digest realm=\"x\""));
  CHECK(!chhttp::parse_digest_challenge(
      "Digest realm=\"x\", nonce=\"unterminated"));
  chhttp::DigestChallenge unsupported{.realm = "r",
                                      .nonce = "n",
                                      .algorithm = "SHA-1",
                                      .qop = "auth"};
  CHECK(!chhttp::digest_auth("GET", "/", "u", "p", unsupported));
  unsupported.algorithm = "MD5";
  unsupported.qop = "auth-int";
  CHECK(!chhttp::digest_auth("GET", "/", "u", "p", unsupported));
}

TEST(digest_helpers_support_session_and_sha512_256_variants) {
  for (const auto *algorithm : {"MD5-sess", "SHA-256-sess", "SHA-512-256"}) {
    chhttp::DigestChallenge challenge{.realm = "agents",
                                      .nonce = "nonce",
                                      .opaque = "opaque",
                                      .algorithm = algorithm,
                                      .qop = "auth"};
    auto result = chhttp::digest_auth("POST", "/v1/run", "agent", "secret",
                                      challenge, 42, "fixed-cnonce");
    CHECK(result);
    CHECK(result->find("algorithm=" + std::string(algorithm)) !=
          std::string::npos);
    CHECK(result->find("nc=0000002a") != std::string::npos);
  }
  chhttp::DigestChallenge legacy{.realm = "r", .nonce = "n"};
  auto without_qop =
      chhttp::digest_auth("GET", "/", "u", "p", legacy, 1, "c");
  CHECK(without_qop && without_qop->find("qop=") == std::string::npos);
}

TEST(status_reason_and_mime_type_cover_known_and_unknown_values) {
  CHECK(chhttp::status_reason(100) == "Continue");
  CHECK(chhttp::status_reason(418) == "I'm a teapot");
  CHECK(chhttp::status_reason(511) == "Network Authentication Required");
  CHECK(chhttp::status_reason(799) == "Unknown");
  CHECK(chhttp::mime_type("index.HTML") == "text/html; charset=utf-8");
  CHECK(chhttp::mime_type("module.wasm") == "application/wasm");
  CHECK(chhttp::mime_type("artifact.unknown") ==
        "application/octet-stream");
}

TEST(sse_formatter_handles_multiline_empty_and_metadata_fields) {
  const auto formatted = chhttp::format_sse(
      {.data = "one\ntwo\n",
       .event = "token",
       .id = "42",
       .retry = std::chrono::milliseconds(1500)});
  CHECK(formatted ==
        "event: token\nid: 42\nretry: 1500\ndata: one\ndata: two\n"
        "data: \n\n");
  CHECK(chhttp::format_sse({.data = ""}) == "data: \n\n");
}

TEST(route_patterns_support_colon_wildcard_and_regular_expressions) {
  chhttp::Server server;
  server.get("/items/:id", [](const chhttp::Request &request,
                               chhttp::Response &response) {
    response.set_content("colon:" + request.path_params.at("id"));
  });
  server.get("/files/*", [](const chhttp::Request &request,
                             chhttp::Response &response) {
    response.set_content("wild:" + request.path_params.at("wildcard"));
  });
  server.get("regex:^/v[0-9]+$", [](const chhttp::Request &,
                                     chhttp::Response &response) {
    response.set_content("regex");
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  CHECK(client.get("/items/a%20b")->body == "colon:a b");
  CHECK(client.get("/files/a/b/c")->body == "wild:a/b/c");
  CHECK(client.get("/v123")->body == "regex");
  CHECK(client.get("/vabc")->status == 404);
  server.stop();
}

TEST(server_accepts_options_and_arbitrary_extension_methods) {
  chhttp::Server server;
  server.options("/resource", [](const chhttp::Request &,
                                  chhttp::Response &response) {
    response.status = 204;
    response.headers.set("Allow", "OPTIONS, PROPFIND");
  });
  server.route("PROPFIND", "/resource",
               [](const chhttp::Request &request,
                  chhttp::Response &response) {
                 response.set_content(request.method);
               });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  chhttp::Request options;
  options.method = "OPTIONS";
  options.target = "/resource";
  auto options_response = client.request(std::move(options));
  CHECK(options_response && options_response->status == 204);
  CHECK(options_response->headers.get("Allow") == "OPTIONS, PROPFIND");
  chhttp::Request propfind;
  propfind.method = "PROPFIND";
  propfind.target = "/resource";
  auto propfind_response = client.request(std::move(propfind));
  CHECK(propfind_response && propfind_response->body == "PROPFIND");
  server.stop();
}

TEST(head_requests_fall_back_to_get_routes_without_a_body) {
  chhttp::Server server;
  server.get("/head", [](const chhttp::Request &request,
                          chhttp::Response &response) {
    response.set_content("generated-for-" + request.method);
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto response = client.head("/head");
  CHECK(response && response->status == 200 && response->body.empty());
  CHECK(response->headers.get("Content-Length") ==
        std::to_string(std::string("generated-for-HEAD").size()));
  server.stop();
}

TEST(custom_exception_handler_and_logger_are_isolated) {
  chhttp::Server server;
  std::atomic_int logged{0};
  server.set_exception_handler(
      [](const chhttp::Request &, chhttp::Response &response,
         std::exception_ptr exception) {
        try {
          std::rethrow_exception(exception);
        } catch (const std::runtime_error &error) {
          response.status = 599;
          response.set_content(std::string("caught:") + error.what());
        }
      });
  server.set_logger([&](const chhttp::Request &, const chhttp::Response &) {
    ++logged;
    throw std::runtime_error("logger exceptions are ignored");
  });
  server.get("/throw", [](const chhttp::Request &, chhttp::Response &) {
    throw std::runtime_error("handler");
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto response = client.get("/throw");
  CHECK(response && response->status == 599 &&
        response->body == "caught:handler");
  CHECK(logged == 1);
  server.stop();
}

TEST(request_helpers_expose_duplicate_query_and_decoded_path_parameters) {
  chhttp::Server server;
  server.get("/query/{id}", [](const chhttp::Request &request,
                                chhttp::Response &response) {
    std::string body = request.path_params.at("id") + "|";
    for (const auto &[key, value] : request.query)
      body += key + "=" + value + ";";
    body += request.has_param("missing") ? "bad" : "fallback";
    response.set_content(body);
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto response = client.get("/query/a%20b?x=1&x=2&empty=");
  CHECK(response && response->body ==
                        "a b|x=1;x=2;empty=;fallback");
  server.stop();
}

TEST(server_accepts_absolute_form_request_targets) {
  chhttp::Server server;
  server.get("/absolute", [](const chhttp::Request &request,
                              chhttp::Response &response) {
    response.set_content(request.path + "|" + request.get_param("x"));
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto authority = "127.0.0.1:" + std::to_string(server.port());
  const auto response = raw_http_exchange(
      server.port(), "GET http://" + authority +
                         "/absolute?x=1 HTTP/1.1\r\nHost: " + authority +
                         "\r\nConnection: close\r\n\r\n");
  CHECK(response.starts_with("HTTP/1.1 200 OK\r\n"));
  CHECK(response.ends_with("/absolute|1"));
  server.stop();
}

TEST(sync_client_routes_methods_and_headers) {
  chhttp::Client client(fixture().base_url);
  auto hello = client.get("/hello?who=agent");
  CHECK(hello && hello->status == 200 && hello->body == "hello:agent");
  CHECK(hello->headers.get_all("Set-Cookie").size() == 2);
  CHECK(hello->headers.get("X-Post-Route") == "yes");
  auto route = client.get("/users/a%20b");
  CHECK(route && route->body == "a b");
  auto post = client.post("/echo", "body", "text/custom");
  CHECK(post && post->body == "POST:body");
  auto put = client.put("/echo", "put", "text/plain");
  CHECK(put && put->body == "PUT:put");
  auto patch = client.patch("/echo", "patch", "text/plain");
  CHECK(patch && patch->body == "PATCH:patch");
  auto deleted = client.del("/echo");
  CHECK(deleted && deleted->body == "DELETE:");
  auto head = client.head("/hello");
  CHECK(head && head->body.empty());
}

TEST(middleware_error_exception_and_redirect) {
  chhttp::Client client(fixture().base_url);
  auto blocked = client.get("/blocked");
  CHECK(blocked && blocked->status == 403 && blocked->body == "blocked");
  auto missing = client.get("/missing");
  CHECK(missing && missing->status == 404 && missing->body == "custom-not-found");
  auto failed = client.get("/fail");
  CHECK(failed && failed->status == 500);
  auto redirected = client.get("/redirect");
  CHECK(redirected && redirected->body == "hello:redirected");

  chhttp::ClientOptions redirect_options;
  redirect_options.max_redirects = 2;
  chhttp::Client redirect_client(fixture().base_url,
                                 std::move(redirect_options));
  auto loop = redirect_client.get("/redirect-loop");
  CHECK(!loop && loop.error().code == chhttp::Error::redirect_limit);
}

TEST(payload_limits_timeouts_and_invalid_urls) {
  chhttp::ServerOptions limited_server_options;
  limited_server_options.max_body_size = 8;
  chhttp::Server limited_server(std::move(limited_server_options));
  limited_server.post("/body", [](const chhttp::Request &request,
                                   chhttp::Response &response) {
    response.set_content(request.body);
  });
  CHECK(limited_server.start("127.0.0.1", 0));
  chhttp::Client upload_client(
      "http://127.0.0.1:" + std::to_string(limited_server.port()));
  auto upload = upload_client.post("/body", std::string(32, 'x'),
                                   "application/octet-stream");
  CHECK(upload && upload->status == 413);
  limited_server.stop();

  chhttp::ClientOptions response_limit_options;
  response_limit_options.max_response_body_size = 16;
  chhttp::Client response_limit_client(fixture().base_url,
                                       std::move(response_limit_options));
  auto too_large = response_limit_client.get("/large");
  CHECK(!too_large &&
        too_large.error().code == chhttp::Error::body_too_large);

  chhttp::ClientOptions timeout_options;
  timeout_options.read_timeout = std::chrono::milliseconds(10);
  chhttp::Client timeout_client(fixture().base_url, std::move(timeout_options));
  auto timed_out = timeout_client.get("/slow");
  CHECK(!timed_out && timed_out.error().code == chhttp::Error::timeout);

  chhttp::Client invalid_client("not-a-url");
  auto invalid = invalid_client.get("/");
  CHECK(!invalid && invalid.error().code == chhttp::Error::invalid_url);
}

TEST(cross_origin_redirect_strips_authorization) {
  chhttp::Server target;
  target.get("/target", [](const chhttp::Request &request,
                            chhttp::Response &response) {
    response.set_content(request.get_header("Authorization"));
  });
  CHECK(target.start("127.0.0.1", 0));
  chhttp::Server origin;
  origin.get("/start", [&](const chhttp::Request &,
                            chhttp::Response &response) {
    response.set_redirect("http://127.0.0.1:" +
                          std::to_string(target.port()) + "/target");
  });
  CHECK(origin.start("127.0.0.1", 0));
  chhttp::ClientOptions client_options;
  client_options.authentication = {
      .type = chhttp::AuthenticationType::basic,
      .username = "agent",
      .password = "secret"};
  chhttp::Client client(
      "http://127.0.0.1:" + std::to_string(origin.port()),
      std::move(client_options));
  auto response = client.get("/start");
  CHECK(response && response->body.empty());
  origin.stop();
  target.stop();
}

TEST(compression_and_stream_callbacks) {
  chhttp::Client client(fixture().base_url);
  auto large = client.get("/large");
  CHECK(large && large->body == std::string(128 * 1024, 'x'));
  std::string streamed;
  auto stream = client.get(
      "/stream", {},
      {.on_data = [&](std::string_view chunk) {
         streamed.append(chunk);
         return true;
       }});
  CHECK(stream && streamed == "onetwo" && stream->body.empty());
  for (const auto *encoding : {"gzip", "deflate", "br", "zstd"}) {
    auto encoded = client.get("/large", {{"Accept-Encoding", encoding}});
    CHECK(encoded && encoded->body == std::string(128 * 1024, 'x'));
    CHECK(!encoded->headers.contains("Content-Encoding"));
  }
}

TEST(static_files_and_ranges) {
  chhttp::Client client(fixture().base_url);
  auto file = client.get("/static/asset.txt");
  CHECK(file && file->body == "0123456789");
  CHECK(file->headers.get("Cache-Control") == "public, max-age=60");
  auto range = client.get("/static/asset.txt", {{"Range", "bytes=2-5"}});
  CHECK(range && range->status == 206 && range->body == "2345");
  CHECK(range->headers.get("Content-Range") == "bytes 2-5/10");
  auto suffix = client.get("/static/asset.txt", {{"Range", "bytes=-3"}});
  CHECK(suffix && suffix->status == 206 && suffix->body == "789");
  auto open_ended =
      client.get("/static/asset.txt", {{"Range", "bytes=7-"}});
  CHECK(open_ended && open_ended->status == 206 && open_ended->body == "789");
  auto clamped =
      client.get("/static/asset.txt", {{"Range", "bytes=8-999"}});
  CHECK(clamped && clamped->status == 206 && clamped->body == "89");
  auto invalid = client.get("/static/asset.txt", {{"Range", "bytes=100-200"}});
  CHECK(invalid && invalid->status == 416);
  auto multiple =
      client.get("/static/asset.txt", {{"Range", "bytes=0-1,3-4"}});
  CHECK(multiple && multiple->status == 416);
  auto traversal = client.get("/static/%2e%2e/secret.txt");
  CHECK(traversal && traversal->status == 404);
}

TEST(multipart_server_parsing) {
  chhttp::MultipartForm parts{{.name = "a", .content = "one"},
                              {.name = "b", .content = "two"}};
  auto [type, body] = chhttp::make_multipart(parts);
  chhttp::Client client(fixture().base_url);
  auto response = client.post("/multipart", std::move(body), std::move(type));
  CHECK(response && response->body == "a=one;b=two;");
}

TEST(native_http_protocol_framing_pipeline_and_expect) {
  chhttp::Server server;
  server.post("/raw", [](const chhttp::Request &request,
                          chhttp::Response &response) {
    response.set_content(request.body + "|" +
                         request.headers.get("X-Checksum"));
  });
  server.get("/next", [](const chhttp::Request &,
                          chhttp::Response &response) {
    response.set_content("next");
  });
  server.get_async(
      "/stream", [](const chhttp::Request &,
                     chhttp::Response &response) -> chhttp::Task<void> {
        response.set_stream(
            "text/plain", [](chhttp::StreamWriter &writer)
                              -> chhttp::Task<void> {
              CHECK(co_await writer.write("alpha"));
              CHECK(co_await writer.write("beta"));
            });
        co_return;
      });
  server.get("/empty", [](const chhttp::Request &,
                           chhttp::Response &response) {
    response.status = 204;
    response.set_content("must-not-be-framed");
  });
  CHECK(server.start("127.0.0.1", 0));

  const auto pipelined = raw_http_exchange(
      server.port(),
      "POST /raw HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Transfer-Encoding: chunked\r\n"
      "Connection: keep-alive\r\n\r\n"
      "4;extension=yes\r\nWiki\r\n"
      "5\r\npedia\r\n"
      "0\r\nX-Checksum: ok\r\n\r\n"
      "GET /next HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Connection: close\r\n\r\n");
  CHECK(pipelined.find("Wikipedia|ok") != std::string::npos);
  CHECK(pipelined.find("next") != std::string::npos);
  const auto first_status = pipelined.find("HTTP/1.1 200 OK");
  CHECK(first_status != std::string::npos);
  CHECK(pipelined.find("HTTP/1.1 200 OK", first_status + 1) !=
        std::string::npos);

  const auto expected = raw_http_exchange(
      server.port(),
      "POST /raw HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Content-Length: 4\r\n"
      "Expect: 100-continue\r\n"
      "Connection: close\r\n\r\nbody");
  CHECK(expected.starts_with("HTTP/1.1 100 Continue\r\n\r\n"));
  CHECK(expected.find("body|") != std::string::npos);

  const auto http10 = raw_http_exchange(
      server.port(),
      "GET /next HTTP/1.0\r\nConnection: close\r\n\r\n");
  CHECK(http10.starts_with("HTTP/1.0 200 OK\r\n"));
  const auto http10_stream = raw_http_exchange(
      server.port(),
      "GET /stream HTTP/1.0\r\nConnection: close\r\n\r\n");
  CHECK(http10_stream.starts_with("HTTP/1.0 200 OK\r\n"));
  CHECK(http10_stream.find("Transfer-Encoding") == std::string::npos);
  CHECK(http10_stream.find("Content-Length") == std::string::npos);
  CHECK(http10_stream.ends_with("\r\n\r\nalphabeta"));
  const auto no_content = raw_http_exchange(
      server.port(),
      "GET /empty HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
  CHECK(no_content.starts_with("HTTP/1.1 204 No Content\r\n"));
  CHECK(no_content.find("Content-Length") == std::string::npos);
  CHECK(no_content.find("must-not-be-framed") == std::string::npos);
  server.stop();
}

TEST(http10_keep_alive_can_process_multiple_pipelined_requests) {
  chhttp::Server server;
  server.get("/one", [](const chhttp::Request &,
                         chhttp::Response &response) {
    response.set_content("one");
  });
  server.get("/two", [](const chhttp::Request &,
                         chhttp::Response &response) {
    response.set_content("two");
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto response = raw_http_exchange(
      server.port(),
      "GET /one HTTP/1.0\r\nConnection: keep-alive\r\n\r\n"
      "GET /two HTTP/1.0\r\nConnection: close\r\n\r\n");
  const auto first = response.find("HTTP/1.0 200 OK");
  CHECK(first != std::string::npos);
  CHECK(response.find("HTTP/1.0 200 OK", first + 1) != std::string::npos);
  CHECK(response.find("one") != std::string::npos);
  CHECK(response.ends_with("two"));
  server.stop();
}

TEST(server_keep_alive_request_cap_closes_after_exact_limit) {
  chhttp::ServerOptions options;
  options.keep_alive_max_requests = 2;
  chhttp::Server server(std::move(options));
  std::atomic_int handled{0};
  server.get("/", [&](const chhttp::Request &, chhttp::Response &response) {
    response.set_content(std::to_string(++handled));
  });
  CHECK(server.start("127.0.0.1", 0));
  const std::string request =
      "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
      "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
      "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
  const auto response = raw_http_exchange(server.port(), request);
  std::size_t count = 0;
  for (std::size_t cursor = 0;
       (cursor = response.find("HTTP/1.1 200 OK", cursor)) !=
       std::string::npos;
       cursor += 15)
    ++count;
  CHECK(count == 2 && handled == 2);
  CHECK(response.find("Connection: close") != std::string::npos);
  server.stop();
}

TEST(content_length_zero_is_a_valid_empty_request_body) {
  chhttp::Server server;
  server.post("/", [](const chhttp::Request &request,
                       chhttp::Response &response) {
    response.set_content(request.body.empty() ? "empty" : "unexpected");
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto response = raw_http_exchange(
      server.port(),
      "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n"
      "Connection: close\r\n\r\n");
  CHECK(response.starts_with("HTTP/1.1 200 OK\r\n"));
  CHECK(response.ends_with("empty"));
  server.stop();
}

TEST(chunked_zero_chunk_is_a_valid_empty_request_body) {
  chhttp::Server server;
  server.post("/", [](const chhttp::Request &request,
                       chhttp::Response &response) {
    response.set_content(request.body.empty() ? "empty" : "unexpected");
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto response = raw_http_exchange(
      server.port(),
      "POST / HTTP/1.1\r\nHost: localhost\r\n"
      "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
      "0\r\n\r\n");
  CHECK(response.starts_with("HTTP/1.1 200 OK\r\n"));
  CHECK(response.ends_with("empty"));
  server.stop();
}

TEST(chunk_extensions_accept_valid_quoted_and_token_values) {
  chhttp::Server server;
  server.post("/", [](const chhttp::Request &request,
                       chhttp::Response &response) {
    response.set_content(request.body);
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto response = raw_http_exchange(
      server.port(),
      "POST / HTTP/1.1\r\nHost: localhost\r\n"
      "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
      "3;name=token;quoted=\"yes\"\r\nabc\r\n0\r\n\r\n");
  CHECK(response.starts_with("HTTP/1.1 200 OK\r\n"));
  CHECK(response.ends_with("abc"));
  server.stop();
}

TEST(chunked_trailers_preserve_duplicate_field_values) {
  chhttp::Server server;
  server.post("/", [](const chhttp::Request &request,
                       chhttp::Response &response) {
    const auto values = request.headers.get_all("X-Trailer");
    response.set_content(std::to_string(values.size()) + "|" + values[0] +
                         "|" + values[1]);
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto response = raw_http_exchange(
      server.port(),
      "POST / HTTP/1.1\r\nHost: localhost\r\n"
      "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
      "1\r\nx\r\n0\r\nX-Trailer: one\r\nX-Trailer: two\r\n\r\n");
  CHECK(response.starts_with("HTTP/1.1 200 OK\r\n"));
  CHECK(response.ends_with("2|one|two"));
  server.stop();
}

TEST(inbound_header_optional_whitespace_is_trimmed) {
  chhttp::Server server;
  server.get("/", [](const chhttp::Request &request,
                      chhttp::Response &response) {
    response.set_content("[" + request.get_header("X-OWS") + "]");
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto response = raw_http_exchange(
      server.port(),
      "GET / HTTP/1.1\r\nHost: localhost\r\nX-OWS:\t  value \t\r\n"
      "Connection: close\r\n\r\n");
  CHECK(response.ends_with("[value]"));
  server.stop();
}

TEST(inbound_header_control_characters_are_rejected) {
  chhttp::Server server;
  server.get("/", [](const chhttp::Request &, chhttp::Response &response) {
    response.set_content("must-not-run");
  });
  CHECK(server.start("127.0.0.1", 0));
  std::string request =
      "GET / HTTP/1.1\r\nHost: localhost\r\nX-Bad: value";
  request.push_back('\x01');
  request += "control\r\nConnection: close\r\n\r\n";
  const auto response = raw_http_exchange(server.port(), request);
  CHECK(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
  CHECK(response.find("must-not-run") == std::string::npos);
  server.stop();
}

TEST(inbound_header_names_with_whitespace_are_rejected) {
  chhttp::Server server;
  server.get("/", [](const chhttp::Request &, chhttp::Response &response) {
    response.set_content("must-not-run");
  });
  CHECK(server.start("127.0.0.1", 0));
  for (const auto *header : {"Bad Name: value", "Bad\tName: value",
                             "Bad@: value"}) {
    const auto response = raw_http_exchange(
        server.port(), "GET / HTTP/1.1\r\nHost: localhost\r\n" +
                           std::string(header) +
                           "\r\nConnection: close\r\n\r\n");
    CHECK(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
  }
  server.stop();
}

TEST(content_length_rejects_signs_suffixes_conflicts_and_overflow) {
  chhttp::Server server;
  server.post("/", [](const chhttp::Request &, chhttp::Response &response) {
    response.set_content("must-not-run");
  });
  CHECK(server.start("127.0.0.1", 0));
  for (const auto *length : {"+1", "-1", "1x", "1, 2",
                             "18446744073709551616"}) {
    const auto response = raw_http_exchange(
        server.port(), "POST / HTTP/1.1\r\nHost: localhost\r\n"
                       "Content-Length: " +
                           std::string(length) +
                           "\r\nConnection: close\r\n\r\nx");
    CHECK(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
  }
  server.stop();
}

TEST(native_http_protocol_rejects_request_smuggling) {
  chhttp::Server server;
  server.post("/", [](const chhttp::Request &,
                       chhttp::Response &response) {
    response.set_content("must-not-run");
  });
  CHECK(server.start("127.0.0.1", 0));
  std::size_t malformed_index = 0;
  for (const auto &request : {
           std::string("POST / HTTP/1.1\r\nHost: localhost\r\n"
                       "Content-Length: 4\r\nContent-Length: 5\r\n"
                       "Connection: close\r\n\r\nabcde"),
           std::string("POST / HTTP/1.1\r\nHost: localhost\r\n"
                       "Content-Length: 4\r\nTransfer-Encoding: chunked\r\n"
                       "Connection: close\r\n\r\n0\r\n\r\n"),
           std::string("POST / HTTP/1.1\r\nHost: one\r\nHost: two\r\n"
                       "Content-Length: 0\r\nConnection: close\r\n\r\n"),
           std::string("POST / HTTP/1.1\r\nHost: localhost\r\n"
                       "Content-Length:\r\nConnection: close\r\n\r\n"),
           std::string("POST / HTTP/1.1\r\nHost: localhost\r\n"
                       "Transfer-Encoding:\r\nConnection: close\r\n\r\n"),
           std::string("POST / HTTP/1.1\r\nHost: localhost\r\n"
                       "Content-Length: 4,\r\nConnection: close\r\n\r\ntest"),
           std::string("POST / HTTP/1.1\r\nHost: localhost\r\n"
                       "Transfer-Encoding: ,chunked\r\n"
                       "Connection: close\r\n\r\n0\r\n\r\n"),
           std::string("POST / HTTP/1.1\r\nHost:\r\n"
                       "Content-Length: 0\r\nConnection: close\r\n\r\n")}) {
    const auto response = raw_http_exchange(server.port(), request);
    if (!response.starts_with("HTTP/1.1 400 Bad Request\r\n"))
      throw Failure("Malformed request case " +
                    std::to_string(malformed_index) +
                    " did not produce HTTP 400; response=" + response);
    CHECK(response.find("must-not-run") == std::string::npos);
    ++malformed_index;
  }
  server.stop();
}

TEST(native_http_server_incremental_syntax_and_size_boundaries) {
  chhttp::ServerOptions options;
  options.worker_threads = 1;
  options.max_header_size = 128;
  options.max_body_size = 8;
  chhttp::Server server(std::move(options));
  server.post("/raw", [](const chhttp::Request &request,
                          chhttp::Response &response) {
    response.set_content("accepted:" + request.body);
  });
  server.get("/", [](const chhttp::Request &, chhttp::Response &response) {
    response.set_content("must-not-run");
  });
  CHECK(server.start("127.0.0.1", 0));

  const auto bytewise = raw_http_exchange(
      server.port(),
      "POST /raw HTTP/1.1\r\nHost: localhost\r\n"
      "Content-Length: 4, 4\r\nConnection: close\r\n\r\ntest",
      1);
  CHECK(bytewise.starts_with("HTTP/1.1 200 OK\r\n"));
  CHECK(bytewise.ends_with("accepted:test"));

  std::size_t malformed_index = 0;
  for (const auto &request : {
           std::string("GET / HTTP/1.1\nHost: localhost\n\n"),
           std::string("GET / HTTP/1.1\r\nHost: localhost\r\n"
                       " X-Folded: rejected\r\n\r\n"),
           std::string("GET / HTTP/1.1\r\nHost: localhost\r\nBroken\r\n\r\n"),
           std::string("GET / HTTP/2\r\nHost: localhost\r\n\r\n"),
           std::string("GET / HTTP/1.1\r\nHost: one,two\r\n\r\n"),
           std::string("GET / HTTP/1.1\r\nHost: localhost\r\n"
                       "Expect: something-else\r\n\r\n"),
           std::string("POST /raw HTTP/1.1\r\nHost: localhost\r\n"
                       "Transfer-Encoding: gzip\r\n\r\n"),
           std::string("POST /raw HTTP/1.1\r\nHost: localhost\r\n"
                       "Transfer-Encoding: chunked\r\n\r\nZ\r\n"),
           std::string("POST /raw HTTP/1.1\r\nHost: localhost\r\n"
                       "Transfer-Encoding: chunked\r\n\r\n1\r\naX"),
           std::string("POST /raw HTTP/1.1\r\nHost: localhost\r\n"
                       "Transfer-Encoding: chunked\r\n\r\n"
                       "0\r\nContent-Length: 0\r\n\r\n"),
           std::string("GET /%XX HTTP/1.1\r\nHost: localhost\r\n\r\n")}) {
    const auto response = raw_http_exchange(server.port(), request);
    if (!response.starts_with("HTTP/1.1 400 Bad Request\r\n"))
      throw Failure("Syntax boundary case " +
                    std::to_string(malformed_index) +
                    " did not produce HTTP 400; response=" + response);
    CHECK(response.find("must-not-run") == std::string::npos);
    ++malformed_index;
  }

  const std::string oversized_header =
      "GET / HTTP/1.1\r\nHost: localhost\r\nX-Large: " +
      std::string(160, 'h') + "\r\n\r\n";
  const auto header_response =
      raw_http_exchange(server.port(), oversized_header);
  CHECK(header_response.starts_with("HTTP/1.1 400 Bad Request\r\n"));

  const auto fixed_body_response = raw_http_exchange(
      server.port(),
      "POST /raw HTTP/1.1\r\nHost: localhost\r\n"
      "Content-Length: 9\r\nConnection: close\r\n\r\n123456789");
  CHECK(fixed_body_response.starts_with(
      "HTTP/1.1 413 Payload Too Large\r\n"));
  const auto chunked_body_response = raw_http_exchange(
      server.port(),
      "POST /raw HTTP/1.1\r\nHost: localhost\r\n"
      "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
      "9\r\n123456789\r\n0\r\n\r\n");
  CHECK(chunked_body_response.starts_with(
      "HTTP/1.1 413 Payload Too Large\r\n"));
  server.stop();
}

TEST(transfer_encoding_rejects_parameters_and_multiple_codings) {
  chhttp::Server server;
  server.post("/", [](const chhttp::Request &, chhttp::Response &response) {
    response.set_content("must-not-run");
  });
  CHECK(server.start("127.0.0.1", 0));
  for (const auto *encoding : {"chunked;foo=bar", "gzip, chunked",
                               "chunked, chunked", "identity"}) {
    const auto response = raw_http_exchange(
        server.port(), "POST / HTTP/1.1\r\nHost: localhost\r\n"
                       "Transfer-Encoding: " +
                           std::string(encoding) +
                           "\r\nConnection: close\r\n\r\n0\r\n\r\n");
    CHECK(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
  }
  server.stop();
}

TEST(chunk_size_lines_over_16kib_are_rejected) {
  chhttp::Server server;
  server.post("/", [](const chhttp::Request &, chhttp::Response &response) {
    response.set_content("must-not-run");
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto request =
      "POST / HTTP/1.1\r\nHost: localhost\r\n"
      "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n" +
      std::string(17 * 1024, '1') + "\r\n";
  const auto response = raw_http_exchange(server.port(), request);
  CHECK(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
  server.stop();
}

TEST(chunked_trailers_obey_the_header_size_limit) {
  chhttp::ServerOptions options;
  options.max_header_size = 128;
  chhttp::Server server(std::move(options));
  server.post("/", [](const chhttp::Request &, chhttp::Response &response) {
    response.set_content("must-not-run");
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto request =
      "POST / HTTP/1.1\r\nHost: localhost\r\n"
      "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
      "0\r\nX-Large: " +
      std::string(160, 'x') + "\r\n\r\n";
  const auto response = raw_http_exchange(server.port(), request);
  CHECK(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
  server.stop();
}

TEST(head_clients_ignore_illegal_wire_bodies) {
  RawResponseServer server(
      "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n"
      "Connection: close\r\n\r\nbody",
      true);
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto response = client.head("/");
  CHECK(response && response->status == 200 && response->body.empty());
  CHECK(response->headers.get("Content-Length") == "4");
}

TEST(no_body_statuses_ignore_illegal_wire_bodies) {
  for (const int status : {205, 304}) {
    auto response = fetch_raw_response(
        "HTTP/1.1 " + std::to_string(status) + " " +
        chhttp::status_reason(status) +
        "\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody",
        true);
    CHECK(response && response->status == status && response->body.empty());
  }
}

TEST(zero_length_and_empty_chunked_responses_are_valid) {
  auto fixed = fetch_raw_response(
      "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
      "Connection: close\r\n\r\n");
  CHECK(fixed && fixed->body.empty());
  auto chunked = fetch_raw_response(
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
      "Connection: close\r\n\r\n0\r\n\r\n",
      true);
  CHECK(chunked && chunked->body.empty());
}

TEST(client_rejects_response_header_control_characters) {
  std::string response = "HTTP/1.1 200 OK\r\nX-Bad: value";
  response.push_back('\x01');
  response += "control\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
  auto result = fetch_raw_response(std::move(response), true);
  CHECK(!result && result.error().code == chhttp::Error::protocol);
}

TEST(client_rejects_empty_and_invalid_chunk_extensions) {
  for (const auto &line : {std::string("1;\r\na\r\n0\r\n\r\n"),
                           std::string("1;") + std::string(1, '\x01') +
                               "\r\na\r\n0\r\n\r\n"}) {
    auto response = fetch_raw_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
        "Connection: close\r\n\r\n" +
            line,
        true);
    CHECK(!response && response.error().code == chhttp::Error::protocol);
  }
}

TEST(client_body_limit_applies_before_reading_large_chunks) {
  chhttp::ClientOptions options;
  options.max_response_body_size = 4;
  auto response = fetch_raw_response(
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
      "Connection: close\r\n\r\n5\r\nabcde\r\n0\r\n\r\n",
      false, std::move(options));
  CHECK(!response && response.error().code == chhttp::Error::body_too_large);
}

TEST(stream_callbacks_preserve_byte_order_and_monotonic_progress) {
  const std::string body = "abcdefghijklmnopqrstuvwxyz";
  RawResponseServer server(
      "HTTP/1.1 200 OK\r\nContent-Length: " +
          std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body,
      true);
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  std::string received;
  std::vector<std::uint64_t> progress;
  auto response = client.get(
      "/", {},
      {.on_data = [&](std::string_view chunk) {
         received += chunk;
         return true;
       },
       .on_progress = [&](std::uint64_t current, std::uint64_t total) {
         CHECK(total == body.size());
         progress.push_back(current);
         return true;
       }});
  CHECK(response && response->body.empty() && received == body);
  CHECK(!progress.empty() && progress.back() == body.size());
  CHECK(std::ranges::is_sorted(progress));
}

TEST(native_http_client_incremental_chunked_and_malformed_response) {
  {
    RawResponseServer server(
        "HTTP/1.1 200 OK\r\n"
        "Transfer-Encoding: chunked\r\n"
        "Connection: close\r\n\r\n"
        "3;source=test\r\nabc\r\n"
        "2\r\nde\r\n"
        "0\r\nX-End: yes\r\n\r\n",
        true);
    chhttp::Client client("http://127.0.0.1:" +
                          std::to_string(server.port()));
    auto response = client.get("/");
    CHECK(response && response->body == "abcde");
    CHECK(response->headers.get("X-End") == "yes");
  }
  {
    RawResponseServer server(
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 4\r\n"
        "Transfer-Encoding: chunked\r\n"
        "Connection: close\r\n\r\n"
        "0\r\n\r\n");
    chhttp::Client client("http://127.0.0.1:" +
                          std::to_string(server.port()));
    auto response = client.get("/");
    CHECK(!response && response.error().code == chhttp::Error::protocol);
  }
}

TEST(native_http_client_interim_close_delimited_and_no_body_responses) {
  auto interim = fetch_raw_response(
      "HTTP/1.1 103 Early Hints\r\nLink: </style.css>; rel=preload\r\n\r\n"
      "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n"
      "final",
      true);
  CHECK(interim && interim->status == 200 && interim->body == "final");

  auto close_delimited = fetch_raw_response(
      "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\n\r\nclose-body",
      true);
  CHECK(close_delimited && close_delimited->version == 10 &&
        close_delimited->body == "close-body" && !close_delimited->keep_alive);

  auto duplicate_length = fetch_raw_response(
      "HTTP/1.1 200 OK\r\nContent-Length: 4, 4\r\n"
      "Connection: close\r\n\r\nsame");
  CHECK(duplicate_length && duplicate_length->body == "same");

  auto no_content = fetch_raw_response(
      "HTTP/1.1 204 No Content\r\nContent-Length: 100\r\n"
      "Connection: close\r\n\r\nignored-wire-bytes");
  CHECK(no_content && no_content->status == 204 && no_content->body.empty());
}

TEST(native_http_client_rejects_malformed_and_truncated_responses) {
  const std::vector<std::string> malformed{
      "HTTP/2 200 OK\r\nConnection: close\r\n\r\n",
      "HTTP/1.1 20 Broken\r\nConnection: close\r\n\r\n",
      "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n"
      "Connection: close\r\n\r\nabc",
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n"
      "Connection: close\r\n\r\ndata",
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
      "Connection: close\r\n\r\nZ\r\n",
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
      "Connection: close\r\n\r\n1\r\naX",
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
      "Connection: close\r\n\r\n0\r\nHost: forbidden\r\n\r\n",
      "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n"
      "Connection: close\r\n\r\nshort",
      "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
      " folded: value\r\nConnection: close\r\n\r\n"};
  for (const auto &wire : malformed) {
    auto response = fetch_raw_response(wire, true);
    CHECK(!response && response.error().code == chhttp::Error::protocol);
  }

  auto oversized_headers = fetch_raw_response(
      "HTTP/1.1 200 OK\r\nX-Large: " + std::string(70 * 1024, 'x') +
      "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
  CHECK(!oversized_headers &&
        oversized_headers.error().code == chhttp::Error::protocol);
}

TEST(response_callbacks_can_cancel_body_and_progress) {
  chhttp::Client client(fixture().base_url);
  std::size_t data_calls = 0;
  auto data_cancelled = client.get(
      "/large", {},
      {.on_data = [&](std::string_view) {
         ++data_calls;
         return false;
       }});
  CHECK(!data_cancelled &&
        data_cancelled.error().code == chhttp::Error::cancelled);
  CHECK(data_calls == 1);

  std::uint64_t last_current = 0;
  std::uint64_t last_total = 0;
  auto progress_cancelled = client.get(
      "/large", {},
      {.on_progress = [&](std::uint64_t current, std::uint64_t total) {
         last_current = current;
         last_total = total;
         return false;
       }});
  CHECK(!progress_cancelled &&
        progress_cancelled.error().code == chhttp::Error::cancelled);
  CHECK(last_current > 0 && last_total == 128 * 1024);

#ifdef CHHTTP_HAS_COMPRESSION
  auto corrupt = fetch_raw_response(
      "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 4\r\n"
      "Connection: close\r\n\r\njunk");
  CHECK(!corrupt && corrupt.error().code == chhttp::Error::compression);
#endif
}

TEST(pre_cancelled_request_tokens_avoid_network_work) {
  auto cancellation = std::make_shared<std::atomic_bool>(true);
  chhttp::AsyncClient client(fixture().base_url);
  auto response = client.get(
      "/hello", {}, {.cancellation = std::move(cancellation)}).get();
  CHECK(!response && response.error().code == chhttp::Error::cancelled);
}

TEST(request_tokens_cancel_a_response_while_it_is_inflight) {
  auto cancellation = std::make_shared<std::atomic_bool>(false);
  chhttp::AsyncClient client(fixture().base_url);
  auto response_task =
      client.get("/slow", {}, {.cancellation = cancellation});
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  cancellation->store(true);
  auto response = response_task.get();
  CHECK(!response && response.error().code == chhttp::Error::cancelled);
}

TEST(disabled_redirect_following_returns_the_original_response) {
  chhttp::ClientOptions options;
  options.follow_redirects = false;
  chhttp::Client client(fixture().base_url, std::move(options));
  auto response = client.get("/redirect");
  CHECK(response && response->status == 302 && response->body.empty());
  CHECK(response->headers.get("Location") == "/hello?who=redirected");
}

TEST(post_redirects_rewrite_302_and_preserve_307_requests) {
  chhttp::Server server;
  server.post("/start302", [](const chhttp::Request &,
                               chhttp::Response &response) {
    response.set_redirect("/sink302", 302);
  });
  server.get("/sink302", [](const chhttp::Request &request,
                             chhttp::Response &response) {
    response.set_content(request.method + "|" + request.body);
  });
  server.post("/start307", [](const chhttp::Request &,
                               chhttp::Response &response) {
    response.set_redirect("/sink307", 307);
  });
  server.post("/sink307", [](const chhttp::Request &request,
                              chhttp::Response &response) {
    response.set_content(request.method + "|" + request.body);
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto rewritten = client.post("/start302", "body", "text/plain");
  CHECK(rewritten && rewritten->body == "GET|");
  auto preserved = client.post("/start307", "body", "text/plain");
  CHECK(preserved && preserved->body == "POST|body");
  server.stop();
}

TEST(compression_negotiation_honors_quality_and_request_errors) {
#ifdef CHHTTP_HAS_COMPRESSION
  chhttp::ServerOptions server_options;
  server_options.compression_threshold = 32;
  chhttp::Server server(std::move(server_options));
  server.get("/data", [](const chhttp::Request &,
                          chhttp::Response &response) {
    response.set_content(std::string(4096, 'c'));
  });
  server.post("/upload", [](const chhttp::Request &request,
                             chhttp::Response &response) {
    response.set_content(request.body);
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::ClientOptions options;
  options.auto_decompress = false;
  chhttp::Client client("http://127.0.0.1:" +
                            std::to_string(server.port()),
                        std::move(options));
  auto preferred =
      client.get("/data", {{"Accept-Encoding", "gzip;q=0.2, br;q=0.9"}});
  CHECK(preferred && preferred->headers.get("Content-Encoding") == "br");
  auto disabled = client.get(
      "/data", {{"Accept-Encoding", "gzip;q=0, br;q=0, zstd;q=0"}});
  CHECK(disabled && !disabled->headers.contains("Content-Encoding") &&
        disabled->body == std::string(4096, 'c'));
  auto corrupt = client.post("/upload", "junk", "application/octet-stream",
                             {{"Content-Encoding", "gzip"}});
  CHECK(corrupt && corrupt->status == 400);
  server.stop();
#else
  CHECK(true);
#endif
}

TEST(static_index_unknown_mime_head_and_range_boundaries) {
  std::ofstream(fixture().root / "index.html", std::ios::binary) << "index";
  std::ofstream(fixture().root / "blob.agentdata", std::ios::binary) << "blob";
  chhttp::Client client(fixture().base_url);
  auto index = client.get("/static/");
  CHECK(index && index->body == "index" &&
        index->headers.get("Content-Type") == "text/html; charset=utf-8");
  auto unknown = client.get("/static/blob.agentdata");
  CHECK(unknown && unknown->body == "blob" &&
        unknown->headers.get("Content-Type") == "application/octet-stream");
  auto head = client.head("/static/asset.txt", {{"Range", "bytes=2-4"}});
  CHECK(head && head->status == 206 && head->body.empty());
  CHECK(head->headers.get("Content-Length") == "3");
  CHECK(head->headers.get("Content-Range") == "bytes 2-4/10");
}

TEST(sse_clients_reject_non_event_stream_and_non_200_endpoints) {
  {
    RawResponseServer server(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
        "Content-Length: 4\r\nConnection: close\r\n\r\ndata");
    chhttp::AsyncClient client("http://127.0.0.1:" +
                               std::to_string(server.port()));
    chhttp::SseClient events(client, "/", {}, {.reconnect = false});
    auto error = events.connect().get();
    CHECK(error.code == chhttp::Error::protocol);
  }
  {
    RawResponseServer server(
        "HTTP/1.1 503 Service Unavailable\r\n"
        "Content-Type: text/event-stream\r\nContent-Length: 0\r\n"
        "Connection: close\r\n\r\n");
    chhttp::AsyncClient client("http://127.0.0.1:" +
                               std::to_string(server.port()));
    chhttp::SseClient events(client, "/", {}, {.reconnect = false});
    auto error = events.connect().get();
    CHECK(error.code == chhttp::Error::protocol);
    CHECK(error.message.find("503") != std::string::npos);
  }
}

TEST(sse_rejects_duplicate_connect_and_stops_an_active_stream) {
  chhttp::Server server;
  std::atomic_bool entered{false};
  std::atomic_bool release{false};
  server.get_async(
      "/events", [&](const chhttp::Request &,
                      chhttp::Response &response) -> chhttp::Task<void> {
        response.set_sse([&](chhttp::SseWriter &writer)
                             -> chhttp::Task<void> {
          entered = true;
          while (!release)
            co_await chhttp::sleep_for(std::chrono::milliseconds(1));
          co_await writer.data("done");
        });
        co_return;
      });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(server.port()));
  chhttp::SseClient events(client, "/events", {}, {.reconnect = false});
  auto first = events.connect();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!entered && std::chrono::steady_clock::now() < deadline)
    std::this_thread::yield();
  CHECK(entered && events.running());
  auto duplicate = events.connect().get();
  CHECK(duplicate.code == chhttp::Error::protocol);
  release = true;
  events.stop();
  auto stopped = first.get();
  CHECK(!stopped);
  CHECK(!events.running());
  server.stop();
}

TEST(websocket_clients_reject_invalid_urls_and_subprotocol_tokens) {
  auto wrong_scheme =
      chhttp::AsyncWebSocketClient::connect(fixture().base_url + "/ws").get();
  CHECK(!wrong_scheme && wrong_scheme.error().code == chhttp::Error::invalid_url);
  auto bad_protocol = chhttp::AsyncWebSocketClient::connect(
      "ws://127.0.0.1:" + std::to_string(fixture().server.port()) + "/ws",
      {{"Sec-WebSocket-Protocol", "bad protocol"}}).get();
  CHECK(!bad_protocol &&
        bad_protocol.error().code == chhttp::Error::websocket_handshake);

  const auto bad_version = raw_http_exchange(
      fixture().server.port(),
      "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
      "Connection: Upgrade\r\nSec-WebSocket-Version: 12\r\n"
      "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n");
  CHECK(bad_version.starts_with("HTTP/1.1 404 Not Found\r\n"));
}

TEST(websocket_binary_and_ping_pong_frames_round_trip) {
  chhttp::Server server;
  server.websocket(
      "/ws", [](const chhttp::Request &, chhttp::WebSocket &socket)
                  -> chhttp::Task<void> {
        auto ping = co_await socket.read();
        if (!ping || ping->type != chhttp::WebSocket::MessageType::ping)
          co_return;
        auto message = co_await socket.read();
        if (message &&
            message->type == chhttp::WebSocket::MessageType::binary) {
          const auto *data = reinterpret_cast<const std::byte *>(
              message->data.data());
          co_await socket.send_binary(
              std::span<const std::byte>(data, message->data.size()));
        }
      });
  CHECK(server.start("127.0.0.1", 0));
  auto connected = chhttp::AsyncWebSocketClient::connect(
      "ws://127.0.0.1:" + std::to_string(server.port()) + "/ws").get();
  CHECK(connected);
  (*connected)->ping("probe").get();
  auto pong = (*connected)->read().get();
  CHECK(pong && pong->type == chhttp::WebSocket::MessageType::pong &&
        pong->data == "probe");
  const std::array<std::byte, 5> payload{
      std::byte{0x00}, std::byte{0x7f}, std::byte{0x80}, std::byte{0xff},
      std::byte{0x42}};
  CHECK((*connected)->send_binary(payload).get());
  auto echoed = (*connected)->read().get();
  CHECK(echoed && echoed->type == chhttp::WebSocket::MessageType::binary);
  CHECK(echoed->data == std::string(reinterpret_cast<const char *>(payload.data()),
                                   payload.size()));
  (*connected)->close().get();
  server.stop();
}

TEST(stress_async_high_concurrency) {
  chhttp::ClientOptions options;
  options.connection_pool_size = 64;
  chhttp::AsyncClient client(fixture().base_url, std::move(options));
  constexpr int concurrency = 256;
  constexpr int batches = 4;
  for (int batch = 0; batch != batches; ++batch) {
    std::vector<chhttp::Task<chhttp::ResponseResult>> requests;
    requests.reserve(concurrency);
    for (int index = 0; index != concurrency; ++index)
      requests.push_back(client.get("/async"));
    for (int index = 0; index != concurrency; ++index) {
      auto response = requests[index].get();
      const auto request_number = batch * concurrency + index;
      if (!response)
        throw Failure("Async concurrency request " +
                      std::to_string(request_number) +
                      " failed with error " +
                      std::to_string(static_cast<int>(response.error().code)) +
                      ": " + response.error().message);
      if (response->body != "async")
        throw Failure("Async concurrency request " +
                      std::to_string(request_number) +
                      " returned an invalid body of " +
                      std::to_string(response->body.size()) + " bytes");
    }
  }
}

TEST(stress_multithreaded_sync_request_integrity) {
  constexpr int thread_count = 8;
  constexpr int requests_per_thread = 100;
  std::atomic_bool start{false};
  std::atomic_int failures{0};
  std::vector<std::thread> workers;
  workers.reserve(thread_count);
  for (int worker = 0; worker != thread_count; ++worker) {
    workers.emplace_back([&, worker] {
      chhttp::Client client(fixture().base_url);
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      const auto expected = "hello:worker-" + std::to_string(worker);
      for (int request = 0; request != requests_per_thread; ++request) {
        auto response = client.get("/hello?who=worker-" +
                                   std::to_string(worker));
        if (!response || response->status != 200 || response->body != expected)
          failures.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto &worker : workers) worker.join();
  CHECK(failures.load() == 0);
}

TEST(stress_keep_alive_limit_reconnects_transparently) {
  chhttp::ServerOptions server_options;
  server_options.worker_threads = 2;
  server_options.keep_alive_max_requests = 7;
  chhttp::Server server(std::move(server_options));
  server.get("/port", [](const chhttp::Request &request,
                          chhttp::Response &response) {
    response.set_content(std::to_string(request.remote_port));
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  std::set<std::string> remote_ports;
  for (int index = 0; index != 250; ++index) {
    auto response = client.get("/port");
    CHECK(response && response->status == 200 && !response->body.empty());
    remote_ports.insert(response->body);
  }
  CHECK(remote_ports.size() >= 30);
  CHECK(remote_ports.size() <= 40);
  server.stop();
}

TEST(stress_cancellation_storm_and_client_recovery) {
  chhttp::ClientOptions options;
  options.connection_pool_size = 32;
  chhttp::AsyncClient client(fixture().base_url, std::move(options));
  constexpr int count = 128;
  std::vector<chhttp::Task<chhttp::ResponseResult>> requests;
  requests.reserve(count);
  for (int index = 0; index != count; ++index)
    requests.push_back(client.get("/slow"));
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  client.cancel();
  for (auto &request : requests) {
    auto result = request.get();
    CHECK(!result && result.error().code == chhttp::Error::cancelled);
  }
  auto recovered = client.get("/hello?who=recovered").get();
  CHECK(recovered && recovered->body == "hello:recovered");
}

TEST(stress_server_restart_cycles) {
  for (int cycle = 0; cycle != 30; ++cycle) {
    chhttp::ServerOptions options;
    options.worker_threads = 1;
    chhttp::Server server(std::move(options));
    server.get("/", [cycle](const chhttp::Request &,
                             chhttp::Response &response) {
      response.set_content(std::to_string(cycle));
    });
    CHECK(server.start("127.0.0.1", 0));
    chhttp::Client client("http://127.0.0.1:" +
                          std::to_string(server.port()));
    auto response = client.get("/");
    CHECK(response && response->body == std::to_string(cycle));
    server.stop();
    CHECK(!server.running());
  }
}

TEST(connection_pool_and_active_cancellation) {
  chhttp::Client client(fixture().base_url);
  auto first = client.get("/connection");
  auto second = client.get("/connection");
  auto third = client.get("/connection");
  CHECK(first && second && third);
  CHECK(first->body == second->body && second->body == third->body);

  chhttp::AsyncClient async_client(fixture().base_url);
  auto pending = async_client.get("/slow");
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  async_client.cancel();
  auto cancelled = pending.get();
  CHECK(!cancelled && cancelled.error().code == chhttp::Error::cancelled);

  chhttp::Task<chhttp::ResponseResult> abandoned;
  {
    chhttp::AsyncClient short_lived(fixture().base_url);
    abandoned = short_lived.get("/slow");
  }
  auto safely_cancelled = abandoned.get();
  CHECK(!safely_cancelled &&
        safely_cancelled.error().code == chhttp::Error::cancelled);
}

TEST(graceful_shutdown_drains_inflight_response) {
  chhttp::ServerOptions server_options;
  server_options.shutdown_timeout = std::chrono::seconds(2);
  chhttp::Server server(std::move(server_options));
  std::promise<void> entered_promise;
  auto entered = entered_promise.get_future();
  server.get_async(
      "/drain", [&](const chhttp::Request &,
                     chhttp::Response &response) -> chhttp::Task<void> {
        entered_promise.set_value();
        co_await chhttp::sleep_for(std::chrono::milliseconds(100));
        response.set_content("drained");
      });
  CHECK(server.start("127.0.0.1", 0));

  chhttp::AsyncClient client(
      "http://127.0.0.1:" + std::to_string(server.port()));
  auto response_task = client.get("/drain");
  CHECK(entered.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  const auto before = std::chrono::steady_clock::now();
  server.stop();
  const auto elapsed = std::chrono::steady_clock::now() - before;
  auto response = response_task.get();
  CHECK(response && response->body == "drained");
  CHECK(elapsed >= std::chrono::milliseconds(50));
}

TEST(stress_graceful_shutdown_drains_many_inflight_responses) {
  chhttp::ServerOptions server_options;
  server_options.worker_threads = 2;
  server_options.shutdown_timeout = std::chrono::seconds(3);
  chhttp::Server server(std::move(server_options));
  std::atomic_int entered{0};
  std::atomic_bool release{false};
  server.get_async(
      "/drain", [&](const chhttp::Request &,
                     chhttp::Response &response) -> chhttp::Task<void> {
        entered.fetch_add(1, std::memory_order_release);
        while (!release.load(std::memory_order_acquire))
          co_await chhttp::sleep_for(std::chrono::milliseconds(1));
        response.set_content("drained");
      });
  CHECK(server.start("127.0.0.1", 0));

  chhttp::ClientOptions client_options;
  client_options.connection_pool_size = 32;
  chhttp::AsyncClient client(
      "http://127.0.0.1:" + std::to_string(server.port()),
      std::move(client_options));
  constexpr int count = 64;
  std::vector<chhttp::Task<chhttp::ResponseResult>> requests;
  requests.reserve(count);
  for (int index = 0; index != count; ++index)
    requests.push_back(client.get("/drain"));

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (entered.load(std::memory_order_acquire) != count &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::yield();
  CHECK(entered.load(std::memory_order_acquire) == count);
  auto stopping = std::async(std::launch::async, [&] { server.stop(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  CHECK(stopping.wait_for(std::chrono::milliseconds(0)) !=
        std::future_status::ready);
  release.store(true, std::memory_order_release);
  CHECK(stopping.wait_for(std::chrono::seconds(3)) ==
        std::future_status::ready);
  stopping.get();
  for (auto &request : requests) {
    auto response = request.get();
    CHECK(response && response->body == "drained");
  }
}

TEST(http_proxy_request_format_and_authentication) {
  chhttp::Server proxy;
  proxy.get("^.*$", [](const chhttp::Request &request,
                        chhttp::Response &response) {
    response.set_content(request.target + "|" +
                         request.get_header("Proxy-Authorization"));
  });
  CHECK(proxy.start("127.0.0.1", 0));
  chhttp::ClientOptions options;
  options.proxy.url =
      "http://127.0.0.1:" + std::to_string(proxy.port());
  options.proxy.username = "agent";
  options.proxy.password = "secret";
  chhttp::Client client(fixture().base_url, std::move(options));
  auto response = client.get("/through-proxy?x=1");
  CHECK(response);
  CHECK(response->body.starts_with(fixture().base_url +
                                   "/through-proxy?x=1|Basic "));
  proxy.stop();
}

TEST(websocket_echo_and_subprotocol) {
  auto connected = chhttp::AsyncWebSocketClient::connect(
      "ws://127.0.0.1:" + std::to_string(fixture().server.port()) + "/ws",
      {{"Sec-WebSocket-Protocol", "other, agent.v1"}}).get();
  CHECK(connected);
  CHECK((*connected)->subprotocol() == "agent.v1");
  CHECK(!(*connected)->send_text(std::string("\xC0\xAF", 2)).get());
  CHECK((*connected)->send_text("hi").get());
  auto message = (*connected)->read().get();
  CHECK(message && message->data == "echo:hi");
  (*connected)->close().get();

  auto rejected = chhttp::AsyncWebSocketClient::connect(
      "ws://127.0.0.1:" + std::to_string(fixture().server.port()) +
      "/hello").get();
  CHECK(!rejected &&
        rejected.error().code == chhttp::Error::websocket_handshake);

  std::string fragmented_request =
      "GET /ws HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Upgrade: websocket\r\n"
      "Connection: Upgrade\r\n"
      "Sec-WebSocket-Version: 13\r\n"
      "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
  fragmented_request += masked_websocket_frame(0x01, "hel");
  fragmented_request += masked_websocket_frame(0x89, "p");
  fragmented_request += masked_websocket_frame(0x80, "lo");
  const auto fragmented = raw_http_exchange(fixture().server.port(),
                                             fragmented_request);
  CHECK(fragmented.starts_with("HTTP/1.1 101 Switching Protocols\r\n"));
  CHECK(fragmented.find("echo:hello") != std::string::npos);
}

TEST(stress_websocket_concurrent_large_frames) {
  chhttp::Server server;
  server.websocket(
      "/ws", [](const chhttp::Request &, chhttp::WebSocket &socket)
                  -> chhttp::Task<void> {
        auto message = co_await socket.read();
        if (message) co_await socket.send_text(message->data);
      });
  CHECK(server.start("127.0.0.1", 0));
  const auto url =
      "ws://127.0.0.1:" + std::to_string(server.port()) + "/ws";
  constexpr int count = 24;
  std::vector<chhttp::Task<chhttp::Result<std::shared_ptr<chhttp::WebSocket>>>>
      connections;
  connections.reserve(count);
  for (int index = 0; index != count; ++index)
    connections.push_back(chhttp::AsyncWebSocketClient::connect(url));

  std::vector<std::shared_ptr<chhttp::WebSocket>> sockets;
  sockets.reserve(count);
  for (auto &connection : connections) {
    auto result = connection.get();
    CHECK(result);
    sockets.push_back(std::move(*result));
  }

  std::vector<std::string> payloads;
  std::vector<chhttp::Task<bool>> writes;
  payloads.reserve(count);
  writes.reserve(count);
  for (int index = 0; index != count; ++index) {
    const auto size = index == count - 1 ? std::size_t{128 * 1024}
        : static_cast<std::size_t>(4096 + index);
    payloads.emplace_back(size, static_cast<char>('a' + index % 26));
    writes.push_back(sockets[index]->send_text(payloads.back()));
  }
  for (auto &write : writes) CHECK(write.get());

  std::vector<chhttp::Task<chhttp::Result<chhttp::WebSocket::Message>>> reads;
  reads.reserve(count);
  for (auto &socket : sockets) reads.push_back(socket->read());
  for (int index = 0; index != count; ++index) {
    auto message = reads[index].get();
    CHECK(message && message->data == payloads[index]);
  }
  for (auto &socket : sockets) socket->close().get();
  server.stop();
}

TEST(sse_parser_and_stream) {
  chhttp::AsyncClient client(fixture().base_url);
  chhttp::SseClient events(client, "/sse", {}, {.reconnect = false});
  std::vector<chhttp::SseEvent> received;
  events.on_event("tick", [&](const chhttp::SseEvent &event) {
    received.push_back(event);
  });
  auto error = events.connect().get();
  CHECK(error.code == chhttp::Error::read);
  CHECK(received.size() == 3);
  CHECK(received[0].data == "line1\nline2");
  CHECK(received[2].id == "2");
}

TEST(sse_parser_handles_bytewise_bom_crlf_and_final_event) {
  const std::string body =
      std::string("\xEF\xBB\xBF") +
      "data: first\r\n: ignored comment\r\ndata: second line\r\n"
      "event: custom\r\nid: 7\r\nretry: 25\r\n\r\n"
      "id:\r\ndata: tail";
  RawResponseServer server(
      "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
      "Content-Length: " +
          std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body,
      true);
  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(server.port()));
  chhttp::SseClient events(client, "/", {}, {.reconnect = false});
  std::vector<chhttp::SseEvent> received;
  events.on_message(
      [&](const chhttp::SseEvent &event) { received.push_back(event); });
  auto error = events.connect().get();
  CHECK(error.code == chhttp::Error::read);
  CHECK(received.size() == 2);
  CHECK(received[0].event == "custom");
  CHECK(received[0].data == "first\nsecond line");
  CHECK(received[0].id == "7");
  CHECK(received[0].retry == std::chrono::milliseconds(25));
  CHECK(received[1].data == "tail");
  CHECK(received[1].id.empty());
}

TEST(sse_callback_failures_are_reported_without_escape) {
  chhttp::AsyncClient client(fixture().base_url);
  chhttp::SseClient events(client, "/sse", {}, {.reconnect = false});
  events.on_event("tick", [](const chhttp::SseEvent &) {
    throw std::runtime_error("consumer failure");
  });
  auto error = events.connect().get();
  CHECK(error.code == chhttp::Error::internal);
  CHECK(error.message.find("consumer failure") != std::string::npos);
  CHECK(!events.running());
}

TEST(stress_thousands_of_requests_reuse_one_keep_alive_connection) {
  chhttp::ServerOptions options;
  options.keep_alive_max_requests = 5000;
  chhttp::Server server(std::move(options));
  server.get("/port", [](const chhttp::Request &request,
                          chhttp::Response &response) {
    response.set_content(std::to_string(request.remote_port));
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  std::string first_port;
  for (int index = 0; index != 3000; ++index) {
    auto response = client.get("/port");
    CHECK(response && response->status == 200);
    if (index == 0) first_port = response->body;
    CHECK(response->body == first_port);
  }
  server.stop();
}

TEST(stress_mixed_methods_share_one_async_client) {
  chhttp::ClientOptions options;
  options.connection_pool_size = 64;
  chhttp::AsyncClient client(fixture().base_url, std::move(options));
  constexpr int count = 256;
  std::vector<chhttp::Task<chhttp::ResponseResult>> requests;
  requests.reserve(count);
  for (int index = 0; index != count; ++index) {
    if (index % 2 == 0)
      requests.push_back(client.get("/hello?who=" + std::to_string(index)));
    else
      requests.push_back(client.post("/echo", std::to_string(index),
                                     "text/plain"));
  }
  for (int index = 0; index != count; ++index) {
    auto response = requests[index].get();
    CHECK(response);
    const auto expected = index % 2 == 0
                              ? "hello:" + std::to_string(index)
                              : "POST:" + std::to_string(index);
    CHECK(response->body == expected);
  }
}

TEST(stress_concurrent_large_uploads_preserve_body_integrity) {
  chhttp::Server server;
  server.post("/upload", [](const chhttp::Request &request,
                             chhttp::Response &response) {
    response.set_content(std::to_string(request.body.size()) + "|" +
                         std::string(1, request.body.front()) +
                         std::string(1, request.body.back()));
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::ClientOptions options;
  options.connection_pool_size = 32;
  chhttp::AsyncClient client(
      "http://127.0.0.1:" + std::to_string(server.port()),
      std::move(options));
  constexpr int count = 32;
  constexpr std::size_t payload_size = 256 * 1024;
  std::vector<chhttp::Task<chhttp::ResponseResult>> uploads;
  uploads.reserve(count);
  for (int index = 0; index != count; ++index) {
    const char marker = static_cast<char>('a' + index % 26);
    uploads.push_back(client.post("/upload", std::string(payload_size, marker),
                                  "application/octet-stream"));
  }
  for (int index = 0; index != count; ++index) {
    auto response = uploads[index].get();
    const char marker = static_cast<char>('a' + index % 26);
    CHECK(response && response->body == std::to_string(payload_size) + "|" +
                                          marker + marker);
  }
  server.stop();
}

TEST(stress_concurrent_stream_callbacks_do_not_cross_contaminate) {
  chhttp::AsyncClient client(fixture().base_url);
  constexpr int count = 128;
  std::array<std::string, count> bodies;
  std::vector<chhttp::Task<chhttp::ResponseResult>> requests;
  requests.reserve(count);
  for (int index = 0; index != count; ++index) {
    requests.push_back(client.get(
        "/stream", {},
        {.on_data = [&, index](std::string_view chunk) {
           bodies[index].append(chunk);
           return true;
         }}));
  }
  for (int index = 0; index != count; ++index) {
    auto response = requests[index].get();
    CHECK(response && response->body.empty());
    CHECK(bodies[index] == "onetwo");
  }
}

TEST(stress_individual_cancellation_tokens_isolate_requests) {
  chhttp::AsyncClient client(fixture().base_url);
  constexpr int count = 96;
  std::array<std::shared_ptr<std::atomic_bool>, count> cancellations;
  std::vector<chhttp::Task<chhttp::ResponseResult>> requests;
  requests.reserve(count);
  for (int index = 0; index != count; ++index) {
    cancellations[index] = std::make_shared<std::atomic_bool>(false);
    requests.push_back(client.get(
        "/slow", {}, {.cancellation = cancellations[index]}));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  for (int index = 0; index != count; index += 2)
    cancellations[index]->store(true);
  for (int index = 0; index != count; ++index) {
    auto response = requests[index].get();
    if (index % 2 == 0)
      CHECK(!response && response.error().code == chhttp::Error::cancelled);
    else
      CHECK(response && response->body == "late");
  }
}

TEST(stress_async_client_construction_and_destruction_cancels_safely) {
  for (int iteration = 0; iteration != 80; ++iteration) {
    chhttp::Task<chhttp::ResponseResult> pending;
    {
      chhttp::AsyncClient client(fixture().base_url);
      pending = client.get("/slow");
    }
    auto response = pending.get();
    CHECK(!response && response.error().code == chhttp::Error::cancelled);
  }
}

TEST(stress_multiple_servers_and_clients_run_in_parallel) {
  constexpr int server_count = 12;
  constexpr int requests_per_server = 50;
  std::vector<std::unique_ptr<chhttp::Server>> servers;
  servers.reserve(server_count);
  for (int index = 0; index != server_count; ++index) {
    chhttp::ServerOptions options;
    options.worker_threads = 1;
    auto server = std::make_unique<chhttp::Server>(std::move(options));
    server->get("/", [index](const chhttp::Request &,
                              chhttp::Response &response) {
      response.set_content(std::to_string(index));
    });
    CHECK(server->start("127.0.0.1", 0));
    servers.push_back(std::move(server));
  }
  std::atomic_int failures{0};
  std::vector<std::thread> clients;
  clients.reserve(server_count);
  for (int index = 0; index != server_count; ++index) {
    clients.emplace_back([&, index] {
      chhttp::Client client("http://127.0.0.1:" +
                            std::to_string(servers[index]->port()));
      for (int request = 0; request != requests_per_server; ++request) {
        auto response = client.get("/");
        if (!response || response->body != std::to_string(index)) ++failures;
      }
    });
  }
  for (auto &client : clients) client.join();
  CHECK(failures == 0);
  for (auto &server : servers) server->stop();
}

TEST(stress_many_sse_clients_receive_complete_independent_streams) {
  constexpr int count = 24;
  std::array<int, count> received{};
  std::vector<std::unique_ptr<chhttp::AsyncClient>> clients;
  std::vector<std::unique_ptr<chhttp::SseClient>> streams;
  std::vector<chhttp::Task<chhttp::ErrorInfo>> tasks;
  clients.reserve(count);
  streams.reserve(count);
  tasks.reserve(count);
  for (int index = 0; index != count; ++index) {
    clients.push_back(std::make_unique<chhttp::AsyncClient>(fixture().base_url));
    streams.push_back(std::make_unique<chhttp::SseClient>(
        *clients.back(), "/sse", chhttp::Headers{},
        chhttp::SseClientOptions{.reconnect = false}));
    streams.back()->on_event("tick", [&, index](const chhttp::SseEvent &) {
      ++received[index];
    });
    tasks.push_back(streams.back()->connect());
  }
  for (int index = 0; index != count; ++index) {
    auto error = tasks[index].get();
    CHECK(error.code == chhttp::Error::read);
    CHECK(received[index] == 3);
  }
}

TEST(stress_websocket_connection_churn) {
  chhttp::Server server;
  server.websocket(
      "/ws", [](const chhttp::Request &, chhttp::WebSocket &socket)
                  -> chhttp::Task<void> {
        auto message = co_await socket.read();
        if (message) co_await socket.send_text(message->data);
      });
  CHECK(server.start("127.0.0.1", 0));
  const auto url =
      "ws://127.0.0.1:" + std::to_string(server.port()) + "/ws";
  constexpr int count = 48;
  std::vector<chhttp::Task<chhttp::Result<std::shared_ptr<chhttp::WebSocket>>>>
      connections;
  connections.reserve(count);
  for (int index = 0; index != count; ++index)
    connections.push_back(chhttp::AsyncWebSocketClient::connect(url));
  std::vector<std::shared_ptr<chhttp::WebSocket>> sockets;
  sockets.reserve(count);
  for (auto &connection : connections) {
    auto result = connection.get();
    CHECK(result);
    sockets.push_back(std::move(*result));
  }
  std::vector<chhttp::Task<bool>> writes;
  for (int index = 0; index != count; ++index)
    writes.push_back(sockets[index]->send_text(std::to_string(index)));
  for (auto &write : writes) CHECK(write.get());
  for (int index = 0; index != count; ++index) {
    auto message = sockets[index]->read().get();
    CHECK(message && message->data == std::to_string(index));
    sockets[index]->close().get();
  }
  server.stop();
}

TEST(stress_websocket_many_messages_on_one_connection) {
  constexpr int count = 200;
  chhttp::Server server;
  server.websocket(
      "/ws", [](const chhttp::Request &, chhttp::WebSocket &socket)
                  -> chhttp::Task<void> {
        for (int index = 0; index != count; ++index) {
          auto message = co_await socket.read();
          if (!message) co_return;
          co_await socket.send_text(message->data);
        }
      });
  CHECK(server.start("127.0.0.1", 0));
  auto connected = chhttp::AsyncWebSocketClient::connect(
      "ws://127.0.0.1:" + std::to_string(server.port()) + "/ws").get();
  CHECK(connected);
  for (int index = 0; index != count; ++index) {
    const auto payload = index == count - 1
                             ? std::string(70 * 1024, 'z')
                             : "message-" + std::to_string(index);
    CHECK((*connected)->send_text(payload).get());
    auto echoed = (*connected)->read().get();
    CHECK(echoed && echoed->data == payload);
  }
  (*connected)->close().get();
  server.stop();
}

TEST(stress_malformed_request_flood_remains_available) {
  chhttp::Server server;
  server.get("/health", [](const chhttp::Request &,
                            chhttp::Response &response) {
    response.set_content("healthy");
  });
  CHECK(server.start("127.0.0.1", 0));
  constexpr int thread_count = 8;
  constexpr int requests_per_thread = 25;
  std::atomic_int failures{0};
  std::vector<std::thread> workers;
  for (int thread = 0; thread != thread_count; ++thread) {
    workers.emplace_back([&] {
      for (int index = 0; index != requests_per_thread; ++index) {
        try {
          const auto response = raw_http_exchange(
              server.port(),
              "POST / HTTP/1.1\r\nHost: localhost\r\n"
              "Content-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n"
              "0\r\n\r\n");
          if (!response.starts_with("HTTP/1.1 400 Bad Request\r\n"))
            ++failures;
        } catch (...) {
          ++failures;
        }
      }
    });
  }
  for (auto &worker : workers) worker.join();
  CHECK(failures == 0);
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto health = client.get("/health");
  CHECK(health && health->body == "healthy");
  server.stop();
}

#ifdef CHHTTP_HAS_TLS
struct CertificateFiles {
  std::filesystem::path directory;
  std::filesystem::path certificate;
  std::filesystem::path key;
  CertificateFiles() = default;
  CertificateFiles(const CertificateFiles &) = delete;
  CertificateFiles &operator=(const CertificateFiles &) = delete;
  CertificateFiles(CertificateFiles &&other) noexcept
      : directory(std::exchange(other.directory, {})),
        certificate(std::move(other.certificate)), key(std::move(other.key)) {}
  ~CertificateFiles() {
    std::error_code ignored;
    std::filesystem::remove_all(directory, ignored);
  }
};

CertificateFiles make_certificate() {
  CertificateFiles files;
  files.directory = unique_test_directory("chhttp-tls-test");
  files.certificate = files.directory / "cert.pem";
  files.key = files.directory / "key.pem";
  EVP_PKEY *key = EVP_RSA_gen(2048);
  CHECK(key != nullptr);
  X509 *certificate = X509_new();
  CHECK(certificate != nullptr);
  ASN1_INTEGER_set(X509_get_serialNumber(certificate), 1);
  X509_gmtime_adj(X509_get_notBefore(certificate), 0);
  X509_gmtime_adj(X509_get_notAfter(certificate), 3600);
  X509_set_pubkey(certificate, key);
  auto *name = X509_get_subject_name(certificate);
  X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                             reinterpret_cast<const unsigned char *>("localhost"),
                             -1, -1, 0);
  X509_set_issuer_name(certificate, name);
  X509V3_CTX extension_context{};
  X509V3_set_ctx(&extension_context, certificate, certificate, nullptr, nullptr,
                 0);
  for (const auto &[nid, value] :
       std::vector<std::pair<int, const char *>>{
           {NID_basic_constraints, "critical,CA:TRUE"},
           {NID_key_usage, "critical,digitalSignature,keyEncipherment,keyCertSign"},
           {NID_ext_key_usage, "serverAuth,clientAuth"},
           {NID_subject_alt_name, "DNS:localhost,IP:127.0.0.1"}}) {
    X509_EXTENSION *extension =
        X509V3_EXT_conf_nid(nullptr, &extension_context, nid,
                            const_cast<char *>(value));
    CHECK(extension != nullptr);
    CHECK(X509_add_ext(certificate, extension, -1) == 1);
    X509_EXTENSION_free(extension);
  }
  CHECK(X509_sign(certificate, key, EVP_sha256()) != 0);
  BIO *key_file = BIO_new_file(files.key.string().c_str(), "wb");
  CHECK(key_file != nullptr);
  CHECK(PEM_write_bio_PrivateKey(key_file, key, nullptr, nullptr, 0, nullptr,
                                 nullptr) == 1);
  BIO_free(key_file);
  BIO *cert_file = BIO_new_file(files.certificate.string().c_str(), "wb");
  CHECK(cert_file != nullptr);
  CHECK(PEM_write_bio_X509(cert_file, certificate) == 1);
  BIO_free(cert_file);
  X509_free(certificate);
  EVP_PKEY_free(key);
  return files;
}

TEST(https_client_server) {
  auto certificate = make_certificate();
  chhttp::ServerOptions server_options;
  server_options.tls = chhttp::TlsServerOptions{
      .certificate_file = certificate.certificate,
      .private_key_file = certificate.key};
  chhttp::Server server(std::move(server_options));
  server.get("/secure", [](const chhttp::Request &,
                            chhttp::Response &response) {
    response.set_content("secure");
  });
  server.get("/connection", [](const chhttp::Request &request,
                                chhttp::Response &response) {
    response.set_content(std::to_string(request.remote_port));
  });
  server.websocket(
      "/ws", [](const chhttp::Request &, chhttp::WebSocket &socket)
                  -> chhttp::Task<void> {
        auto message = co_await socket.read();
        if (message) co_await socket.send_text("secure:" + message->data);
      });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::ClientOptions client_options;
  client_options.tls.verify_peer = false;
  chhttp::Client client("https://127.0.0.1:" + std::to_string(server.port()),
                        std::move(client_options));
  auto response = client.get("/secure");
  CHECK(response && response->body == "secure");
  auto first_connection = client.get("/connection");
  auto second_connection = client.get("/connection");
  CHECK(first_connection && second_connection);
  CHECK(first_connection->body == second_connection->body);

  chhttp::ClientOptions verified_options;
  verified_options.tls.use_system_certificates = false;
  verified_options.tls.ca_file = certificate.certificate;
  chhttp::Client verified_client(
      "https://localhost:" + std::to_string(server.port()),
      std::move(verified_options));
  auto verified = verified_client.get("/secure");
  CHECK(verified && verified->body == "secure");

  chhttp::ClientOptions verified_ip_options;
  verified_ip_options.tls.use_system_certificates = false;
  verified_ip_options.tls.ca_file = certificate.certificate;
  chhttp::Client verified_ip_client(
      "https://127.0.0.1:" + std::to_string(server.port()),
      std::move(verified_ip_options));
  auto verified_ip = verified_ip_client.get("/secure");
  CHECK(verified_ip && verified_ip->body == "secure");

  chhttp::Client untrusted_client("https://localhost:" +
                                  std::to_string(server.port()));
  auto untrusted = untrusted_client.get("/secure");
  CHECK(!untrusted &&
        untrusted.error().code == chhttp::Error::tls_verification);

  chhttp::ClientOptions websocket_options;
  websocket_options.tls.verify_peer = false;
  auto connected = chhttp::AsyncWebSocketClient::connect(
      "wss://127.0.0.1:" + std::to_string(server.port()) + "/ws", {},
      std::move(websocket_options)).get();
  CHECK(connected);
  CHECK((*connected)->send_text("agent").get());
  auto message = (*connected)->read().get();
  CHECK(message && message->data == "secure:agent");
  (*connected)->close().get();
  server.stop();
}

TEST(stress_https_async_concurrency_and_large_payloads) {
  auto certificate = make_certificate();
  chhttp::ServerOptions server_options;
  server_options.worker_threads = 4;
  server_options.tls = chhttp::TlsServerOptions{
      .certificate_file = certificate.certificate,
      .private_key_file = certificate.key};
  chhttp::Server server(std::move(server_options));
  server.get_async(
      "/secure", [](const chhttp::Request &request,
                     chhttp::Response &response) -> chhttp::Task<void> {
        co_await chhttp::sleep_for(std::chrono::milliseconds(1));
        response.set_content(request.get_param("id") + ":" +
                             std::string(32 * 1024, 's'));
      });
  CHECK(server.start("127.0.0.1", 0));

  chhttp::ClientOptions client_options;
  client_options.connection_pool_size = 32;
  client_options.tls.verify_peer = false;
  chhttp::AsyncClient client(
      "https://127.0.0.1:" + std::to_string(server.port()),
      std::move(client_options));
  constexpr int count = 96;
  std::vector<chhttp::Task<chhttp::ResponseResult>> requests;
  requests.reserve(count);
  for (int index = 0; index != count; ++index)
    requests.push_back(client.get("/secure?id=" + std::to_string(index)));
  for (int index = 0; index != count; ++index) {
    auto response = requests[index].get();
    CHECK(response && response->status == 200);
    CHECK(response->body.starts_with(std::to_string(index) + ":"));
    CHECK(response->body.size() ==
          std::to_string(index).size() + 1 + 32 * 1024);
  }
  server.stop();
}

TEST(stress_tls_client_context_and_handshake_churn) {
  auto certificate = make_certificate();
  chhttp::ServerOptions server_options;
  server_options.tls = chhttp::TlsServerOptions{
      .certificate_file = certificate.certificate,
      .private_key_file = certificate.key};
  chhttp::Server server(std::move(server_options));
  server.get("/secure", [](const chhttp::Request &,
                            chhttp::Response &response) {
    response.set_content("secure");
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto url =
      "https://127.0.0.1:" + std::to_string(server.port());
  constexpr int thread_count = 8;
  constexpr int clients_per_thread = 8;
  std::atomic_int failures{0};
  std::vector<std::thread> workers;
  workers.reserve(thread_count);
  for (int thread = 0; thread != thread_count; ++thread) {
    workers.emplace_back([&] {
      for (int index = 0; index != clients_per_thread; ++index) {
        chhttp::ClientOptions options;
        options.tls.verify_peer = false;
        chhttp::Client client(url, std::move(options));
        auto response = client.get("/secure");
        if (!response || response->body != "secure") ++failures;
      }
    });
  }
  for (auto &worker : workers) worker.join();
  CHECK(failures == 0);
  server.stop();
}

TEST(mtls_requires_client_certificate) {
  auto certificate = make_certificate();
  chhttp::ServerOptions server_options;
  server_options.tls = chhttp::TlsServerOptions{
      .certificate_file = certificate.certificate,
      .private_key_file = certificate.key,
      .client_ca_file = certificate.certificate,
      .require_client_certificate = true};
  chhttp::Server server(std::move(server_options));
  server.get("/mtls", [](const chhttp::Request &,
                          chhttp::Response &response) {
    response.set_content("mutual");
  });
  CHECK(server.start("127.0.0.1", 0));

  chhttp::ClientOptions anonymous_options;
  anonymous_options.tls.verify_peer = false;
  anonymous_options.connect_timeout = std::chrono::seconds(2);
  chhttp::Client anonymous(
      "https://127.0.0.1:" + std::to_string(server.port()),
      std::move(anonymous_options));
  auto rejected = anonymous.get("/mtls");
  CHECK(!rejected);

  chhttp::ClientOptions authenticated_options;
  authenticated_options.tls.verify_peer = false;
  authenticated_options.tls.certificate_file = certificate.certificate;
  authenticated_options.tls.private_key_file = certificate.key;
  chhttp::Client authenticated(
      "https://127.0.0.1:" + std::to_string(server.port()),
      std::move(authenticated_options));
  auto accepted = authenticated.get("/mtls");
  CHECK(accepted && accepted->body == "mutual");
  server.stop();
}
#endif

} // namespace

int main(int argc, char **argv) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  int failures = 0;
  std::size_t executed = 0;
  const std::string_view selector = argc > 1 ? argv[1] : "";
  for (const auto &[name, function] : tests()) {
    const std::string_view test_name = name;
    if (selector == "--functional" && test_name.starts_with("stress_"))
      continue;
    if (selector == "--stress" && !test_name.starts_with("stress_")) continue;
    if (!selector.empty() && selector != "--functional" &&
        selector != "--stress" && test_name.find(selector) == std::string_view::npos)
      continue;
    ++executed;
    const auto started = std::chrono::steady_clock::now();
    try {
      function();
      const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - started);
      std::cout << "[PASS] " << name << " (" << elapsed.count() << " ms)\n";
    } catch (const std::exception &exception) {
      ++failures;
      std::cerr << "[FAIL] " << name << ": " << exception.what() << '\n';
    } catch (...) {
      ++failures;
      std::cerr << "[FAIL] " << name << ": unknown exception\n";
    }
  }
  fixture().server.stop();
  std::cout << (executed - failures) << "/" << executed
            << " tests passed\n";
  return failures == 0 && executed != 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
