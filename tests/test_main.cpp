#include <chhttp/chhttp.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_future.hpp>

#ifdef CHHTTP_HAS_TLS
#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#endif

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

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
    root = std::filesystem::temp_directory_path() / "chhttp-tests-static";
    std::filesystem::create_directories(root);
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
                     chhttp::Response &response) -> chhttp::asio::awaitable<void> {
          chhttp::asio::steady_timer timer(
              co_await chhttp::asio::this_coro::executor);
          timer.expires_after(std::chrono::milliseconds(250));
          co_await timer.async_wait(chhttp::asio::use_awaitable);
          response.set_content("late");
        });
    server.get("/fail", [](const chhttp::Request &, chhttp::Response &) {
      throw std::runtime_error("boom");
    });
    server.get_async(
        "/async", [](const chhttp::Request &,
                      chhttp::Response &response) -> chhttp::asio::awaitable<void> {
          chhttp::asio::steady_timer timer(co_await chhttp::asio::this_coro::executor);
          timer.expires_after(std::chrono::milliseconds(2));
          co_await timer.async_wait(chhttp::asio::use_awaitable);
          response.set_content("async");
        });
    server.get_async(
        "/stream", [](const chhttp::Request &,
                       chhttp::Response &response) -> chhttp::asio::awaitable<void> {
          response.set_stream(
              "text/plain", [](chhttp::StreamWriter &writer)
                                -> chhttp::asio::awaitable<void> {
                CHECK(co_await writer.write("one"));
                CHECK(co_await writer.write("two"));
              });
          co_return;
        });
    server.get_async(
        "/sse", [](const chhttp::Request &,
                    chhttp::Response &response) -> chhttp::asio::awaitable<void> {
          response.set_sse([](chhttp::SseWriter &writer)
                               -> chhttp::asio::awaitable<void> {
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
            -> chhttp::asio::awaitable<void> {
          auto message = co_await socket.read();
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
}

TEST(multipart_round_trip) {
  chhttp::MultipartForm input{
      {.name = "prompt", .content = "hello"},
      {.name = "file", .filename = "a.txt", .content_type = "text/plain",
       .content = std::string("a\0b", 3)}};
  auto [content_type, body] = chhttp::make_multipart(input, "boundary42");
  auto parsed = chhttp::parse_multipart(body, content_type);
  CHECK(parsed && parsed->size() == 2);
  CHECK((*parsed)[1].content == std::string("a\0b", 3));
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
  auto invalid = client.get("/static/asset.txt", {{"Range", "bytes=100-200"}});
  CHECK(invalid && invalid->status == 416);
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

TEST(async_high_concurrency) {
  chhttp::asio::io_context io;
  chhttp::AsyncClient client(io.get_executor(), fixture().base_url);
  constexpr int count = 200;
  std::vector<std::future<chhttp::ResponseResult>> futures;
  futures.reserve(count);
  for (int index = 0; index != count; ++index) {
    futures.push_back(chhttp::asio::co_spawn(
        io, client.get("/async"), chhttp::asio::use_future));
  }
  std::vector<std::thread> workers;
  for (int index = 0; index != 4; ++index)
    workers.emplace_back([&] { io.run(); });
  for (auto &future : futures) {
    auto response = future.get();
    CHECK(response && response->body == "async");
  }
  for (auto &worker : workers) worker.join();
}

TEST(connection_pool_and_active_cancellation) {
  chhttp::Client client(fixture().base_url);
  auto first = client.get("/connection");
  auto second = client.get("/connection");
  auto third = client.get("/connection");
  CHECK(first && second && third);
  CHECK(first->body == second->body && second->body == third->body);

  chhttp::asio::io_context io;
  chhttp::AsyncClient async_client(io.get_executor(), fixture().base_url);
  auto future = chhttp::asio::co_spawn(io, async_client.get("/slow"),
                                       chhttp::asio::use_future);
  std::thread worker([&] { io.run(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  async_client.cancel();
  auto cancelled = future.get();
  CHECK(!cancelled && cancelled.error().code == chhttp::Error::cancelled);
  worker.join();
}

TEST(graceful_shutdown_drains_inflight_response) {
  chhttp::ServerOptions server_options;
  server_options.shutdown_timeout = std::chrono::seconds(2);
  chhttp::Server server(std::move(server_options));
  std::promise<void> entered_promise;
  auto entered = entered_promise.get_future();
  server.get_async(
      "/drain", [&](const chhttp::Request &,
                     chhttp::Response &response) -> chhttp::asio::awaitable<void> {
        entered_promise.set_value();
        chhttp::asio::steady_timer timer(
            co_await chhttp::asio::this_coro::executor);
        timer.expires_after(std::chrono::milliseconds(100));
        co_await timer.async_wait(chhttp::asio::use_awaitable);
        response.set_content("drained");
      });
  CHECK(server.start("127.0.0.1", 0));

  chhttp::asio::io_context io;
  chhttp::AsyncClient client(
      io.get_executor(),
      "http://127.0.0.1:" + std::to_string(server.port()));
  auto response_future = chhttp::asio::co_spawn(
      io, client.get("/drain"), chhttp::asio::use_future);
  std::thread worker([&] { io.run(); });
  CHECK(entered.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  const auto before = std::chrono::steady_clock::now();
  server.stop();
  const auto elapsed = std::chrono::steady_clock::now() - before;
  auto response = response_future.get();
  CHECK(response && response->body == "drained");
  CHECK(elapsed >= std::chrono::milliseconds(50));
  worker.join();
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
  chhttp::asio::io_context io;
  auto result = chhttp::asio::co_spawn(
      io,
      [&]() -> chhttp::asio::awaitable<void> {
        auto connected = co_await chhttp::AsyncWebSocketClient::connect(
            co_await chhttp::asio::this_coro::executor,
            "ws://127.0.0.1:" + std::to_string(fixture().server.port()) + "/ws",
            {{"Sec-WebSocket-Protocol", "other, agent.v1"}});
        CHECK(connected);
        CHECK((*connected)->subprotocol() == "agent.v1");
        CHECK(co_await (*connected)->send_text("hi"));
        auto message = co_await (*connected)->read();
        CHECK(message && message->data == "echo:hi");
        co_await (*connected)->close();
      },
      chhttp::asio::use_future);
  io.run();
  result.get();
}

TEST(sse_parser_and_stream) {
  chhttp::asio::io_context io;
  chhttp::AsyncClient client(io.get_executor(), fixture().base_url);
  chhttp::SseClient events(client, "/sse", {}, {.reconnect = false});
  std::vector<chhttp::SseEvent> received;
  events.on_event("tick", [&](const chhttp::SseEvent &event) {
    received.push_back(event);
  });
  auto future = chhttp::asio::co_spawn(io, events.connect(),
                                       chhttp::asio::use_future);
  io.run();
  auto error = future.get();
  CHECK(error.code == chhttp::Error::read);
  CHECK(received.size() == 3);
  CHECK(received[0].data == "line1\nline2");
  CHECK(received[2].id == "2");
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
  files.directory = std::filesystem::temp_directory_path() / "chhttp-tls-test";
  std::filesystem::create_directories(files.directory);
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
                  -> chhttp::asio::awaitable<void> {
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

  chhttp::Client untrusted_client("https://localhost:" +
                                  std::to_string(server.port()));
  auto untrusted = untrusted_client.get("/secure");
  CHECK(!untrusted &&
        untrusted.error().code == chhttp::Error::tls_verification);

  chhttp::asio::io_context websocket_io;
  auto websocket_future = chhttp::asio::co_spawn(
      websocket_io,
      [&]() -> chhttp::asio::awaitable<void> {
        chhttp::ClientOptions websocket_options;
        websocket_options.tls.verify_peer = false;
        auto connected = co_await chhttp::AsyncWebSocketClient::connect(
            co_await chhttp::asio::this_coro::executor,
            "wss://127.0.0.1:" + std::to_string(server.port()) + "/ws", {},
            std::move(websocket_options));
        CHECK(connected);
        CHECK(co_await (*connected)->send_text("agent"));
        auto message = co_await (*connected)->read();
        CHECK(message && message->data == "secure:agent");
        co_await (*connected)->close();
      },
      chhttp::asio::use_future);
  websocket_io.run();
  websocket_future.get();
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

int main() {
  int failures = 0;
  for (const auto &[name, function] : tests()) {
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
  std::cout << (tests().size() - failures) << "/" << tests().size()
            << " tests passed\n";
  return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
