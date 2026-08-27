#include <chhttp/chhttp.hpp>

#include <iostream>

int main() {
  // A local server keeps this example deterministic and safe to run offline.
  chhttp::Server server;
  server.get("/search", [](const chhttp::Request &request,
                            chhttp::Response &response) {
    response.set_content("query=" + request.get_param("q", "missing"),
                         "text/plain");
    // add() preserves duplicates; use set() when only one value is allowed.
    response.headers.add("Set-Cookie", "session=one");
    response.headers.add("Set-Cookie", "theme=dark");
  });
  server.post("/documents", [](const chhttp::Request &request,
                                chhttp::Response &response) {
    response.status = 201;
    response.set_content(request.body, request.get_header("Content-Type"));
  });
  if (!server.start("127.0.0.1", 0)) return 1;

  chhttp::ClientOptions options;
  // Default headers are added only when a request does not override them.
  options.default_headers.set("User-Agent", "chhttp-sync-example/1.0");
  options.connect_timeout = std::chrono::seconds(2);
  options.read_timeout = std::chrono::seconds(5);
  chhttp::Client client("http://127.0.0.1:" +
                            std::to_string(server.port()),
                        std::move(options));

  // make_query() applies application/x-www-form-urlencoded escaping.
  const auto query = chhttp::make_query({{"q", "C++ agents"}});
  auto search = client.get("/search?" + query,
                           {{"X-Request-ID", "search-1"}});
  if (!search) {
    std::cerr << "GET failed: " << search.error().message << '\n';
    server.stop();
    return 2;
  }
  std::cout << search->body << '\n';
  std::cout << "cookies=" << search->headers.get_all("Set-Cookie").size()
            << '\n';

  auto created = client.post("/documents", R"({"name":"agent"})",
                             "application/json");
  if (!created || created->status != 201) {
    std::cerr << "POST failed\n";
    server.stop();
    return 3;
  }
  std::cout << created->body << '\n';
  server.stop();
}
