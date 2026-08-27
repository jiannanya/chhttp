#include <chhttp/chhttp.hpp>

#include <iostream>
#include <string>

int main(int argc, char **argv) {
  const auto port = argc > 1
                        ? static_cast<std::uint16_t>(std::stoi(argv[1]))
                        : static_cast<std::uint16_t>(8080);

  chhttp::ServerOptions options;
  // Synchronous route handlers run here; async handlers remain on the I/O loop.
  options.worker_threads = 4;
  options.max_body_size = 8 * 1024 * 1024;
  options.shutdown_timeout = std::chrono::seconds(5);
  chhttp::Server server(std::move(options));

  // Curly-brace parameters are available through Request::path_params.
  server.get("/v1/agents/{id}", [](const chhttp::Request &request,
                                    chhttp::Response &response) {
    response.set_content(
        R"({"id":")" + request.path_params.at("id") + R"(","ready":true})",
        "application/json");
  });
  server.post("/v1/agents", [](const chhttp::Request &request,
                                chhttp::Response &response) {
    // Set status and Location explicitly when creating a resource.
    response.status = 201;
    response.headers.set("Location", "/v1/agents/generated");
    response.set_content(request.body, "application/json");
  });
  server.del("/v1/agents/{id}", [](const chhttp::Request &,
                                    chhttp::Response &response) {
    // A 204 response is serialized without Content-Length or body bytes.
    response.status = 204;
  });
  server.set_error_handler([](const chhttp::Request &,
                               chhttp::Response &response) {
    // Error handlers can standardize the service's media type and error schema.
    response.set_content(R"({"error":"not found"})", "application/json");
  });

  if (!server.start("0.0.0.0", port)) {
    std::cerr << "unable to listen on port " << port << '\n';
    return 1;
  }
  std::cout << "REST server listening on http://127.0.0.1:"
            << server.port() << "\nPress Enter to stop.\n";
  // A production application would call stop() from its signal/service handler.
  std::string ignored;
  std::getline(std::cin, ignored);
  server.stop();
}
