#include <chhttp/chhttp.hpp>

#include <atomic>
#include <iostream>

int main() {
  chhttp::Server server;
  std::atomic_int logged{0};

  server.set_pre_routing_handler(
      [](const chhttp::Request &request, chhttp::Response &response) {
        // Returning true short-circuits route dispatch.
        if (request.path.starts_with("/private") &&
            request.get_header("X-API-Key") != "secret") {
          response.status = 401;
          response.set_content("missing or invalid API key");
          return true;
        }
        return false;
      });
  server.set_post_routing_handler(
      [](const chhttp::Request &, chhttp::Response &response) {
        // Post-routing middleware can attach headers to successful routed responses.
        response.headers.set("X-Service", "agent-gateway");
        return false;
      });
  server.set_error_handler([](const chhttp::Request &request,
                               chhttp::Response &response) {
    // The status is already 404; customize only the representation here.
    response.set_content("no route for " + request.path);
  });
  server.set_exception_handler(
      [](const chhttp::Request &, chhttp::Response &response,
         std::exception_ptr) {
        // Convert handler exceptions into a stable response at the server boundary.
        response.status = 500;
        response.set_content("handler failed safely");
      });
  server.set_logger([&](const chhttp::Request &request,
                        const chhttp::Response &response) {
    // Logging runs after serialization decisions and must not throw.
    ++logged;
    std::cout << request.method << " " << request.path << " -> "
              << response.status << '\n';
  });

  server.get("/private/status", [](const chhttp::Request &,
                                    chhttp::Response &response) {
    response.set_content("authorized");
  });
  server.get("/throws", [](const chhttp::Request &, chhttp::Response &) {
    throw std::runtime_error("demonstration");
  });
  if (!server.start("127.0.0.1", 0)) return 1;

  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  // Exercise the rejected, accepted, and exceptional paths in one run.
  auto denied = client.get("/private/status");
  auto allowed =
      client.get("/private/status", {{"X-API-Key", "secret"}});
  auto failed = client.get("/throws");
  server.stop();

  if (!denied || denied->status != 401 || !allowed ||
      allowed->body != "authorized" || !failed || failed->status != 500)
    return 2;
  std::cout << "logged responses=" << logged << '\n';
}
