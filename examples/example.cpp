#include <chhttp/chhttp.hpp>

#include <iostream>

int main() {
  chhttp::Server server;
  server.get("/hello/{name}", [](const chhttp::Request &request,
                                  chhttp::Response &response) {
    response.set_content("Hello, " + request.path_params.at("name") + "!");
  });
  server.post("/echo", [](const chhttp::Request &request,
                           chhttp::Response &response) {
    response.set_content(request.body, request.get_header("Content-Type"));
  });
  server.get_async(
      "/events", [](const chhttp::Request &,
                     chhttp::Response &response) -> chhttp::Task<void> {
        response.set_sse([](chhttp::SseWriter &events)
                             -> chhttp::Task<void> {
          for (int index = 0; index < 3; ++index) {
            if (!co_await events.send({.data = "event " + std::to_string(index),
                                      .event = "message",
                                      .id = std::to_string(index)}))
              break;
          }
        });
        co_return;
      });

  if (!server.start("127.0.0.1", 0)) {
    std::cerr << "Unable to start server\n";
    return 1;
  }

  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto response = client.get("/hello/agent");
  if (response) std::cout << response->body << '\n';
  else std::cerr << response.error().message << '\n';

  server.stop();
}
