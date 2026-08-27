#include <chhttp/chhttp.hpp>

#include <iostream>

int main() {
  // start() is non-blocking. The Server owns its libuv loop and worker pool.
  chhttp::Server server;

  // Named path parameters are percent-decoded before the handler sees them.
  server.get("/hello/{name}", [](const chhttp::Request &request,
                                  chhttp::Response &response) {
    response.set_content("Hello, " + request.path_params.at("name") + "!");
  });

  // Synchronous handlers run on the configurable server worker pool.
  server.post("/echo", [](const chhttp::Request &request,
                           chhttp::Response &response) {
    response.set_content(request.body, request.get_header("Content-Type"));
  });

  // Async handlers may await timers or I/O without blocking a worker thread.
  server.get_async(
      "/events", [](const chhttp::Request &,
                     chhttp::Response &response) -> chhttp::Task<void> {
        // set_sse() selects event-stream headers and chunked HTTP/1.1 output.
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

  // Port zero asks the operating system for an unused ephemeral port.
  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto response = client.get("/hello/agent");
  if (response) {
    std::cout << response->body << '\n';
  } else {
    // Result<T> carries the library error category plus diagnostic details.
    std::cerr << response.error().message << '\n';
  }

  // stop() closes the listener and drains in-flight handlers up to the timeout.
  server.stop();
}
