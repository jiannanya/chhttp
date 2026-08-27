#include <chhttp/chhttp.hpp>

#include <iostream>
#include <vector>

int main() {
  chhttp::Server server;
  server.get_async(
      "/events", [](const chhttp::Request &,
                     chhttp::Response &response) -> chhttp::Task<void> {
        response.set_sse([](chhttp::SseWriter &events)
                             -> chhttp::Task<void> {
          // Comments are commonly used as keep-alives through proxies.
          if (!co_await events.comment("stream opened")) co_return;
          for (int index = 0; index != 3; ++index) {
            if (!co_await events.send(
                    {.data = "line one\nline two",
                     .event = "delta",
                     .id = std::to_string(index),
                     .retry = std::chrono::milliseconds(1000)}))
              co_return;
          }
        });
        co_return;
      });
  if (!server.start("127.0.0.1", 0)) return 1;

  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(server.port()));
  // Disable reconnect because this finite demonstration intentionally ends.
  chhttp::SseClient events(client, "/events", {}, {.reconnect = false});
  std::vector<chhttp::SseEvent> received;
  events.on_event("delta", [&](const chhttp::SseEvent &event) {
    received.push_back(event);
    std::cout << "event id=" << event.id << " data=" << event.data << '\n';
  });
  events.on_error([](const chhttp::ErrorInfo &error) {
    std::cout << "stream ended: " << error.message << '\n';
  });

  const auto final_error = events.connect().get();
  // A finite HTTP event stream ends with Error::read; production SSE usually reconnects.
  const bool expected_end = final_error.code == chhttp::Error::read;
  server.stop();
  return expected_end && received.size() == 3 ? 0 : 2;
}
