#include <chhttp/chhttp.hpp>

#include <atomic>
#include <iostream>
#include <string>

int main() {
  chhttp::Server provider;
  std::atomic_int attempts{0};
  provider.post_async(
      "/retryable", [&](const chhttp::Request &,
                          chhttp::Response &response) -> chhttp::Task<void> {
        if (attempts.fetch_add(1) == 0) {
          response.status = 503;
          response.set_content("temporarily unavailable");
          co_return;
        }
        response.set_sse([](chhttp::SseWriter &events)
                             -> chhttp::Task<void> {
          co_await events.send({.data = "recovered", .event = "content"});
        });
        co_return;
      });
  provider.post_async(
      "/interrupted", [](const chhttp::Request &,
                           chhttp::Response &response) -> chhttp::Task<void> {
        response.set_sse([](chhttp::SseWriter &events)
                             -> chhttp::Task<void> {
          co_await events.send({.data = "visible-token",
                                .event = "content"});
          // Finite close simulates a provider interruption after visible output.
        });
        co_return;
      });
  if (!provider.start("127.0.0.1", 0)) return 1;
  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(provider.port()));

  const auto attempt = [&](std::string target, std::string &visible) {
    chhttp::Request request;
    request.method = "POST";
    request.target = std::move(target);
    request.body = R"({"stream":true})";
    chhttp::SseClient stream(client, std::move(request), {.reconnect = false});
    stream.on_event("content", [&](const chhttp::SseEvent &event) {
      visible.append(event.data);
    });
    return stream.connect().get();
  };

  std::string recovered;
  auto first = attempt("/retryable", recovered);
  // Retrying is presentation-safe because no content was committed yet.
  if (first.code == chhttp::Error::protocol && recovered.empty())
    first = attempt("/retryable", recovered);
  if (first.code != chhttp::Error::read || recovered != "recovered") return 2;

  std::string partial;
  const auto interrupted = attempt("/interrupted", partial);
  if (interrupted.code == chhttp::Error::read && !partial.empty()) {
    // Do not append a fallback provider's answer to this attempt. Persist an
    // explicit interruption event and let orchestration open a new attempt.
    std::cout << "model.stream-interrupted after: " << partial << '\n';
  }
  provider.stop();
  return partial == "visible-token" ? 0 : 3;
}
