#include <chhttp/chhttp.hpp>

#include <iostream>
#include <string>
#include <vector>

int main() {
  // This local provider implements the same POST JSON -> SSE shape used by
  // OpenAI-compatible chat completion APIs.
  chhttp::Server provider;
  provider.post_async(
      "/api/paas/v4/chat/completions",
      [](const chhttp::Request &request,
         chhttp::Response &response) -> chhttp::Task<void> {
        if (request.get_header("Authorization") != "Bearer demo-key" ||
            request.get_header("Content-Type") != "application/json" ||
            request.body.find("\"stream\":true") == std::string::npos) {
          response.status = 400;
          response.set_content(R"({"error":"invalid request"})",
                               "application/json");
          co_return;
        }
        response.set_sse([](chhttp::SseWriter &events)
                             -> chhttp::Task<void> {
          for (const auto *token : {"Hello", ", ", "agent", "!"}) {
            if (!co_await events.send({
                    .data = std::string(R"({"delta":")") + token + "\"}",
                    .event = "delta"}))
              co_return;
            co_await chhttp::sleep_for(std::chrono::milliseconds(5));
          }
          co_await events.data("[DONE]");
        });
        co_return;
      });
  if (!provider.start("127.0.0.1", 0)) return 1;

  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(provider.port()));
  chhttp::Request request;
  request.method = "POST";
  request.target = "/api/paas/v4/chat/completions";
  request.headers.set("Content-Type", "application/json");
  request.headers.set("Authorization", "Bearer demo-key");
  request.body = R"({"model":"glm-demo","messages":[{"role":"user","content":"Hello"}],"stream":true})";

  // The Request overload is method-agnostic. Model POST streams should not use
  // EventSource-style automatic reconnect because a replay can duplicate output.
  chhttp::SseClient stream(client, std::move(request), {.reconnect = false});
  std::vector<std::string> frames;
  stream.on_open([](const chhttp::ResponseHead &head) {
    std::cout << "HTTP " << head.status << " "
              << head.headers.get("Content-Type") << '\n';
  });
  stream.on_event("delta", [&](const chhttp::SseEvent &event) {
    frames.push_back(event.data);
    std::cout << event.data << '\n';
  });
  stream.on_message([](const chhttp::SseEvent &event) {
    if (event.data == "[DONE]") std::cout << "stream complete\n";
  });

  // A finite SSE response ends with Error::read; long-lived EventSource clients
  // normally enable reconnect instead.
  const auto ended = stream.connect().get();
  provider.stop();
  return ended.code == chhttp::Error::read && frames.size() == 4 ? 0 : 2;
}
