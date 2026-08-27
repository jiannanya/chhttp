#include <chhttp/chhttp.hpp>

#include <iostream>
#include <string>

int main() {
  chhttp::Server provider;
  provider.post_async(
      "/agent", [](const chhttp::Request &request,
                     chhttp::Response &response) -> chhttp::Task<void> {
        response.set_sse([has_result = request.body.find("tool_result") !=
                                      std::string::npos](chhttp::SseWriter &events)
                             -> chhttp::Task<void> {
          if (!has_result) {
            // A real provider may split this JSON across multiple SSE events;
            // the agent owns protocol-specific tool-delta assembly.
            co_await events.send({.data = R"({"name":"add","a":20,"b":22})",
                                  .event = "tool_call"});
          } else {
            co_await events.send({.data = "The answer is 42.",
                                  .event = "content"});
          }
        });
        co_return;
      });
  if (!provider.start("127.0.0.1", 0)) return 1;

  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(provider.port()));
  const auto run_turn = [&](std::string body, std::string &tool_call,
                            std::string &answer) {
    chhttp::Request request;
    request.method = "POST";
    request.target = "/agent";
    request.headers.set("Content-Type", "application/json");
    request.body = std::move(body);
    chhttp::SseClient stream(client, std::move(request), {.reconnect = false});
    stream.on_event("tool_call", [&](const chhttp::SseEvent &event) {
      tool_call = event.data;
    });
    stream.on_event("content", [&](const chhttp::SseEvent &event) {
      answer.append(event.data);
    });
    return stream.connect().get();
  };

  std::string tool_call;
  std::string answer;
  auto first = run_turn(R"({"message":"What is 20 + 22?","stream":true})",
                        tool_call, answer);
  if (first.code != chhttp::Error::read || tool_call.empty()) return 2;

  // Tool execution happens outside the I/O callback. Only the completed,
  // validated result is sent into the next model turn.
  const int tool_result = 20 + 22;
  auto second = run_turn(
      std::string(R"({"tool_result":)") + std::to_string(tool_result) + "}",
      tool_call, answer);
  provider.stop();
  if (second.code != chhttp::Error::read || answer != "The answer is 42.")
    return 3;
  std::cout << answer << '\n';
}
