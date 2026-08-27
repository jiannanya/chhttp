#include <chhttp/chhttp.hpp>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

int main() {
  chhttp::Server provider;
  provider.post_async(
      "/agents/{id}", [](const chhttp::Request &request,
                          chhttp::Response &response) -> chhttp::Task<void> {
        const auto id = request.path_params.at("id");
        response.set_sse([id](chhttp::SseWriter &events)
                             -> chhttp::Task<void> {
          for (int token = 0; token != 3; ++token) {
            co_await events.send({.data = id + ":" + std::to_string(token),
                                  .event = "delta"});
            co_await chhttp::sleep_for(std::chrono::milliseconds(2));
          }
        });
        co_return;
      });
  if (!provider.start("127.0.0.1", 0)) return 1;

  chhttp::ClientOptions options;
  options.max_connections_per_origin = 8;
  options.connection_pool_size = 8;
  chhttp::AsyncClient client("http://127.0.0.1:" +
                                 std::to_string(provider.port()),
                             std::move(options));

  constexpr int agent_count = 24;
  std::vector<std::unique_ptr<chhttp::SseClient>> streams;
  std::vector<chhttp::Task<chhttp::ErrorInfo>> tasks;
  std::vector<std::string> outputs(agent_count);
  for (int agent = 0; agent != agent_count; ++agent) {
    chhttp::Request request;
    request.method = "POST";
    request.target = "/agents/" + std::to_string(agent);
    request.body = R"({"stream":true})";
    streams.push_back(std::make_unique<chhttp::SseClient>(
        client, std::move(request), chhttp::SseClientOptions{.reconnect = false}));
    streams.back()->on_event("delta", [&, agent](const chhttp::SseEvent &event) {
      if (!outputs[agent].empty()) outputs[agent].push_back('|');
      outputs[agent].append(event.data);
    });
    // Task starts immediately, so all agents are in flight before get() joins them.
    tasks.push_back(streams.back()->connect());
  }

  for (int agent = 0; agent != agent_count; ++agent) {
    if (tasks[agent].get().code != chhttp::Error::read ||
        outputs[agent].find(std::to_string(agent) + ":2") == std::string::npos)
      return 2;
  }
  provider.stop();
  std::cout << "completed " << agent_count
            << " bounded concurrent agent streams\n";
}
