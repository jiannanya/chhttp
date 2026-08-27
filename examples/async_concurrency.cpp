#include <chhttp/chhttp.hpp>

#include <iostream>
#include <vector>

int main() {
  chhttp::Server server;
  server.get_async(
      "/work/{id}", [](const chhttp::Request &request,
                        chhttp::Response &response) -> chhttp::Task<void> {
        // sleep_for() suspends the coroutine; it does not occupy a worker thread.
        co_await chhttp::sleep_for(std::chrono::milliseconds(10));
        response.set_content("completed:" + request.path_params.at("id"));
      });
  if (!server.start("127.0.0.1", 0)) return 1;

  chhttp::ClientOptions options;
  // This limits retained idle connections, not the number of in-flight requests.
  options.connection_pool_size = 16;
  chhttp::AsyncClient client("http://127.0.0.1:" +
                                 std::to_string(server.port()),
                             std::move(options));

  // Task<T> starts immediately, so build the whole batch before waiting.
  constexpr int request_count = 32;
  std::vector<chhttp::Task<chhttp::ResponseResult>> requests;
  requests.reserve(request_count);
  for (int index = 0; index != request_count; ++index)
    requests.push_back(client.get("/work/" + std::to_string(index)));

  for (int index = 0; index != request_count; ++index) {
    // A coroutine caller would use co_await; this synchronous main uses get().
    auto response = requests[index].get();
    if (!response || response->body != "completed:" + std::to_string(index)) {
      std::cerr << "request " << index << " failed\n";
      server.stop();
      return 2;
    }
  }
  std::cout << "completed " << request_count << " concurrent requests\n";
  server.stop();
}
