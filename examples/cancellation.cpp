#include <chhttp/chhttp.hpp>

#include <iostream>
#include <thread>

int main() {
  chhttp::Server server;
  // This delayed endpoint gives the client enough time to demonstrate active cancellation.
  server.get_async(
      "/slow", [](const chhttp::Request &,
                   chhttp::Response &response) -> chhttp::Task<void> {
        co_await chhttp::sleep_for(std::chrono::seconds(1));
        response.set_content("late response");
      });
  server.get("/health", [](const chhttp::Request &,
                            chhttp::Response &response) {
    response.set_content("healthy");
  });
  if (!server.start("127.0.0.1", 0)) return 1;

  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(server.port()));
  // Tasks start immediately even though this example waits on the result later.
  auto pending = client.get("/slow");
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // cancel() closes every active request owned by this client generation.
  client.cancel();
  auto cancelled = pending.get();
  if (cancelled || cancelled.error().code != chhttp::Error::cancelled) {
    std::cerr << "request was not cancelled as expected\n";
    server.stop();
    return 2;
  }

  // The client remains reusable; new requests belong to a fresh generation.
  auto health = client.get("/health").get();
  if (!health || health->body != "healthy") {
    server.stop();
    return 3;
  }
  std::cout << "cancelled slow request; recovery request succeeded\n";
  server.stop();
}
