#include <chhttp/chhttp.hpp>

#include <iostream>
#include <string>

int main() {
  chhttp::Server server;
  server.get_async(
      "/tokens", [](const chhttp::Request &,
                     chhttp::Response &response) -> chhttp::Task<void> {
        response.set_stream(
            "text/plain", [](chhttp::StreamWriter &writer)
                              -> chhttp::Task<void> {
              for (const auto *token : {"Hello", ", ", "agent", "!"}) {
                // Each await provides transport backpressure to the producer.
                if (!co_await writer.write(token)) co_return;
                co_await chhttp::sleep_for(std::chrono::milliseconds(5));
              }
              // flush() is useful when an application buffers above StreamWriter.
              co_await writer.flush();
            });
        co_return;
      });
  if (!server.start("127.0.0.1", 0)) return 1;

  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  std::string assembled;
  auto response = client.get(
      "/tokens", {},
      {.on_data = [&](std::string_view bytes) {
         // Returning false would cancel the transfer immediately.
         std::cout << "chunk: " << bytes << '\n';
         assembled.append(bytes);
         return true;
       },
       .on_progress = [](std::uint64_t received, std::uint64_t total) {
         // Chunked streams have no known total, so total is zero.
         std::cout << "received=" << received << " total=" << total << '\n';
         return true;
       }});
  if (!response || assembled != "Hello, agent!") {
    std::cerr << "stream failed\n";
    server.stop();
    return 2;
  }
  // on_data was supplied, therefore Response::body intentionally stays empty.
  std::cout << "assembled: " << assembled << '\n';
  server.stop();
}
