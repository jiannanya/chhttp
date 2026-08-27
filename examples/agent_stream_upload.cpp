#include <chhttp/chhttp.hpp>

#include <iostream>
#include <string>

int main() {
  // This local endpoint represents an agent file/attachment ingestion API.
  // Both sides keep only the current transport chunk in memory.
  chhttp::Server service;
  service.post_stream(
      "/v1/files",
      [](chhttp::Request &, chhttp::RequestBodyStream &body,
         chhttp::Response &response) -> chhttp::Task<void> {
        std::uint64_t received = 0;
        const auto error = co_await body.consume(
            [&](std::string_view chunk) -> chhttp::Task<bool> {
              received += chunk.size();
              co_return true;
            });
        if (error) {
          response.status = error.code == chhttp::Error::body_too_large ? 413
                                                                        : 400;
          response.set_content(error.message);
          co_return;
        }
        response.set_content(R"({"bytes":)" + std::to_string(received) + "}",
                             "application/json");
      },
      {.max_body_size = 128 * 1024 * 1024});
  if (!service.start("127.0.0.1", 0)) return 1;

  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(service.port()));
  chhttp::Request request;
  request.method = "POST";
  request.target = "/v1/files";
  request.headers.set("Content-Type", "application/octet-stream");

  // Omitting the length selects HTTP/1.1 chunked transfer coding. In a real
  // agent, each iteration can read the next file block or generated media
  // fragment. Every awaited write provides transport backpressure.
  constexpr std::size_t chunk_size = 16 * 1024;
  constexpr std::size_t chunk_count = 64;
  request.set_stream_body([](chhttp::StreamWriter &writer)
                              -> chhttp::Task<void> {
    const std::string chunk(chunk_size, 'a');
    for (std::size_t index = 0; index != chunk_count; ++index) {
      if (!co_await writer.write(chunk)) co_return;
    }
  });

  auto response = client.request(std::move(request)).get();
  service.stop();
  if (!response || response->status != 200) return 2;
  std::cout << response->body << '\n';
  return response->body ==
                 R"({"bytes":)" +
                     std::to_string(chunk_size * chunk_count) + "}"
             ? 0
             : 3;
}
