#include <chhttp/chhttp.hpp>

#include <array>
#include <filesystem>
#include <fstream>
#include <iostream>

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "usage: chhttp_agent_vision_json <image-file>\n";
    return 1;
  }
  const std::filesystem::path image_path = argv[1];
  std::error_code filesystem_error;
  const auto image_size =
      std::filesystem::file_size(image_path, filesystem_error);
  if (filesystem_error) return 2;

  // This mock vision provider consumes a potentially 100 MiB JSON request
  // incrementally. A real provider would parse the JSON tokens incrementally.
  chhttp::Server provider;
  provider.post_stream(
      "/v1/responses",
      [](chhttp::Request &, chhttp::RequestBodyStream &body,
         chhttp::Response &response) -> chhttp::Task<void> {
        std::uint64_t wire_json_bytes = 0;
        auto error = co_await body.consume(
            [&](std::string_view chunk) -> chhttp::Task<bool> {
              wire_json_bytes += chunk.size();
              co_return true;
            });
        if (error) {
          response.status = 400;
          response.set_content(error.message);
          co_return;
        }
        response.set_content(R"({"received_json_bytes":)" +
                                 std::to_string(wire_json_bytes) + "}",
                             "application/json");
      },
      {.max_body_size = 160 * 1024 * 1024});
  if (!provider.start("127.0.0.1", 0)) return 3;

  chhttp::Request request;
  request.method = "POST";
  request.target = "/v1/responses";
  request.headers.set("Content-Type", "application/json");
  request.set_stream_body(
      [image_path](chhttp::StreamWriter &output) -> chhttp::Task<void> {
        std::ifstream image(image_path, std::ios::binary);
        if (!image) co_return;
        chhttp::JsonStreamWriter json(output);
        if (!co_await json.raw(
                R"({"model":"vision-agent","input_image":)"))
          co_return;
        if (!co_await json.begin_base64_string(
                "data:application/octet-stream;base64,"))
          co_return;
        std::array<char, 64 * 1024> block{};
        while (image) {
          image.read(block.data(), static_cast<std::streamsize>(block.size()));
          const auto count = image.gcount();
          if (count <= 0) break;
          const auto bytes = std::as_bytes(std::span(
              block.data(), static_cast<std::size_t>(count)));
          if (!co_await json.base64(bytes)) co_return;
        }
        if (!co_await json.end_base64_string()) co_return;
        co_await json.raw("}");
      });

  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(provider.port()));
  auto response = client.request(std::move(request)).get();
  provider.stop();
  if (!response) {
    std::cerr << response.error().message << '\n';
    return 4;
  }
  std::cout << "source bytes: " << image_size << '\n'
            << response->body << '\n';
  return response->status == 200 ? 0 : 5;
}
