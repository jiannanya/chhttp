#include <chhttp/chhttp.hpp>

#include <iostream>

int main() {
  chhttp::Server server;
  server.post("/upload", [](const chhttp::Request &request,
                             chhttp::Response &response) {
    // multipart/form-data is parsed automatically before route dispatch.
    std::string summary;
    for (const auto &part : request.files) {
      summary += part.name + ":" + part.filename + ":" +
                 std::to_string(part.content.size()) + "\n";
    }
    response.set_content(summary);
  });
  if (!server.start("127.0.0.1", 0)) return 1;

  chhttp::MultipartPart prompt{.name = "prompt", .content = "describe image"};
  chhttp::MultipartPart image{.name = "image",
                              .filename = "input.bin",
                              .content_type = "application/octet-stream",
                              .content = std::string("a\0b\0c", 5)};
  // Custom per-part metadata is preserved by the encoder and parser.
  image.headers.set("X-Content-ID", "source-image");
  auto [content_type, body] = chhttp::make_multipart({prompt, image});

  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto response = client.post("/upload", std::move(body),
                              std::move(content_type));
  if (!response) {
    std::cerr << response.error().message << '\n';
    server.stop();
    return 2;
  }
  std::cout << response->body;
  server.stop();
  return response->body.find("image:input.bin:5") != std::string::npos ? 0 : 3;
}
