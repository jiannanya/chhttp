#include <chhttp/chhttp.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>

int main() {
  // Use a unique temporary directory so concurrent example runs cannot collide.
  const auto suffix = std::chrono::steady_clock::now().time_since_epoch().count();
  const auto root = std::filesystem::temp_directory_path() /
                    ("chhttp-static-example-" + std::to_string(suffix));
  std::filesystem::create_directories(root);
  std::ofstream(root / "index.html", std::ios::binary) << "<h1>chhttp</h1>";
  std::ofstream(root / "model.bin", std::ios::binary) << "0123456789";

  chhttp::Server server;
  // Files are canonicalized and checked to remain under root before being opened.
  server.mount("/assets", root,
               {{"Cache-Control", "public, max-age=60"}});
  if (!server.start("127.0.0.1", 0)) {
    std::filesystem::remove_all(root);
    return 1;
  }

  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(server.port()));
  auto index = client.get("/assets/");
  auto range = client.get("/assets/model.bin", {{"Range", "bytes=3-6"}});
  if (!index || !range) {
    server.stop();
    std::filesystem::remove_all(root);
    return 2;
  }
  std::cout << "index type=" << index->headers.get("Content-Type") << '\n';
  std::cout << "range=" << range->body << " "
            << range->headers.get("Content-Range") << '\n';

  server.stop();
  std::error_code ignored;
  std::filesystem::remove_all(root, ignored);
  return range->status == 206 && range->body == "3456" ? 0 : 3;
}
