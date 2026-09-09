#include <chhttp/chhttp.hpp>

int main() {
  chhttp::Headers headers{{"X-Agent", "chhttp"}};
  if (headers.get("x-agent") != "chhttp") return 1;
  const auto decoded = chhttp::url_decode(chhttp::url_encode("C++ agent/中文"));
  if (!decoded || *decoded != "C++ agent/中文") return 2;
  if (chhttp::status_reason(200) != "OK") return 3;
  const auto range = chhttp::parse_content_range("bytes 0-8/9");
  if (!range || !range->satisfied() || range->total != 9) return 6;
  if (chhttp::AsyncFileSink::open({})) return 7;
  chhttp::Request file_request;
  if (file_request.set_file_body({})) return 8;
  chhttp::Server server;
  server.get("/package", [](const chhttp::Request &,
                             chhttp::Response &response) {
    response.set_content("installed");
  });
  if (!server.start("127.0.0.1", 0)) return 4;
  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(server.port()));
  std::uint64_t downloaded = 0;
  auto response = client.get("/package", {},
      {.on_download_progress = [&](const chhttp::TransferProgress &progress) {
        downloaded = progress.transferred;
        return true;
      }}).get();
  server.stop();
  return response && response->body == "installed" && downloaded == 9 ? 0 : 5;
}
