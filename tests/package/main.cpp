#include <chhttp/chhttp.hpp>

int main() {
  chhttp::Headers headers{{"X-Agent", "chhttp"}};
  if (headers.get("x-agent") != "chhttp") return 1;
  const auto decoded = chhttp::url_decode(chhttp::url_encode("C++ agent/中文"));
  if (!decoded || *decoded != "C++ agent/中文") return 2;
  if (chhttp::status_reason(200) != "OK") return 3;
  chhttp::Server server;
  server.get("/package", [](const chhttp::Request &,
                             chhttp::Response &response) {
    response.set_content("installed");
  });
  if (!server.start("127.0.0.1", 0)) return 4;
  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(server.port()));
  auto response = client.get("/package").get();
  server.stop();
  return response && response->body == "installed" ? 0 : 5;
}
