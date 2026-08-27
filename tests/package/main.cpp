#include <chhttp/chhttp.hpp>

int main() {
  chhttp::Headers headers{{"X-Agent", "chhttp"}};
  if (headers.get("x-agent") != "chhttp") return 1;
  const auto decoded = chhttp::url_decode(chhttp::url_encode("C++ agent/中文"));
  if (!decoded || *decoded != "C++ agent/中文") return 2;
  return chhttp::status_reason(200) == "OK" ? 0 : 3;
}

