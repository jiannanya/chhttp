#include <chhttp/chhttp.hpp>

#include <iostream>

int main() {
  // This local endpoint acts as a forward proxy and inspects the request form.
  chhttp::Server proxy;
  proxy.get("^.*$", [](const chhttp::Request &request,
                        chhttp::Response &response) {
    response.set_content("target=" + request.target + "\nauth=" +
                         request.get_header("Proxy-Authorization"));
  });
  if (!proxy.start("127.0.0.1", 0)) return 1;

  chhttp::ClientOptions options;
  options.proxy.url =
      "http://127.0.0.1:" + std::to_string(proxy.port());
  options.proxy.username = "proxy-user";
  options.proxy.password = "proxy-password";

  // For plain HTTP, the proxy receives an absolute-form request target.
  chhttp::Client client("http://service.example", std::move(options));
  auto response = client.get("/v1/models?active=true");
  if (!response) {
    std::cerr << response.error().message << '\n';
    proxy.stop();
    return 2;
  }
  std::cout << response->body << '\n';
  const bool absolute_target = response->body.find(
      "target=http://service.example/v1/models?active=true") !=
      std::string::npos;
  const bool basic_auth = response->body.find("auth=Basic ") !=
                          std::string::npos;
  proxy.stop();
  return absolute_target && basic_auth ? 0 : 3;
}
