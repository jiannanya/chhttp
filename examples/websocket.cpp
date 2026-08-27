#include <chhttp/chhttp.hpp>

#include <algorithm>
#include <iostream>

int main() {
  chhttp::Server server;
  server.websocket(
      "/chat",
      [](const chhttp::Request &, chhttp::WebSocket &socket)
          -> chhttp::Task<void> {
        // read() returns text, binary, ping, pong, or a close/error result.
        auto message = co_await socket.read();
        if (message && message->type == chhttp::WebSocket::MessageType::ping)
          message = co_await socket.read();
        if (message && message->type == chhttp::WebSocket::MessageType::text)
          co_await socket.send_text("echo:" + message->data);
      },
      [](const std::vector<std::string> &offered) {
        // A server may select only a protocol that the client actually offered.
        return std::ranges::find(offered, "agent.v1") != offered.end()
                   ? "agent.v1"
                   : "";
      });
  if (!server.start("127.0.0.1", 0)) return 1;

  auto connected = chhttp::AsyncWebSocketClient::connect(
      "ws://127.0.0.1:" + std::to_string(server.port()) + "/chat",
      {{"Sec-WebSocket-Protocol", "agent.v1, fallback"}}).get();
  if (!connected) {
    std::cerr << connected.error().message << '\n';
    server.stop();
    return 2;
  }
  auto socket = *connected;
  std::cout << "subprotocol=" << socket->subprotocol() << '\n';

  // ping() is answered automatically; read() still exposes the pong to the caller.
  socket->ping("health").get();
  auto pong = socket->read().get();
  if (!pong || pong->type != chhttp::WebSocket::MessageType::pong) return 3;

  if (!socket->send_text("hello").get()) return 4;
  auto echoed = socket->read().get();
  if (!echoed || echoed->data != "echo:hello") return 5;
  std::cout << echoed->data << '\n';

  // Use a normal close code and a short UTF-8 reason.
  socket->close(1000, "example complete").get();
  server.stop();
}
