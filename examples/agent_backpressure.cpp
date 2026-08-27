#include <chhttp/chhttp.hpp>

#include <iostream>
#include <string>

int main() {
  chhttp::Server provider;
  provider.post_async(
      "/tokens", [](const chhttp::Request &,
                      chhttp::Response &response) -> chhttp::Task<void> {
        response.set_sse([](chhttp::SseWriter &events)
                             -> chhttp::Task<void> {
          for (int index = 0; index != 20; ++index)
            if (!co_await events.send({.data = std::to_string(index),
                                       .event = "token"}))
              co_return;
        });
        co_return;
      });
  if (!provider.start("127.0.0.1", 0)) return 1;

  chhttp::Client client("http://127.0.0.1:" +
                        std::to_string(provider.port()));
  chhttp::SseParser parser;
  std::string committed;
  parser.on_event("token", [&](const chhttp::SseEvent &event) {
    if (!committed.empty()) committed.push_back(',');
    committed.append(event.data);
  });
  chhttp::ErrorInfo parser_error;

  auto response = client.post(
      "/tokens", R"({"stream":true})", "application/json", {},
      {.on_response_head = [](const chhttp::ResponseHead &head) {
         return head.status == 200 &&
                head.headers.get("Content-Type").starts_with(
                    "text/event-stream");
       },
       .on_data_async = [&](std::string_view bytes) -> chhttp::Task<bool> {
         parser_error = parser.feed(bytes);
         if (parser_error) co_return false;
         // This await models an asynchronous bounded-queue or durable Photon
         // commit. The transport will not read ahead until it completes.
         co_await chhttp::sleep_for(std::chrono::milliseconds(2));
         co_return true;
       },
       .total_timeout = std::chrono::seconds(5),
       .idle_timeout = std::chrono::seconds(1)});
  if (!parser_error) parser_error = parser.finish();
  provider.stop();
  if (!response || parser_error || committed.find("19") == std::string::npos)
    return 2;
  std::cout << "committed tokens: " << committed << '\n';
}
