# chhttp

`chhttp` is a compiled C++20 HTTP/HTTPS library for agent runtimes and other
long-running services. Its synchronous and asynchronous APIs share one
Boost.Asio/Beast protocol engine: asynchronous operations are stackless C++20
coroutines, while `Client` is a blocking adapter over `AsyncClient`.

The implementation is not header-only. On Windows it uses IOCP through Asio;
on Linux and macOS it uses the best available Asio reactor. A connection is a
small coroutine rather than a dedicated thread, so idle SSE and WebSocket
connections do not consume an operating-system thread.

## Capabilities

- HTTP/1.0 and HTTP/1.1 client and server; GET, HEAD, POST, PUT, PATCH, DELETE,
  OPTIONS and arbitrary extension methods
- Synchronous client and non-blocking coroutine client/server APIs
- Concurrent server sessions, configurable I/O thread count, timeouts, payload
  and header limits, keep-alive, TCP_NODELAY, cancellation and graceful stop
- Per-origin HTTP/HTTPS/CONNECT idle connection pools with exclusive checkout,
  bounded retention and safe stale-connection retry for idempotent requests
- HTTPS/WSS through OpenSSL 3, SNI, hostname verification, custom CAs, Windows
  root-store import, client certificates and optional server-side mTLS
- Ordered duplicate headers, ordered duplicate query/form fields, URL encoding,
  path parameters, regex routes, wildcard routes and routing middleware
- Buffered and callback-streamed downloads with progress and cancellation
- Chunked response writers, WHATWG-format SSE server/client, event IDs, named
  events, retry delays, reconnect and `Last-Event-ID`
- RFC 6455 WebSocket client/server, text/binary messages, ping, close and
  subprotocol negotiation; connections remain asynchronous at scale
- `multipart/form-data` encoding/parsing including binary file data
- gzip, deflate, Brotli and Zstandard response negotiation and client decode
- Static file mounts, index files, MIME types, byte ranges, HEAD, traversal
  prevention and custom mount headers
- Redirects with method rewriting and cross-origin credential stripping
- Forward HTTP proxy and HTTPS `CONNECT` tunnel with Basic proxy auth
- Basic/Bearer authentication plus RFC 7616 Digest challenge/retry (MD5,
  SHA-256, SHA-512-256 and session variants), custom logging, error and
  exception handlers, generated CMake package exports and a vcpkg manifest

The public protocol target is HTTP/1.1. HTTP/2 and HTTP/3 require different
wire engines (HPACK/QPACK, multiplexed streams and QUIC) and are intentionally
not disguised as HTTP/1 features.

## Build

The checked-in manifest pins dependency resolution to vcpkg. On the configured
Windows environment:

```powershell
cmake --preset windows-msvc
cmake --build --preset windows-msvc
ctest --preset windows-msvc
```

The matching `windows-clang` configure, build and test presets provide an
independent Clang 17/MSVC-ABI verification on Windows.

For another generator, point CMake at vcpkg or provide Boost 1.89+, OpenSSL 3,
zlib, Brotli and Zstd packages yourself:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Consumers link the installed package as follows:

```cmake
find_package(chhttp CONFIG REQUIRED)
target_link_libraries(my_agent PRIVATE chhttp::chhttp)
target_compile_features(my_agent PRIVATE cxx_std_20)
```

## Quick start

```cpp
#include <chhttp/chhttp.hpp>

int main() {
  chhttp::Server server;
  server.get("/v1/agents/{id}",
    [](const chhttp::Request& req, chhttp::Response& res) {
      res.set_content(R"({"id":")" + req.path_params.at("id") + "\"}",
                      "application/json");
    });

  server.start("127.0.0.1", 8080); // non-blocking; server owns I/O threads

  chhttp::Client client("http://127.0.0.1:8080");
  auto response = client.get("/v1/agents/demo");
  if (!response) return 1;

  server.stop();
}
```

Use `listen` instead of `start` when the calling thread should remain blocked.
Passing port `0` asks the operating system for a free port; retrieve it with
`server.port()`.

## Async client and high concurrency

`AsyncClient` uses the caller's executor. The same client can have many
independent requests in flight; each operation owns its parser and transport
state and does not block an I/O thread.

```cpp
chhttp::asio::awaitable<void> invoke(chhttp::AsyncClient& client) {
  chhttp::Request request;
  request.method = "POST";
  request.target = "/v1/chat/completions";
  request.headers.set("Authorization", chhttp::bearer_auth(token));
  request.headers.set("Content-Type", "application/json");
  request.body = payload;

  auto response = co_await client.request(std::move(request));
  if (!response) throw std::runtime_error(response.error().message);
}
```

For a streamed model response, set `RequestOptions::on_data`. Returning `false`
from the callback cancels the request without buffering the rest of the body.

```cpp
auto response = co_await client.get("/stream", {}, {
  .on_data = [](std::string_view bytes) {
    consume(bytes);
    return true;
  },
  .on_progress = [](std::uint64_t now, std::uint64_t total) {
    return true;
  }
});
```

## Async handlers and streaming responses

Blocking work should be moved to a separate executor. Network waits and timers
can remain directly in an async handler:

```cpp
server.get_async("/events",
  [](const chhttp::Request&, chhttp::Response& response)
      -> chhttp::asio::awaitable<void> {
    response.set_sse([](chhttp::SseWriter& writer)
        -> chhttp::asio::awaitable<void> {
      for (std::uint64_t sequence = 0; writer.open(); ++sequence) {
        if (!co_await writer.send({
              .data = make_delta(sequence),
              .event = "delta",
              .id = std::to_string(sequence)})) break;
      }
    });
    co_return;
  });
```

`Response::set_stream` exposes the same chunked writer for arbitrary content.
The writer applies backpressure: each `co_await write` completes only after the
transport accepted that chunk.

## SSE client

```cpp
chhttp::SseClient stream(client, "/events", auth_headers);
stream.on_event("delta", [](const chhttp::SseEvent& event) {
  consume(event.data);
});
auto final_error = co_await stream.connect();
```

The parser handles CRLF, split input chunks, multi-line `data`, comments, BOM,
event IDs and server-provided retry delays. Reconnect is enabled by default.

## WebSocket

```cpp
server.websocket("/ws",
  [](const chhttp::Request&, chhttp::WebSocket& ws)
      -> chhttp::asio::awaitable<void> {
    while (auto message = co_await ws.read()) {
      if (!co_await ws.send_text(message->data)) break;
    }
  });

auto ws = co_await chhttp::AsyncWebSocketClient::connect(
    co_await chhttp::asio::this_coro::executor,
    "wss://example.test/ws");
```

All operations on a single WebSocket must be serialized by the application;
many different WebSockets can operate concurrently on the same executor.

## TLS

```cpp
chhttp::ServerOptions server_options;
server_options.tls = chhttp::TlsServerOptions{
  .certificate_file = "server.crt",
  .private_key_file = "server.key",
  .client_ca_file = "clients-ca.crt",
  .require_client_certificate = true
};
chhttp::Server secure_server(server_options);

chhttp::ClientOptions client_options;
client_options.tls.ca_file = "service-ca.crt";
client_options.tls.certificate_file = "agent.crt";
client_options.tls.private_key_file = "agent.key";
chhttp::Client secure_client("https://service.internal", client_options);
```

Peer and hostname verification are enabled by default. Disabling verification
is intended only for controlled tests.

## Resource and safety defaults

The defaults cap request bodies at 64 MiB, response bodies at 128 MiB, headers
at 64 KiB and keep-alive sessions at 1000 requests. Streaming callbacks avoid a
second body allocation. Compression output is bounded by the configured body
limit, route matching is performed before file access, percent-decoded static
paths are canonicalized, and credentials are removed on cross-origin redirects.

Tune `ServerOptions` and `ClientOptions` for model payload sizes and expected
connection counts. The async server does not create a thread per connection;
`worker_threads` normally needs only the number of CPU cores used for network
and lightweight handler work.
