# chhttp

`chhttp` is a compiled static C++20 HTTP/HTTPS library for agent runtimes and other
long-running services. Its synchronous and asynchronous APIs share one
libuv transport engine: asynchronous operations use the library's small
`Task<T>` C++20 coroutine type, while `Client` is a blocking adapter over
`AsyncClient`.

The implementation is not header-only. On Windows libuv uses IOCP, on Linux it
uses epoll and on macOS/BSD it uses kqueue. A connection is a small state
machine and coroutine rather than a dedicated thread, so idle SSE and
WebSocket connections do not consume an operating-system thread.

The HTTP/1.x protocol layer is implemented by chhttp itself. It includes the
incremental request/response parser, serializer, message framing, Chunked and
Trailer handling, Keep-Alive state, `Expect: 100-continue`, pipelining buffers,
proxy forms and the RFC 6455 WebSocket handshake/frame state machine. libuv is
used only for cross-platform asynchronous TCP, DNS, timers and event dispatch;
no third-party HTTP, WebSocket or URL library is used.

## Capabilities

- HTTP/1.0 and HTTP/1.1 client and server; GET, HEAD, POST, PUT, PATCH, DELETE,
  OPTIONS and arbitrary extension methods
- Synchronous client and non-blocking coroutine client/server APIs
- Concurrent server sessions, configurable handler worker pool, timeouts, payload
  and header limits, keep-alive, TCP_NODELAY, cancellation and graceful stop
- Per-origin HTTP/HTTPS/CONNECT connection pools with independent active and
  idle limits, asynchronous queueing, exclusive checkout and safe stale retry
- HTTPS/WSS through OpenSSL 3, SNI, hostname verification, custom CAs, Windows
  root-store import, client certificates and optional server-side mTLS
- Ordered duplicate headers, ordered duplicate query/form fields, URL encoding,
  path parameters, regex routes, wildcard routes and routing middleware
- Buffered and callback-streamed downloads, response-head callbacks, awaited
  consumers with backpressure, progress and exact per-request cancellation
- Fixed-length and HTTP/1.1 chunked request-body producers with awaited
  transport backpressure, avoiding complete upload buffers in the client
- File-slice request bodies, independent upload/download progress, transfer
  failure context, strict Content-Range helpers and ordered asynchronous file
  sinks for application-managed resumable transfers
- Server request-body stream routes with per-route size/deadline/idle limits,
  awaited consumer backpressure, active cancellation and temporary-file spooling
- Public incremental WHATWG SSE parser plus GET/POST/arbitrary-method SSE
  client, event IDs, named events, retry delays, reconnect and `Last-Event-ID`
- RFC 6455 WebSocket client/server, text/binary messages, ping, close and
  subprotocol negotiation; connections remain asynchronous at scale
- Buffered and incremental `multipart/form-data` parsing plus a high-level
  field/file/file-slice/custom-stream writer with automatic fixed-length or
  chunked framing
- Incremental Base64 and streamed JSON-string production for vision/audio model
  inputs without constructing the encoded payload in memory
- Incremental gzip, deflate, Brotli and Zstandard response decoding, including
  genuinely streamed compressed responses
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

For another generator, point CMake at vcpkg or provide libuv 1.x, OpenSSL 3,
zlib, Brotli and Zstd packages yourself:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build build -j
ctest --test-dir build --output-on-failure
```

The test executable separates protocol/feature checks from load and lifecycle
checks. Run either group independently, or repeat the stress group until the
first failure:

```sh
ctest --test-dir build -L functional --output-on-failure
ctest --test-dir build -L stress --output-on-failure
ctest --test-dir build -L stress --repeat until-fail:30 --output-on-failure
```

Running `chhttp_tests` without an argument executes all 157 registered groups:
129 focused functional/boundary groups and 28 load/lifecycle groups. The stress
group covers concurrent sync/async HTTP, thousands of keep-alive requests,
large buffered and streamed uploads (including bounded-memory 100 MiB and
concurrent 10 MiB cases), streamed callbacks, connection recycling, independent
and global cancellation, client/server churn, parallel servers, graceful
draining, SSE fan-out, WebSocket connection/message load, malformed-request
floods and HTTPS/TLS handshake concurrency.

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

  server.start("127.0.0.1", 8080); // non-blocking; server owns its I/O loop

  chhttp::Client client("http://127.0.0.1:8080");
  auto response = client.get("/v1/agents/demo");
  if (!response) return 1;

  server.stop();
}
```

Use `listen` instead of `start` when the calling thread should remain blocked.
Passing port `0` asks the operating system for a free port; retrieve it with
`server.port()`.

## Examples

With `CHHTTP_BUILD_EXAMPLES=ON` (the default), CMake builds the following
independent programs. Each source file contains comments about ownership,
backpressure, cancellation, verification, and other behavior that matters in
long-running agent processes.

| Target | Source | Demonstrates |
|---|---|---|
| `chhttp_example` | `examples/example.cpp` | Minimal local server and client |
| `chhttp_sync_client` | `examples/sync_client.cpp` | Blocking requests, options, headers, queries, and errors |
| `chhttp_async_concurrency` | `examples/async_concurrency.cpp` | Concurrent requests through one libuv client |
| `chhttp_rest_server` | `examples/rest_server.cpp` | Long-running REST routes and graceful stop |
| `chhttp_streaming` | `examples/streaming.cpp` | Chunked producer backpressure and download callbacks |
| `chhttp_sse` | `examples/sse.cpp` | SSE server, client, named events, IDs, and retry values |
| `chhttp_websocket` | `examples/websocket.cpp` | Subprotocols, ping/pong, text echo, and close |
| `chhttp_multipart` | `examples/multipart.cpp` | Binary multipart upload and automatic server parsing |
| `chhttp_static_files` | `examples/static_files.cpp` | Static mounts, index files, MIME types, and byte ranges |
| `chhttp_middleware` | `examples/middleware.cpp` | Authentication middleware, logging, and error hooks |
| `chhttp_proxy` | `examples/proxy.cpp` | Forward-proxy absolute targets and Basic proxy auth |
| `chhttp_https` | `examples/https.cpp` | HTTPS server/client trust configuration |
| `chhttp_cancellation` | `examples/cancellation.cpp` | Active cancellation and safe client reuse |
| `chhttp_post_json_sse` | `examples/post_json_sse.cpp` | OpenAI-compatible POST JSON to validated SSE streaming |
| `chhttp_agent_tool_loop` | `examples/agent_tool_loop.cpp` | Streamed tool request, local execution, and follow-up turn |
| `chhttp_agent_parallel_fanout` | `examples/agent_parallel_fanout.cpp` | Many agent streams through a bounded per-origin pool |
| `chhttp_agent_stream_recovery` | `examples/agent_stream_recovery.cpp` | Retry before visible output and explicit interruption afterward |
| `chhttp_agent_backpressure` | `examples/agent_backpressure.cpp` | Public SSE parser with awaited durable-consumer backpressure |
| `chhttp_agent_stream_upload` | `examples/agent_stream_upload.cpp` | Memory-bounded client and server attachment streaming |
| `chhttp_agent_vision_json` | `examples/agent_vision_json.cpp` | Incremental Base64-in-JSON vision input from a file |
| `chhttp_agent_multipart_stream` | `examples/agent_multipart_stream.cpp` | Agent metadata/file multipart writer and incremental gateway parser |

For example:

```powershell
cmake --build --preset windows-msvc --target chhttp_sse
.\build-msvc\examples\chhttp_sse.exe
```

`chhttp_https` is the only example that needs external files. Pass a PEM
certificate and private key; optionally pass a trusted CA file to keep peer and
hostname verification enabled.

## Async client and high concurrency

`AsyncClient` owns one libuv event-loop thread. The same client can have many
independent requests in flight; each operation owns its parser and transport
state and does not block the loop. A returned `Task<T>` can be awaited or
consumed with `.get()` by a blocking caller.

```cpp
chhttp::Task<void> invoke(chhttp::AsyncClient& client) {
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

For a streamed model response, validate status and content type in
`on_response_head`, then consume bytes with `on_data` or the awaited
`on_data_async`. Returning `false` cancels only that request. Synchronous
callbacks execute on the client's libuv thread; use the asynchronous form when
the consumer must wait for queue capacity or durable storage.

```cpp
auto response = co_await client.get("/stream", {}, {
  .on_response_head = [](const chhttp::ResponseHead& head) {
    return head.status == 200;
  },
  .on_data = [](std::string_view bytes) {
    consume(bytes);
    return true;
  },
  .on_download_progress = [](const chhttp::TransferProgress& progress) {
    return true;
  }
});
```

`RequestOptions` also provides an active `std::stop_token`, an absolute
deadline or relative total timeout, independent connect/read/write/header/first-body
byte/idle timeouts, a per-request response limit and a decompression override.
The legacy shared atomic cancellation flag remains source-compatible but cannot
wake a blocked socket; new code should use `stop_token`.

## Async handlers and streaming responses

Synchronous route handlers run on the configured worker pool. Network waits and
timers can remain directly in an async handler:

```cpp
server.get_async("/events",
  [](const chhttp::Request&, chhttp::Response& response)
      -> chhttp::Task<void> {
    response.set_sse([](chhttp::SseWriter& writer)
        -> chhttp::Task<void> {
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

`Response::set_stream` exposes the same streamed writer for arbitrary content
(chunked in HTTP/1.1 and connection-delimited in HTTP/1.0).
The writer applies backpressure: each `co_await write` completes only after the
transport accepted that chunk.

Client request bodies use the same writer. Supply a content length for fixed
framing or omit it to use HTTP/1.1 chunked transfer coding:

```cpp
chhttp::Request upload;
upload.method = "POST";
upload.target = "/v1/files";
upload.set_stream_body([](chhttp::StreamWriter& writer)
    -> chhttp::Task<void> {
  while (auto block = co_await next_block()) {
    if (!co_await writer.write(*block)) co_return;
  }
});
auto response = co_await client.request(std::move(upload));
```

## Application-managed resumable transfers

`chhttp` exposes the stateless building blocks for resume logic. It does not
store checkpoints, infer a server-committed upload offset, retry side-effecting
requests, or implement provider-specific upload sessions.

Send an exact interval from a file without buffering it:

```cpp
chhttp::Request part;
part.method = "PUT";
part.target = "/uploads/session/chunk";
part.headers.set("Content-Range", "bytes 8388608-12582911/67108864");
auto configured = part.set_file_body(
    source, {.offset = 8388608, .length = 4 * 1024 * 1024});
if (!configured) co_return configured.error();

auto response = co_await client.request(std::move(part), {
  .on_upload_progress = [](const chhttp::TransferProgress& progress) {
    update_upload_ui(progress.transferred, progress.total);
    return true;
  }
});
```

Compose validated Range responses with an asynchronous file sink:

```cpp
auto sink_result = chhttp::AsyncFileSink::open(
    partial_path, {.mode = chhttp::FileSinkMode::append});
if (!sink_result) co_return sink_result.error();
auto sink = std::move(*sink_result);

chhttp::Headers headers;
auto ranged = chhttp::set_byte_range(headers, durable_size,
                                     std::nullopt, saved_etag);
if (!ranged) co_return ranged.error();
headers.set("Accept-Encoding", "identity");

auto response = co_await client.get(target, std::move(headers), {
  .on_response_head = [&](const chhttp::ResponseHead& head) {
    auto range = chhttp::parse_content_range(
        head.headers.get("Content-Range"));
    return head.status == 206 && range && range->unit == "bytes" &&
           range->first == durable_size;
  },
  .on_data_async = [&](std::string_view bytes) -> chhttp::Task<bool> {
    co_return !(co_await sink.write(bytes));
  },
  .auto_decompress = false
});
```

After a failure, `ErrorInfo::transfer` identifies the phase, uploaded and
downloaded payload counts, and whether a final response head was received.
These counters are observability data—not proof that the server durably
accepted the same upload offset.

## Server request-body streaming

Use a stream route when the service must not allocate the complete upload.
Routing happens after the request head and before the body is read. The awaited
consumer is the backpressure boundary: chhttp does not issue another socket read
while the callback is suspended.

```cpp
server.post_stream("/v1/files/{kind}",
  [](chhttp::Request& request, chhttp::RequestBodyStream& body,
     chhttp::Response& response) -> chhttp::Task<void> {
    std::uint64_t bytes = 0;
    auto error = co_await body.consume(
      [&](std::string_view chunk) -> chhttp::Task<bool> {
        co_await bounded_storage_queue.push(chunk); // copy before returning
        bytes += chunk.size();
        co_return true;
      });
    if (error) {
      response.status = error.code == chhttp::Error::body_too_large ? 413 : 400;
      response.set_content(error.message);
      co_return;
    }
    response.set_content(std::to_string(bytes));
  },
  {.max_body_size = 128 * 1024 * 1024,
   .first_body_byte_timeout = 10s,
   .idle_timeout = 30s,
   .total_timeout = 5min});
```

`discard()`, `save_to_file()`, and `save_to_temp()` are convenience consumers.
`save_to_temp()` creates a unique owned directory and removes it on failure; on
success the caller owns the returned file and its parent directory. Calling
`cancel()` actively closes only that request's connection. A callback returning
`false` stops parsing and makes the connection non-reusable because unread body
bytes remain. The server attempts to send the handler's response before closing,
but a client that is still uploading and does not read concurrently may observe
a transport error instead; consume/discard the body when a delivered error
response must be reliable.

Stream routes run on the server libuv thread. Do not perform long blocking file
or database operations in callbacks. Await an asynchronous sink or a bounded
queue instead. The `string_view` is valid only until its callback returns.
`save_to_file()` and `save_to_temp()` use synchronous incremental file writes;
they bound memory but an async file sink is preferable on latency-sensitive
servers.

## Streaming multipart and model payloads

`MultipartWriter` accepts buffered fields, regular files and custom streamed
parts. Regular-file chunks are read through a small shared worker pool so disk
reads do not block the client's libuv thread. If every size is known, `apply()`
declares an exact `Content-Length`; otherwise it selects HTTP/1.1 chunked
transfer coding.

```cpp
chhttp::MultipartWriter form;
form.add_field("metadata", metadata_json, "application/json")
    .add_file("image", image_path, "image/png");
chhttp::Request request;
request.method = "POST";
request.target = "/v1/artifacts";
form.apply(request);
auto response = co_await client.request(std::move(request));
```

On the server, `MultipartParser::feed()` exposes owned `part_begin`, `part_data`
and `part_end` events and retains only delimiter/header look-behind. The
`consume_multipart()` convenience function dispatches those events to awaited
callbacks and enforces independent part count, header, per-part and total-data
limits. It is suitable for routing file chunks directly to temporary files or
object storage.

For providers that require Base64 embedded in JSON, `JsonStreamWriter` combines
JSON escaping with `Base64Encoder`'s two-byte carry buffer:

```cpp
request.set_stream_body([path](chhttp::StreamWriter& output)
    -> chhttp::Task<void> {
  chhttp::JsonStreamWriter json(output);
  if (!co_await json.raw(R"({"image":)")) co_return;
  if (!co_await json.begin_base64_string("data:image/png;base64,")) co_return;
  while (auto bytes = next_file_block(path))
    if (!co_await json.base64(*bytes)) co_return;
  if (!co_await json.end_base64_string()) co_return;
  co_await json.raw("}");
});
```

## SSE client

```cpp
chhttp::SseClient stream(client, "/events", auth_headers);
stream.on_event("delta", [](const chhttp::SseEvent& event) {
  consume(event.data);
});
auto final_error = co_await stream.connect();
```

The parser handles CRLF, split input chunks, multi-line `data`, comments, BOM,
event IDs and server-provided retry delays. Reconnect is enabled by default for
the GET convenience constructor.

POST-based model streams use the method-agnostic `Request` constructor and
normally disable replay:

```cpp
chhttp::Request request;
request.method = "POST";
request.target = "/v1/chat/completions";
request.headers.set("Content-Type", "application/json");
request.body = payload;

chhttp::SseClient model_stream(client, std::move(request),
                               {.reconnect = false});
model_stream.on_event("delta", consume_delta);
auto final_error = co_await model_stream.connect();
```

`SseParser` is separately reusable with `feed()` and `finish()` for MCP
Streamable HTTP, recorded fixtures and provider-specific POST transports.

## WebSocket

```cpp
server.websocket("/ws",
  [](const chhttp::Request&, chhttp::WebSocket& ws)
      -> chhttp::Task<void> {
    while (auto message = co_await ws.read()) {
      if (!co_await ws.send_text(message->data)) break;
    }
  });

auto ws = co_await chhttp::AsyncWebSocketClient::connect(
    "wss://example.test/ws");
```

All operations on a single WebSocket must be serialized by the application;
many different WebSockets can operate concurrently on their libuv runtimes.

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

The defaults cap buffered request bodies at 64 MiB, response bodies at 128 MiB, headers
at 64 KiB, each client origin at 64 total connections and keep-alive sessions
at 1000 requests. A stream route can raise or lower its body limit independently
without allocating that amount. Streaming callbacks avoid a second body allocation.
Incremental compression output is bounded by the configured body limit, route
matching is performed before file access, percent-decoded static paths are
canonicalized, and credentials are removed on cross-origin redirects.

Tune `ServerOptions` and `ClientOptions` for model payload sizes and expected
connection counts. The async server does not create a thread per connection;
one libuv loop handles network I/O and `worker_threads` controls the pool used
for synchronous route handlers.
