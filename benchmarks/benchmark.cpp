// chhttp benchmark harness.
//
// Measures the paths that matter for a long-running service: request-head
// parsing, per-request server overhead, connection scaling, static file
// delivery, body transfer, route matching and the memory held by idle or
// in-flight connections. Every case drives the public API (plus the internal
// read buffer for the parser case) so the numbers track the library as shipped.
//
// Loopback round trips are dominated by kernel scheduling latency rather than by
// library work, so each case reports both wall clock and CPU time per operation.
// The CPU column is the one that moves when the request path gets cheaper.
#include <chhttp/chhttp.hpp>

#include "../src/detail.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#include <psapi.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#include <malloc/malloc.h>
#include <sys/resource.h>
#else
#include <malloc.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

namespace {

using Clock = std::chrono::steady_clock;

struct CpuTime {
  double user{0};
  double system{0};
  [[nodiscard]] double total() const { return user + system; }
};

CpuTime cpu_time() {
#ifdef _WIN32
  FILETIME created{}, exited{}, kernel{}, user{};
  if (!GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user))
    return {};
  const auto to_seconds = [](const FILETIME &value) {
    return static_cast<double>(
               (static_cast<unsigned long long>(value.dwHighDateTime) << 32) |
               value.dwLowDateTime) /
           1e7;
  };
  return {to_seconds(user), to_seconds(kernel)};
#else
  rusage usage{};
  if (getrusage(RUSAGE_SELF, &usage) != 0) return {};
  const auto to_seconds = [](const timeval &value) {
    return static_cast<double>(value.tv_sec) +
           static_cast<double>(value.tv_usec) / 1e6;
  };
  return {to_seconds(usage.ru_utime), to_seconds(usage.ru_stime)};
#endif
}

std::size_t peak_resident_bytes() {
#ifdef _WIN32
  PROCESS_MEMORY_COUNTERS counters{};
  if (GetProcessMemoryInfo(GetCurrentProcess(), &counters, sizeof(counters)))
    return counters.WorkingSetSize;
  return 0;
#else
  rusage usage{};
  if (getrusage(RUSAGE_SELF, &usage) != 0) return 0;
#ifdef __APPLE__
  return static_cast<std::size_t>(usage.ru_maxrss); // bytes
#else
  return static_cast<std::size_t>(usage.ru_maxrss) * 1024; // kibibytes
#endif
#endif
}

// Current resident set, as opposed to peak_resident_bytes(), so a case can show
// what connections cost while they exist and what is returned once they close.
std::size_t current_resident_bytes() {
#ifdef _WIN32
  PROCESS_MEMORY_COUNTERS counters{};
  if (GetProcessMemoryInfo(GetCurrentProcess(), &counters, sizeof(counters)))
    return counters.WorkingSetSize;
  return 0;
#elif defined(__APPLE__)
  mach_task_basic_info info{};
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS)
    return 0;
  return static_cast<std::size_t>(info.resident_size);
#elif defined(__linux__)
  std::ifstream status("/proc/self/statm");
  std::size_t total = 0;
  std::size_t resident = 0;
  if (!(status >> total >> resident)) return 0;
  return resident * static_cast<std::size_t>(sysconf(_SC_PAGESIZE));
#else
  return 0;
#endif
}

// Bytes the allocator has handed out, and how many blocks that is.
//
// Resident memory is the number an operator sees, but it also counts the pages
// the allocator has committed for its own bookkeeping. A burst of short-lived
// allocations enlarges that footprint far beyond what is live at the sampling
// moment, which makes RSS look like "cost per connection" when it is really
// "peak churn". These counters are what actually move when per-connection or
// per-request retention changes, so memory cases report both.
struct HeapUsage {
  std::size_t in_use{0};
  std::size_t blocks{0};
  [[nodiscard]] bool available() const { return blocks != 0 || in_use != 0; }
};

HeapUsage heap_usage() {
#if defined(__APPLE__)
  malloc_statistics_t statistics{};
  malloc_zone_statistics(malloc_default_zone(), &statistics);
  return {statistics.size_in_use, statistics.blocks_in_use};
#elif defined(__linux__) && defined(__GLIBC__) &&                              \
    (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 33))
  const auto info = mallinfo2();
  return {static_cast<std::size_t>(info.uordblks), 0};
#else
  return {};
#endif
}

void report_heap(const char *label, HeapUsage value, HeapUsage baseline) {
  const auto per =
      static_cast<double>(value.in_use) - static_cast<double>(baseline.in_use);
    std::printf("    %-24s %8.2f MiB live  (%+.2f MiB, %+lld blocks)\n", label,
              static_cast<double>(value.in_use) / 1048576.0, per / 1048576.0,
              static_cast<long long>(value.blocks) - static_cast<long long>(baseline.blocks));
}

void report_memory(const char *label, std::size_t bytes, std::size_t baseline) {
  const auto delta = static_cast<double>(bytes) - static_cast<double>(baseline);
  std::printf("    %-24s %8.2f MiB  (%+.2f MiB)\n", label,
              static_cast<double>(bytes) / 1048576.0, delta / 1048576.0);
}

// Brackets a measured region and reports wall clock plus CPU time per operation.
class Measurement {
public:
  Measurement() : start_(Clock::now()), cpu_start_(cpu_time()) {}

  void report(const std::string &name, double operations) const {
    const auto wall = std::chrono::duration<double>(Clock::now() - start_).count();
    const auto cpu = cpu_time();
    const auto user = cpu.user - cpu_start_.user;
    const auto system = cpu.system - cpu_start_.system;
    const auto per = [&](double value) {
      return operations > 0 ? value / operations * 1e6 : 0;
    };
    std::printf("%-42s %9.0f ops/s  wall %7.3f us  user %7.3f us  sys %7.3f us\n",
                name.c_str(), wall > 0 ? operations / wall : 0,
                operations > 0 ? wall / operations * 1e6 : 0, per(user),
                per(system));
  }

  void report_throughput(double bytes) const {
    const auto wall = std::chrono::duration<double>(Clock::now() - start_).count();
    std::printf("    %.1f MiB/s payload throughput\n",
                wall > 0 ? bytes / wall / 1048576.0 : 0);
  }

  [[nodiscard]] double seconds() const {
    return std::chrono::duration<double>(Clock::now() - start_).count();
  }

private:
  Clock::time_point start_;
  CpuTime cpu_start_;
};

std::string working_directory(const char *name) {
  const auto path = std::filesystem::temp_directory_path() / name;
  std::filesystem::create_directories(path);
  return path.string();
}

// ---------------------------------------------------------------------------
// Request-head parsing with no transport involved: the whole head is already in
// the read buffer, so this isolates the parser, the framing validation and the
// cost of taking ownership of the parsed fields.
// ---------------------------------------------------------------------------
void require(bool condition) {
  if (!condition) throw std::runtime_error("Benchmark correctness check failed");
}

void benchmark_parse(std::size_t iterations) {
  std::string head = "POST /v1/items/42?expand=all&limit=10 HTTP/1.1\r\n";
  head += "Host: api.example.com\r\n";
  head += "User-Agent: chhttp-bench/1.0\r\n";
  head += "Accept: application/json\r\n";
  head += "Accept-Encoding: gzip, deflate, br, zstd\r\n";
  head += "Content-Type: application/json; charset=utf-8\r\n";
  head += "Content-Length: 2\r\n";
  head += "Authorization: Bearer abcdefghijklmnopqrstuvwxyz012345\r\n";
  head += "X-Request-Id: 0123456789abcdef0123456789abcdef\r\n";
  head += "X-Trace-Span: 0123456789abcdef\r\n";
  head += "\r\n";
  std::size_t parsed_fields = 0;
  double parse_seconds = 0;
  {
    Measurement measurement;
    for (std::size_t index = 0; index < iterations; ++index) {
      chhttp::detail::ReadBuffer buffer;
      buffer.append(head);
      auto result = chhttp::detail::read_request_head({}, buffer, {}).get();
      require(result && result->body.headers.size() == 9);
      parsed_fields += result->body.headers.size();
    }
    measurement.report("parse request head (9 fields)",
                       static_cast<double>(iterations));
    parse_seconds = measurement.seconds();
  }
  if (parse_seconds > 0)
    std::printf("    %.1f M header fields/s\n",
                static_cast<double>(parsed_fields) / parse_seconds / 1e6);
}

// ---------------------------------------------------------------------------
// Encoding and streaming protocol paths, checked against their expected output.
// ---------------------------------------------------------------------------
void benchmark_encoding(std::size_t iterations) {
  std::string payload(64 * 1024, '\0');
  for (std::size_t i = 0; i < payload.size(); ++i)
    payload[i] = static_cast<char>(i);
  const auto encoded = chhttp::detail::base64_encode(payload);
  const auto bytes = std::as_bytes(std::span(payload));
  {
    Measurement measurement;
    for (std::size_t i = 0; i < iterations; ++i) {
      chhttp::Base64Encoder encoder;
      auto result = encoder.update(bytes);
      result += encoder.finish();
      require(result == encoded);
    }
    measurement.report("stream Base64 encode 64 KiB", static_cast<double>(iterations));
    measurement.report_throughput(static_cast<double>(iterations) * payload.size());
  }
  {
    Measurement measurement;
    for (std::size_t i = 0; i < iterations; ++i) {
      auto result = chhttp::detail::base64_decode(encoded);
      require(result && *result == payload);
    }
    measurement.report("Base64 decode 64 KiB", static_cast<double>(iterations));
    measurement.report_throughput(static_cast<double>(iterations) * payload.size());
  }
}

void benchmark_sse(std::size_t iterations) {
  const std::string payload(256, 's');
  const auto event = chhttp::format_sse({.data = payload, .id = "42"});
  std::string batch;
  for (int i = 0; i < 256; ++i) batch += event;
  chhttp::SseParser parser;
  std::size_t count = 0;
  parser.on_message([&](const chhttp::SseEvent &value) {
    require(value.data == payload && value.id == "42");
    ++count;
  });
  Measurement measurement;
  for (std::size_t i = 0; i < iterations; ++i) require(!parser.feed(batch));
  require(count == iterations * 256 && !parser.finish());
  measurement.report("SSE parse 256-byte events", static_cast<double>(count));
}

void benchmark_chunked(std::size_t iterations) {
  std::string wire = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
  for (int i = 0; i < 4096; ++i) wire += "8\r\n12345678\r\n";
  wire += "0\r\nX-End: yes\r\n\r\n";
  auto runtime = std::make_shared<chhttp::detail::Runtime>();
  auto connection = std::make_shared<chhttp::detail::Connection>(runtime);
  auto run = [&]() -> chhttp::Task<void> {
    co_await chhttp::detail::resume_on(runtime);
    Measurement measurement;
    for (std::size_t i = 0; i < iterations; ++i) {
      chhttp::detail::ReadBuffer buffer;
      buffer.append_view(wire);
      auto response = co_await chhttp::detail::read_response(connection, buffer, "GET", {});
      require(response && response->body.size() == 4096 * 8 &&
              response->headers.get("X-End") == "yes" && buffer.empty());
    }
    measurement.report("HTTP parse 8-byte chunks", static_cast<double>(iterations) * 4096);
  };
  run().get();
}

void benchmark_websocket_frames(std::size_t iterations) {
  const std::string payload(256, 'w');
  std::string frame{"\x82\xfe\x01\x00\x01\x02\x03\x04", 8};
  for (std::size_t i = 0; i < payload.size(); ++i)
    frame.push_back(static_cast<char>(payload[i] ^ ((i % 4) + 1)));
  std::string batch;
  for (int i = 0; i < 1024; ++i) batch += frame;
  Measurement measurement;
  for (std::size_t i = 0; i < iterations; ++i) {
    chhttp::detail::ReadBuffer buffer;
    buffer.append_view(batch);
    auto channel = chhttp::detail::make_websocket_channel(
        {}, std::move(buffer), false, {}, std::chrono::seconds(5));
    for (int j = 0; j < 1024; ++j) {
      auto message = channel->read().get();
      require(message && message->data == payload &&
              message->type == chhttp::WebSocket::MessageType::binary);
    }
  }
  measurement.report("WebSocket parse masked 256-byte frames",
                     static_cast<double>(iterations) * 1024);
}

void benchmark_streaming(std::size_t iterations) {
  chhttp::ServerOptions options;
  options.auto_compress_response = false;
  chhttp::Server server(options);
  server.get("/stream", [](const auto &, auto &response) {
    response.set_stream("application/octet-stream",
        [](chhttp::StreamWriter &writer) -> chhttp::Task<void> {
          for (int i = 0; i < 4096; ++i)
            require(co_await writer.write("0123456789abcdef0123456789abcdef"));
        });
  });
  require(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  Measurement measurement;
  for (std::size_t i = 0; i < iterations; ++i) {
    auto response = client.get("/stream");
    require(response && response->body.size() == 4096 * 32);
    for (std::size_t j = 0; j < response->body.size(); j += 32)
      require(std::string_view(response->body).substr(j, 32) == "0123456789abcdef0123456789abcdef");
  }
  measurement.report("stream 32-byte chunks over TCP", static_cast<double>(iterations) * 4096);
  server.stop();
}

void benchmark_keep_alive_requests(std::size_t iterations) {
  chhttp::ServerOptions options;
  options.auto_compress_response = false;
  chhttp::Server server(options);
  server.get("/ping", [](const chhttp::Request &, chhttp::Response &response) {
    response.set_content("pong");
  });
  server.route("GET", "/users/:id", [](const chhttp::Request &request,
                                       chhttp::Response &response) {
    response.set_content("user:" + request.path_params.at("id"));
  });
  require(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  for (int index = 0; index < 64; ++index) require(static_cast<bool>(client.get("/ping")));
  Measurement measurement;
  for (std::size_t index = 0; index < iterations; ++index) {
    const bool ping = (index & 1U) != 0;
    auto response = client.get(ping ? "/ping" : "/users/7");
    require(response && response->body == (ping ? "pong" : "user:7"));
  }
  measurement.report("keep-alive requests (1 connection)",
                     static_cast<double>(iterations));
  server.stop();
}

// ---------------------------------------------------------------------------
// Server scalability: the same request at increasing connection counts. Once the
// server loop saturates, extra connections stop helping and per-request CPU cost
// becomes the throughput limit.
// ---------------------------------------------------------------------------
void benchmark_connection_scaling(std::size_t iterations, unsigned connections) {
  chhttp::ServerOptions options;
  options.auto_compress_response = false;
  chhttp::Server server(options);
  server.get("/ping", [](const chhttp::Request &, chhttp::Response &response) {
    response.set_content("pong");
  });
  require(server.start("127.0.0.1", 0));
  const auto url = "http://127.0.0.1:" + std::to_string(server.port());
  std::atomic_size_t completed{0};
  const std::size_t per_connection = iterations / connections;
  Measurement measurement;
  std::vector<std::thread> threads;
  threads.reserve(connections);
  for (unsigned index = 0; index < connections; ++index) {
    threads.emplace_back([&] {
      chhttp::Client client(url);
      for (std::size_t request = 0; request < per_connection; ++request) {
        auto response = client.get("/ping");
        if (response && response->body == "pong")
          completed.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (auto &thread : threads) thread.join();
  require(completed.load() == per_connection * connections);
  measurement.report("requests over " + std::to_string(connections) + " connections",
                     static_cast<double>(completed.load()));
  server.stop();
}

// ---------------------------------------------------------------------------
// Static file delivery: exercises the file pool, the socket writer and the
// directory-traversal checks.
// ---------------------------------------------------------------------------
void benchmark_static_file(std::size_t size, std::size_t iterations) {
  const auto directory = working_directory("chhttp-bench-static");
  const auto path = std::filesystem::path(directory) / "payload.bin";
  {
    const std::string payload(size, 'x');
    std::ofstream output(path, std::ios::binary);
    output.write(payload.data(), static_cast<std::streamsize>(payload.size()));
  }
  chhttp::ServerOptions options;
  options.auto_compress_response = false;
  chhttp::Server server(options);
  server.mount("/static", directory);
  require(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  std::size_t transferred = 0;
  {
    Measurement measurement;
    for (std::size_t index = 0; index < iterations; ++index) {
      auto response = client.get("/static/payload.bin");
      require(response && response->body.size() == size);
      transferred += response->body.size();
    }
    measurement.report("static file " + std::to_string(size / 1024) + " KiB",
                       static_cast<double>(iterations));
    measurement.report_throughput(static_cast<double>(transferred));
  }
  server.stop();
}

// ---------------------------------------------------------------------------
// Large buffered request and response bodies.
// ---------------------------------------------------------------------------
void benchmark_body_transfer(std::size_t size, std::size_t iterations) {
  chhttp::ServerOptions options;
  options.auto_compress_response = false;
  chhttp::Server server(options);
  server.post("/echo", [](const chhttp::Request &request,
                          chhttp::Response &response) {
    response.body = request.body;
  });
  require(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  const std::string payload(size, 'b');
  std::size_t transferred = 0;
  {
    Measurement measurement;
    for (std::size_t index = 0; index < iterations; ++index) {
      auto response = client.post("/echo", payload, "application/octet-stream");
      require(response && response->body == payload);
      transferred += response->body.size();
    }
    measurement.report("buffered echo " + std::to_string(size / 1024) + " KiB",
                       static_cast<double>(iterations));
    measurement.report_throughput(static_cast<double>(transferred));
  }
  server.stop();
}

// ---------------------------------------------------------------------------
// Route lookup against a large table with literal, parameterised and wildcard
// routes, mixing hits at the front and the back of the registration order.
// ---------------------------------------------------------------------------
void benchmark_route_table(std::size_t routes, std::size_t iterations) {
  chhttp::ServerOptions options;
  options.auto_compress_response = false;
  chhttp::Server server(options);
  for (std::size_t index = 0; index < routes; ++index) {
    server.get("/literal/" + std::to_string(index),
               [](const chhttp::Request &, chhttp::Response &response) {
                 response.set_content("literal");
               });
    server.post("/literal/" + std::to_string(index),
                [](const chhttp::Request &, chhttp::Response &response) {
                  response.set_content("literal");
                });
    server.get("/item/" + std::to_string(index) + "/:id",
               [](const chhttp::Request &request, chhttp::Response &response) {
                 response.set_content(request.path_params.at("id"));
               });
  }
  server.get("/assets/*", [](const chhttp::Request &request,
                             chhttp::Response &response) {
    response.set_content(request.path_params.at("wildcard"));
  });
  require(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  const auto target = "/literal/" + std::to_string(routes / 2);
  const auto parameterised = "/item/" + std::to_string(routes / 2) + "/42";
  for (int index = 0; index < 32; ++index) require(static_cast<bool>(client.get(target)));
  Measurement measurement;
  for (std::size_t index = 0; index < iterations; ++index) {
    const auto choice = index % 3;
    auto response = client.get(choice == 0 ? target : choice == 1 ? parameterised : "/assets/a/b/c");
    require(response && response->body == (choice == 0 ? "literal" : choice == 1 ? "42" : "a/b/c"));
  }
  measurement.report("routing across " + std::to_string(routes * 3 + 1) + " routes",
                     static_cast<double>(iterations));
  server.stop();
}

// ---------------------------------------------------------------------------
// Memory: what a keep-alive connection costs while it is open. Both ends of the
// loopback pair are in this process, so the delta covers the server session and
// the client lease together. A server gate holds every request until all
// connections are established, which forces the pool to keep them all.
// ---------------------------------------------------------------------------
void benchmark_idle_connections(std::size_t connections) {
  chhttp::ServerOptions options;
  options.auto_compress_response = false;
  chhttp::Server server(options);
  std::atomic_size_t arrived{0};
  server.get_async("/gate", [&](const chhttp::Request &,
                                chhttp::Response &response) -> chhttp::Task<void> {
    arrived.fetch_add(1);
    const auto deadline = Clock::now() + std::chrono::seconds(10);
    while (arrived.load() < connections && Clock::now() < deadline)
      co_await chhttp::sleep_for(std::chrono::milliseconds(1));
    response.set_content("open");
    co_return;
  });
  server.get("/ping", [](const chhttp::Request &, chhttp::Response &response) {
    response.set_content("pong");
  });
  if (!server.start("127.0.0.1", 0)) {
    std::printf("idle-connection benchmark: server did not start\n");
    return;
  }
  chhttp::ClientOptions client_options;
  client_options.max_connections_per_origin = connections;
  client_options.connection_pool_size = connections;
  const auto baseline = current_resident_bytes();
  const auto heap_baseline = heap_usage();
  std::size_t idle_bytes = 0;
  std::size_t reused_bytes = 0;
  HeapUsage idle_heap{};
  HeapUsage reused_heap{};
  bool completed = false;
  {
    chhttp::AsyncClient client("http://127.0.0.1:" + std::to_string(server.port()),
                               client_options);
    std::vector<chhttp::Task<chhttp::ResponseResult>> pending;
    pending.reserve(connections);
    for (std::size_t index = 0; index < connections; ++index)
      pending.push_back(client.get("/gate"));
    bool ok = true;
    for (auto &task : pending) {
      auto response = task.get();
      ok = static_cast<bool>(response) && ok;
    }
    if (ok) {
      // Every socket is parked in the pool at this point.
      idle_bytes = current_resident_bytes();
      idle_heap = heap_usage();
      // Prove each parked socket is usable, so the sample cannot hide sockets
      // that would be discarded on the next use.
      for (std::size_t index = 0; index < connections; ++index)
        client.get("/ping").get();
      reused_bytes = current_resident_bytes();
      reused_heap = heap_usage();
      completed = true;
    } else {
      std::printf("idle-connection benchmark: request failed\n");
    }
  }
  // The client is destroyed here: every pooled socket has been closed.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  const auto released = current_resident_bytes();
  if (completed) {
    std::printf("idle keep-alive connections: %zu\n", connections);
    report_memory("resident baseline", baseline, baseline);
    report_memory("all idle", idle_bytes, baseline);
    report_memory("after reuse", reused_bytes, baseline);
    report_memory("after close", released, baseline);
    const auto per_connection = [&](std::size_t bytes) {
      return connections == 0
                 ? 0.0
                 : (static_cast<double>(bytes) - static_cast<double>(baseline)) /
                       static_cast<double>(connections) / 1024.0;
    };
    std::printf("    %.1f KiB resident per idle connection (both ends in-process)\n",
                per_connection(idle_bytes));
    if (heap_baseline.available() && idle_heap.available()) {
      report_heap("heap baseline", heap_baseline, heap_baseline);
      report_heap("heap all idle", idle_heap, heap_baseline);
      report_heap("heap after reuse", reused_heap, heap_baseline);
      const auto per_heap = [&](HeapUsage value) {
        return connections == 0
                   ? 0.0
                   : (static_cast<double>(value.in_use) -
                      static_cast<double>(heap_baseline.in_use)) /
                         static_cast<double>(connections) / 1024.0;
      };
      std::printf(
          "    %.2f KiB live, %.2f blocks per idle connection (both ends)\n",
          per_heap(idle_heap),
          connections == 0
              ? 0.0
              : (static_cast<double>(idle_heap.blocks) -
                 static_cast<double>(heap_baseline.blocks)) /
                    static_cast<double>(connections));
    }
  }
  server.stop();
}

// ---------------------------------------------------------------------------
// Memory: streamed request bodies must stay bounded. `connections` uploads of
// `payload_bytes` are held in flight while the server drains them slowly, and
// the resident set is sampled at that moment. A buffered reader would need
// connections * payload_bytes of heap here.
// ---------------------------------------------------------------------------
void benchmark_streaming_memory(std::size_t connections, std::size_t payload_bytes) {
  chhttp::ServerOptions options;
  options.auto_compress_response = false;
  chhttp::Server server(options);
  std::atomic_size_t consuming{0};
  std::atomic_uint64_t received{0};
  server.post_stream("/drain", [&](chhttp::Request &, chhttp::RequestBodyStream &body,
                                   chhttp::Response &response) -> chhttp::Task<void> {
    consuming.fetch_add(1);
    auto error = co_await body.consume(
        [&](std::string_view data) -> chhttp::Task<bool> {
          received.fetch_add(data.size(), std::memory_order_relaxed);
          // Slow consumer: backpressure keeps the payload in the sockets
          // rather than in library buffers.
          co_await chhttp::sleep_for(std::chrono::milliseconds(1));
          co_return true;
        });
    consuming.fetch_sub(1);
    if (error)
      response.status = 400;
    else
      response.set_content("drained");
    co_return;
  });
  if (!server.start("127.0.0.1", 0)) {
    std::printf("streaming-memory benchmark: server did not start\n");
    return;
  }
  chhttp::ClientOptions client_options;
  client_options.max_connections_per_origin = connections;
  client_options.connection_pool_size = connections;
  const auto baseline = current_resident_bytes();
  const auto heap_baseline = heap_usage();
  std::size_t in_flight_bytes = 0;
  HeapUsage in_flight_heap{};
  bool completed = false;
  {
    chhttp::AsyncClient client("http://127.0.0.1:" + std::to_string(server.port()),
                               client_options);
    std::vector<chhttp::Task<chhttp::ResponseResult>> pending;
    pending.reserve(connections);
    for (std::size_t index = 0; index < connections; ++index) {
      chhttp::Request request;
      request.method = "POST";
      request.target = "/drain";
      request.set_stream_body(
          [payload_bytes](chhttp::StreamWriter &writer) -> chhttp::Task<void> {
            const std::string chunk(64 * 1024, 'm');
            std::uint64_t remaining = payload_bytes;
            while (remaining > 0 && writer.open()) {
              const auto count = static_cast<std::size_t>(
                  std::min<std::uint64_t>(remaining, chunk.size()));
              if (!co_await writer.write(
                      std::string_view(chunk).substr(0, count)))
                break;
              remaining -= count;
            }
          },
          payload_bytes);
      pending.push_back(client.request(std::move(request)));
    }
    // Wait until every handler is consuming, then sample while the payloads are
    // genuinely in flight.
    const auto deadline = Clock::now() + std::chrono::seconds(30);
    while (consuming.load() < connections && Clock::now() < deadline)
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    in_flight_bytes = current_resident_bytes();
    in_flight_heap = heap_usage();
    bool ok = consuming.load() == connections;
    for (auto &task : pending) {
      auto response = task.get();
      ok = static_cast<bool>(response) && ok;
    }
    completed = ok;
  }
  server.stop();
  const double payload_mib =
      static_cast<double>(connections) * static_cast<double>(payload_bytes) /
      1048576.0;
  if (completed) {
    std::printf("streamed uploads in flight: %zu x %.1f MiB (%.1f MiB payload)\n",
                connections, static_cast<double>(payload_bytes) / 1048576.0,
                payload_mib);
    report_memory("resident baseline", baseline, baseline);
    report_memory("while in flight", in_flight_bytes, baseline);
    if (heap_baseline.available() && in_flight_heap.available())
      report_heap("heap while in flight", in_flight_heap, heap_baseline);
    std::printf("    resident growth is %.2f%% of the payload volume\n",
                payload_mib > 0
                    ? (static_cast<double>(in_flight_bytes) -
                       static_cast<double>(baseline)) /
                          (payload_mib * 1048576.0) * 100.0
                    : 0.0);
  } else {
    std::printf("streaming-memory benchmark: transfer failed\n");
  }
}

} // namespace

int main(int argc, char **argv) {
  if (argc > 1 && std::string_view(argv[1]) == "--help") {
    std::printf("usage: %s [scale] [case-substring]\n\n"
                "  scale            multiplies every operation count (default 1)\n"
                "  case-substring   run only cases whose name contains this text\n\n"
                "cases:\n"
                "  parse        request-head parsing\n"
                "  chunked      buffered chunked-body parsing\n"
                "  encoding     streaming Base64 encode and decode\n"
                "  sse          SSE event parsing\n"
                "  websocket    buffered masked WebSocket frame parsing\n"
                "  streaming    small streamed chunks over TCP\n"
                "  keep-alive   single-connection round trips\n"
                "  1-conn       60k requests over 1 connection\n"
                "  8-conn       60k requests over 8 connections\n"
                "  64-conn      120k requests over 64 connections\n"
                "  static       static file delivery\n"
                "  echo         buffered request/response bodies\n"
                "  routing      large route table\n"
                "  memory-idle  resident memory per idle keep-alive connection\n"
                "  memory-stream  resident memory for streamed uploads in flight\n",
                argv[0]);
    return 0;
  }
  const std::size_t scale = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 1;
  if (scale == 0) {
    std::printf("usage: %s [scale] [case-substring]\n", argv[0]);
    return 2;
  }
  const std::string_view selector = argc > 2 ? argv[2] : "";
  const auto selected = [&](std::string_view name) {
    return selector.empty() || name.find(selector) != std::string_view::npos;
  };
  std::printf("chhttp benchmarks (scale %zu), peak resident %.1f MiB\n\n", scale,
              static_cast<double>(peak_resident_bytes()) / 1048576.0);
  if (selected("parse")) benchmark_parse(200000 * scale);
  if (selected("chunked")) benchmark_chunked(256 * scale);
  if (selected("encoding")) benchmark_encoding(2048 * scale);
  if (selected("sse")) benchmark_sse(4096 * scale);
  if (selected("websocket")) benchmark_websocket_frames(512 * scale);
  if (selected("streaming")) benchmark_streaming(32 * scale);
  if (selected("keep-alive")) benchmark_keep_alive_requests(20000 * scale);
  if (selected("1-conn")) benchmark_connection_scaling(60000 * scale, 1);
  if (selected("8-conn")) benchmark_connection_scaling(60000 * scale, 8);
  if (selected("64-conn")) benchmark_connection_scaling(120000 * scale, 64);
  if (selected("static")) benchmark_static_file(1024 * 1024, 200 * scale);
  if (selected("echo")) benchmark_body_transfer(512 * 1024, 200 * scale);
  if (selected("routing")) benchmark_route_table(64, 20000 * scale);
  if (selected("memory-idle")) benchmark_idle_connections(64 * scale);
  if (selected("memory-stream"))
    benchmark_streaming_memory(16 * scale, 4 * 1024 * 1024);
  std::printf("\npeak resident after run %.1f MiB\n",
              static_cast<double>(peak_resident_bytes()) / 1048576.0);
  return 0;
}
