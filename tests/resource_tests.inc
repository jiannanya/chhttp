void check_pool_notification_handoff(int mode) {
  chhttp::Server server;
  std::atomic_bool release{false};
  server.get_async("/hold", [&](const auto &, auto &response) -> chhttp::Task<void> {
    while (!release) co_await chhttp::sleep_for(std::chrono::milliseconds(1));
    response.body = "released";
  });
  server.get_async("/", [](const auto &, auto &response) -> chhttp::Task<void> {
    response.body = "ok";
    co_return;
  });
  chhttp::Server barrier;
  barrier.get_async("/", [](const auto &, auto &) -> chhttp::Task<void> { co_return; });
  CHECK(server.start("127.0.0.1", 0));
  CHECK(barrier.start("127.0.0.1", 0));
  chhttp::ClientOptions options;
  options.max_connections_per_origin = 1;
  chhttp::AsyncClient client("http://127.0.0.1:" + std::to_string(server.port()), options);
  std::stop_source stop;
  auto legacy_stop = std::make_shared<std::atomic_bool>(false);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
  auto blocker = client.get("/hold", {}, {
      .on_response_head = [&](const auto &) {
        if (mode == 0) stop.request_stop();
        if (mode == 1) *legacy_stop = true;
        if (mode == 2) std::this_thread::sleep_until(deadline + std::chrono::milliseconds(1));
        return true;
      }});
  chhttp::RequestOptions waiting_options;
  if (mode == 0) waiting_options.stop_token = stop.get_token();
  if (mode == 1) waiting_options.cancellation = legacy_stop;
  if (mode == 2) waiting_options.deadline = deadline;
  auto cancelled = client.get("/", {}, waiting_options);
  auto next = client.get("/", {}, {.total_timeout = std::chrono::seconds(2)});
  // A request on another origin runs after the two queue registrations on the
  // shared client loop. Cancel the front waiter just before releasing its slot.
  const auto synchronized = client.get(
      "http://127.0.0.1:" + std::to_string(barrier.port()) + "/").get();
  release = true;
  const auto held = blocker.get();
  const auto stopped = cancelled.get();
  const auto result = next.get();
  CHECK(synchronized && held);
  CHECK(!stopped && stopped.error().code ==
      (mode == 2 ? chhttp::Error::timeout : chhttp::Error::cancelled));
  CHECK(result && result->body == "ok");
  CHECK(client.get("/").get());
}

TEST(pool_notification_survives_stop_token_cancellation) { check_pool_notification_handoff(0); }
TEST(pool_notification_survives_legacy_cancellation) { check_pool_notification_handoff(1); }
TEST(pool_notification_survives_deadline_expiration) { check_pool_notification_handoff(2); }

TEST(streaming_body_modes_release_replaced_buffered_payloads) {
  constexpr std::size_t payload_size = 2 * 1024 * 1024;
  chhttp::Request request;
  request.body.assign(payload_size, 'x');
  request.set_stream_body([](chhttp::StreamWriter &) -> chhttp::Task<void> { co_return; }, 0);
  CHECK(request.is_streaming_body() && request.body_stream_length == 0 && request.body.empty());
  CHECK(request.body.capacity() < payload_size);

  chhttp::Response response;
  response.body.assign(payload_size, 'y');
  response.set_stream("text/plain", [](chhttp::StreamWriter &) -> chhttp::Task<void> { co_return; });
  CHECK(response.is_streaming() && response.body.empty());
  CHECK(response.body.capacity() < payload_size);
  response.set_content(std::string(payload_size, 'z'));
  CHECK(!response.is_streaming() && response.body.size() == payload_size);
  response.set_file("payload.bin");
  CHECK(!response.is_streaming() && response.body.empty());
  CHECK(response.body.capacity() < payload_size);
}

TEST(http_framing_preserves_duplicate_lengths_and_rejects_ambiguous_lists) {
  const std::array<std::string_view, 4> valid{
      "Content-Length: 42\r\n", "content-length: 042,\t42 , 00042\r\n",
      "Content-Length: 42\r\nCONTENT-LENGTH: 0042\r\n",
      "Content-Length: \t 42 \t\r\nContent-Length: 042,42\r\n"};
  for (const auto fields : valid) {
    chhttp::detail::ReadBuffer buffer;
    buffer.append("POST / HTTP/1.1\r\nHost: localhost\r\n" + std::string(fields) + "\r\nnext");
    auto result = chhttp::detail::read_request_head({}, buffer, {}).get();
    CHECK(result && result->body.content_length == 42 && !result->body.chunked);
    CHECK(buffer.view() == "next");
    CHECK(result->request.headers.get_all("Content-Length") ==
          result->body.headers.get_all("Content-Length"));
  }
  const std::array<std::string_view, 16> invalid{
      "Content-Length: \r\n", "Content-Length: ,42\r\n", "Content-Length: 42,\r\n",
      "Content-Length: 42,,42\r\n", "Content-Length: 42, \t,42\r\n",
      "Content-Length: 42\r\nContent-Length: 43\r\n",
      "Content-Length: 18446744073709551616\r\n",
      "Content-Length: +42\r\n", "Content-Length: 4 2\r\n",
      "Transfer-Encoding: chunked,\r\n", "Transfer-Encoding: ,chunked\r\n",
      "Transfer-Encoding: chunked\r\nTransfer-Encoding: CHUNKED\r\n",
      "Transfer-Encoding: gzip, chunked\r\n", "Transfer-Encoding: chunked; q=1\r\n",
      "Transfer-Encoding: chunked\r\nContent-Length: 0\r\n",
      "Content-Length: 0\r\nTransfer-Encoding: chunked\r\n"};
  for (const auto fields : invalid) {
    chhttp::detail::ReadBuffer buffer;
    buffer.append("POST / HTTP/1.1\r\nHost: localhost\r\n" + std::string(fields) + "\r\n");
    auto result = chhttp::detail::read_request_head({}, buffer, {}).get();
    CHECK(!result && result.error().code == chhttp::Error::protocol);
  }
}

TEST(http_request_head_owns_metadata_and_preserves_pipeline_bytes) {
  const std::string large_value(4096, 'm');
  chhttp::detail::ReadBuffer buffer;
  buffer.append("GET /first?q=one HTTP/1.1\r\nhOsT: \tlocalhost \t\r\n"
                "X-Metadata: " + large_value + "\r\nX-Metadata: second\r\n\r\n"
                "GET /second HTTP/1.0\r\nConnection: keep-alive\r\n\r\n");
  auto first = chhttp::detail::read_request_head({}, buffer, {}).get();
  auto second = chhttp::detail::read_request_head({}, buffer, {}).get();
  CHECK(first && second && buffer.empty());
  CHECK(first->request.path == "/first" && first->request.get_param("q") == "one");
  CHECK(second->request.path == "/second" && second->request.keep_alive);
  CHECK(first->request.headers.get_all("X-Metadata") ==
        std::vector<std::string>({large_value, "second"}));
  first->request.headers.set("X-Metadata", "changed");
  CHECK(first->body.headers.get("X-Metadata") == large_value);
  for (const auto host : {"", "Host: \t \r\n", "Host: a,b\r\n", "Host: a\r\nHOST: b\r\n"}) {
    buffer.append("GET / HTTP/1.1\r\n" + std::string(host) + "X-Test: a\r\n\r\n");
    auto invalid = chhttp::detail::read_request_head({}, buffer, {}).get();
    CHECK(!invalid && invalid.error().code == chhttp::Error::protocol);
  }
}

TEST(http_response_head_snapshot_survives_trailers_and_buffer_reuse) {
  const std::string metadata(8192, 'h');
  RawResponseServer server("HTTP/1.1 103 Early Hints\r\nX-Hint: ignored\r\n\r\n"
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: ChUnKeD\r\nX-Metadata: " + metadata +
      "\r\nX-Value: first\r\nX-Value: second\r\nConnection: close\r\n\r\n"
      "3\r\nabc\r\n0\r\nX-Value: trailer\r\n\r\n", true);
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  std::optional<chhttp::ResponseHead> snapshot;
  std::size_t heads = 0;
  auto result = client.get("/", {}, {.on_response_head = [&](const auto &head) {
    snapshot = head;
    ++heads;
    return true;
  }});
  CHECK(result && result->body == "abc" && heads == 1 && snapshot);
  CHECK(snapshot->headers.get("X-Metadata") == metadata && !snapshot->headers.contains("X-Hint"));
  CHECK(snapshot->headers.get_all("X-Value") == std::vector<std::string>({"first", "second"}));
  CHECK(result->headers.get_all("X-Value") == std::vector<std::string>({"first", "second", "trailer"}));
  result->headers.set("X-Metadata", "modified");
  CHECK(snapshot->headers.get("X-Metadata") == metadata);
}

TEST(property_http_content_length_lists_match_numeric_model) {
  std::mt19937_64 random(property_seed ^ 0x424f4459);
  for (property_case = 0; property_case < 1500; ++property_case) {
    const auto expected = random() % 100000;
    const bool conflict = random() % 3 == 0;
    std::string wire = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: " +
                       std::to_string(expected);
    const auto duplicates = 1 + random() % 12;
    for (std::size_t i = 0; i < duplicates; ++i) {
      wire += random() % 2 ? ", \t" : "\r\ncOnTeNt-LeNgTh: ";
      wire += std::string(random() % 5, '0');
      wire += std::to_string(expected + (conflict && i + 1 == duplicates ? 1 : 0));
      wire += " \t";
    }
    wire += "\r\n\r\n";
    chhttp::detail::ReadBuffer buffer;
    buffer.append(std::move(wire));
    auto result = chhttp::detail::read_request_head({}, buffer, {}).get();
    if (conflict) CHECK(!result && result.error().code == chhttp::Error::protocol);
    else CHECK(result && result->body.content_length == expected);
  }
}

TEST(stress_pool_mixed_cancellation_deadlines_and_origin_isolation) {
  std::atomic_bool release{false};
  chhttp::Server first, second;
  for (auto *server : {&first, &second}) {
    server->get_async("/hold", [&](const auto &, auto &response) -> chhttp::Task<void> {
      while (!release) co_await chhttp::sleep_for(std::chrono::milliseconds(1));
      response.body = "held";
    });
    server->get_async("/{id}", [](const auto &request, auto &response) -> chhttp::Task<void> {
      response.body = request.path_params.at("id");
      co_return;
    });
    CHECK(server->start("127.0.0.1", 0));
  }
  const std::array<std::string, 2> origins{
      "http://127.0.0.1:" + std::to_string(first.port()),
      "http://127.0.0.1:" + std::to_string(second.port())};
  chhttp::ClientOptions options;
  options.max_connections_per_origin = 1;
  chhttp::AsyncClient client(origins[0], options);
  auto held_first = client.get(origins[0] + "/hold");
  auto held_second = client.get(origins[1] + "/hold");
  constexpr std::size_t count = 256;
  std::vector<std::stop_source> stops(count);
  std::vector<chhttp::Task<chhttp::ResponseResult>> pending;
  for (std::size_t i = 0; i < count; ++i) {
    chhttp::RequestOptions request_options;
    request_options.total_timeout = std::chrono::seconds(3);
    if (i % 3 == 0) request_options.stop_token = stops[i].get_token();
    if (i % 3 == 1) request_options.total_timeout = std::chrono::milliseconds(20);
    pending.push_back(client.get(origins[i % 2] + "/" + std::to_string(i), {}, request_options));
  }
  for (std::size_t i = count; i != 0; --i) if ((i - 1) % 3 == 0) stops[i - 1].request_stop();
  // Wait for actual deadline completions before allowing either origin to drain.
  bool expired = true;
  for (std::size_t i = 1; i < count; i += 3) {
    const auto result = pending[i].get();
    expired = expired && !result && result.error().code == chhttp::Error::timeout;
  }
  release = true;
  CHECK(held_first.get() && held_second.get() && expired);
  for (std::size_t i = 0; i < count; ++i) {
    if (i % 3 == 1) continue;
    const auto result = pending[i].get();
    if (i % 3 == 0) CHECK(!result && result.error().code == chhttp::Error::cancelled);
    else CHECK(result && result->body == std::to_string(i));
  }
  for (const auto &origin : origins) {
    auto recovered = client.get(origin + "/recovered").get();
    CHECK(recovered && recovered->body == "recovered");
  }
}
