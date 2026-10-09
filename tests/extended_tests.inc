// Included inside test_main.cpp's test namespace to share the socket fixtures.

TEST(relative_url_resolution_preserves_path_query_and_fragment_boundaries) {
  const std::pair<std::string_view, std::string_view> cases[]{
      {".", "/b/c/"}, {"..", "/b/"}, {"../..", "/"},
      {"g/.", "/b/c/g/"}, {"g/..", "/b/c/"}, {"g//h", "/b/c/g//h"},
      {"/x//y/../z", "/x//z"}, {"g/?q", "/b/c/g/?q"},
      {"g?y/./x", "/b/c/g?y/./x"}, {"g#s/../x", "/b/c/g"},
      {"?next=https://other.test/a", "/b/c/d;p?next=https://other.test/a"},
      {"g?next=https://other.test/a", "/b/c/g?next=https://other.test/a"},
      {"g%2Fh", "/b/c/g%2Fh"}, {"%2e%2e/g", "/b/c/%2e%2e/g"}};
  for (const auto &[reference, expected] : cases) {
    auto result = chhttp::detail::parse_url(reference, "http://a/b/c/d;p?q");
    CHECK(result && result->host == "a" && result->target == expected);
  }
}

TEST(relative_redirects_keep_double_slashes_and_url_valued_queries) {
  chhttp::Server server;
  server.get("/start", [](const auto &, auto &response) {
    response.set_redirect("/path//leaf/?next=https://other.test/a#ignored");
  });
  server.get("/path//leaf/", [](const auto &request, auto &response) {
    response.body = request.target;
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  auto response = client.get("/start");
  CHECK(response && response->body == "/path//leaf/?next=https://other.test/a");
}

TEST(compression_quality_values_obey_http_decimal_grammar) {
  for (const std::string_view invalid : {"1e0", "0.0001", "1.001", "+1", "-0", ".5",
                                         "NaN", "inf", "", "2", "01", "0.5junk"}) {
    CHECK(chhttp::detail::select_encoding("gzip;q=" + std::string(invalid)).empty());
  }
  for (const std::string_view valid : {"0.001", "0.5", "1", "1.000", "1.", "0.999"})
    CHECK(chhttp::detail::select_encoding("GZIP ; Q=" + std::string(valid)) == "gzip");
  CHECK(chhttp::detail::select_encoding("gzip;q=0.8;q=1").empty());
  CHECK(chhttp::detail::select_encoding("gzip;q=0, *;q=0.1") == "br");
  CHECK(chhttp::detail::select_encoding("gzip;q=0.9, br;q=0.8, zstd;q=0.7") == "gzip");
}

TEST(file_ranges_ignore_unknown_units_and_accept_case_insensitive_bytes) {
  Round2TestDirectory directory;
  const auto path = directory.path / "range.bin";
  std::ofstream(path, std::ios::binary) << "0123456789";
  chhttp::Server server;
  server.get("/", [&](const auto &, auto &response) { response.set_file(path); });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  auto unknown = client.get("/", {{"Range", "records=1-2"}});
  CHECK(unknown && unknown->status == 200 && unknown->body == "0123456789");
  auto mixed_case = client.get("/", {{"Range", "ByTeS=1-2"}});
  CHECK(mixed_case && mixed_case->status == 206 && mixed_case->body == "12");
}

TEST(file_if_range_requires_matching_strong_validator) {
  Round2TestDirectory directory;
  const auto path = directory.path / "range.bin";
  std::ofstream(path, std::ios::binary) << "0123456789";
  chhttp::Server server;
  server.get("/", [&](const auto &, auto &response) {
    response.set_file(path);
    response.headers.set("ETag", "\"current\"");
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  for (std::string_view validator : {"\"stale\"", "W/\"current\"", "", "garbage",
                                    "Wed, 21 Oct 2015 07:28:00 GMT"}) {
    auto response = client.get("/", {{"Range", "bytes=2-4"}, {"If-Range", std::string(validator)}});
    CHECK(response && response->status == 200 && response->body == "0123456789");
  }
  auto matched = client.get("/", {{"Range", "bytes=2-4"}, {"If-Range", "\"current\""}});
  CHECK(matched && matched->status == 206 && matched->body == "234");
}

TEST(rfc3986_http_reference_resolution_examples) {
  const std::pair<std::string_view, std::string_view> cases[]{
      {"g", "/b/c/g"}, {"./g", "/b/c/g"}, {"g/", "/b/c/g/"}, {"/g", "/g"},
      {"?y", "/b/c/d;p?y"}, {"g?y", "/b/c/g?y"}, {"#s", "/b/c/d;p?q"},
      {"g#s", "/b/c/g"}, {"g?y#s", "/b/c/g?y"}, {";x", "/b/c/;x"},
      {"g;x", "/b/c/g;x"}, {"g;x?y#s", "/b/c/g;x?y"}, {"", "/b/c/d;p?q"},
      {"./", "/b/c/"}, {"../", "/b/"}, {"../g", "/b/g"}, {"../../", "/"},
      {"../../g", "/g"}, {"../../../g", "/g"}, {"../../../../g", "/g"},
      {"/./g", "/g"}, {"/../g", "/g"}, {"g.", "/b/c/g."}, {".g", "/b/c/.g"},
      {"g..", "/b/c/g.."}, {"..g", "/b/c/..g"}, {"./../g", "/b/g"},
      {"./g/.", "/b/c/g/"}, {"g/./h", "/b/c/g/h"}, {"g/../h", "/b/c/h"},
      {"g;x=1/./y", "/b/c/g;x=1/y"}, {"g;x=1/../y", "/b/c/y"},
      {"g?y/../x", "/b/c/g?y/../x"}, {"g#s/./x", "/b/c/g"}};
  for (const auto &[reference, target] : cases) {
    auto result = chhttp::detail::parse_url(reference, "http://a/b/c/d;p?q");
    CHECK(result && result->target == target);
  }
  auto other = chhttp::detail::parse_url("//other.test/x", "https://a/base");
  CHECK(other && other->host == "other.test" && other->secure && other->target == "/x");
  for (auto reference : {"ftp://a/x", "data:text/plain,hello", "http:g", "mailto:a@b"})
    CHECK(!chhttp::detail::parse_url(reference, "http://a/base"));
}

TEST(request_trailers_cannot_inject_control_or_authentication_fields) {
  chhttp::Server server;
  std::atomic_int accepted{0};
  server.post("/", [&](const auto &, auto &response) { ++accepted; response.body = "ok"; });
  CHECK(server.start("127.0.0.1", 0));
  for (std::string_view field : {"Authorization", "cOoKiE", "Host", "Connection", "Content-Type",
                                 "Content-Encoding", "Transfer-Encoding", "Content-Length", "Range", "If-Range"}) {
    const auto request =
        "POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
        "1\r\nx\r\n0\r\n" + std::string(field) + ": injected\r\n\r\n";
    const auto response = raw_http_exchange(server.port(), request);
    CHECK(response.starts_with("HTTP/1.1 400"));
    const auto fragmented = raw_http_exchange(server.port(), request, 3);
    // Rejection can close the socket before the final CRLF arrives. A TCP
    // reset may discard the error response; it must never dispatch the route.
    CHECK(fragmented.empty() || fragmented.starts_with("HTTP/1.1 400"));
    CHECK(accepted == 0);
  }
  CHECK(accepted == 0);
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  auto recovered = client.post("/", "healthy", "text/plain");
  CHECK(recovered && recovered->body == "ok" && accepted == 1);
}

TEST(response_trailers_cannot_change_redirect_cookie_or_encoding_semantics) {
  for (std::string_view field : {"Location", "Set-Cookie", "WWW-Authenticate", "Proxy-Authenticate",
                                 "Upgrade", "TE", "Trailer", "Content-Encoding", "Content-Range"}) {
    auto response = fetch_raw_response("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
        "Connection: close\r\n\r\n1\r\nx\r\n0\r\n" + std::string(field) + ": invalid\r\n\r\n", true);
    CHECK(!response && response.error().code == chhttp::Error::protocol);
  }
}

TEST(control_byte_header_mutation_corpus_is_rejected_without_losing_availability) {
  chhttp::Server server;
  server.get("/", [](const auto &, auto &response) { response.body = "healthy"; });
  CHECK(server.start("127.0.0.1", 0));
  for (int byte = 0; byte < 32; ++byte) {
    if (byte == '\t' || byte == '\r' || byte == '\n') continue;
    const auto response = raw_http_exchange(server.port(),
        "GET / HTTP/1.1\r\nHost: localhost\r\nX-Test: before" + std::string(1, static_cast<char>(byte)) +
        "after\r\nConnection: close\r\n\r\n", 5);
    CHECK(response.starts_with("HTTP/1.1 400"));
  }
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  auto healthy = client.get("/");
  CHECK(healthy && healthy->body == "healthy");
}

TEST(truncated_chunked_response_corpus_never_reports_success) {
  for (std::string_view suffix : {"", "1", "1\r", "1\r\n", "2\r\nx", "1\r\nx", "1\r\nx\r",
                                  "1\r\nx\r\n", "0\r\n", "0\r\nX-Final: missing"}) {
    auto result = fetch_raw_response("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
                                     "Connection: close\r\n\r\n" + std::string(suffix), true);
    CHECK(!result);
  }
}

TEST(sse_reset_recovers_from_limits_and_callback_exceptions) {
  chhttp::SseParser parser({.max_line_size = 32, .max_event_size = 64, .last_event_id = "initial"});
  CHECK(parser.feed(std::string(33, 'x')).code == chhttp::Error::body_too_large);
  CHECK(parser.feed("data: ignored\n\n"));
  parser.reset();
  CHECK(parser.last_event_id() == "initial" && !parser.retry());
  parser.on_message([](const auto &) { throw std::runtime_error("callback"); });
  CHECK(parser.feed("data: x\n\n").code == chhttp::Error::internal);
  parser.reset();
  std::string data;
  parser.on_message([&](const auto &event) { data = event.data; });
  CHECK(!parser.feed("data: recovered\n\n") && data == "recovered");
}

TEST(sse_large_incomplete_events_are_discarded_before_parser_reuse) {
  chhttp::SseParser parser;
  std::vector<std::string> delivered;
  parser.on_message([&](const auto &event) { delivered.push_back(event.data); });
  for (int round = 0; round < 16; ++round) {
    CHECK(!parser.feed("data: " + std::string(256 * 1024, 'x')));
    CHECK(!parser.finish());
    parser.reset();
    CHECK(!parser.feed("data: small\n\n"));
  }
  CHECK(delivered.size() == 16);
  CHECK(std::ranges::all_of(delivered, [](const auto &value) { return value == "small"; }));
}

TEST(cancel_from_response_callback_does_not_deadlock_or_cancel_future_requests) {
  chhttp::Server server;
  server.get("/", [](const auto &, auto &response) { response.body = "ok"; });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::AsyncClient client("http://127.0.0.1:" + std::to_string(server.port()));
  auto cancelled = client.get("/", {}, {.on_response_head = [&](const auto &) {
    client.cancel();
    return true;
  }}).get();
  CHECK(!cancelled && cancelled.error().code == chhttp::Error::cancelled);
  auto next = client.get("/").get();
  CHECK(next && next->body == "ok");
}

TEST(expired_stop_callbacks_do_not_affect_reused_connections) {
  chhttp::Server server;
  server.get("/", [](const auto &, auto &response) { response.body = "ok"; });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::AsyncClient client("http://127.0.0.1:" + std::to_string(server.port()));
  std::vector<std::stop_source> completed_tokens(128);
  for (auto &source : completed_tokens) {
    auto response = client.get("/", {}, {.stop_token = source.get_token()}).get();
    CHECK(response && response->body == "ok");
  }
  for (auto &source : completed_tokens) source.request_stop();
  auto response = client.get("/").get();
  CHECK(response && response->body == "ok");
}

TEST(async_client_move_assignment_cancels_old_inflight_requests) {
  chhttp::Server server;
  server.get_async("/slow", [](const auto &, auto &response) -> chhttp::Task<void> {
    co_await chhttp::sleep_for(std::chrono::milliseconds(40)); response.body = "slow";
  });
  server.get("/", [](const auto &, auto &response) { response.body = "ok"; });
  CHECK(server.start("127.0.0.1", 0));
  const auto base = "http://127.0.0.1:" + std::to_string(server.port());
  chhttp::AsyncClient destination(base), source(base);
  auto pending = destination.get("/slow");
  destination = std::move(source);
  auto cancelled = pending.get();
  CHECK(!cancelled && cancelled.error().code == chhttp::Error::cancelled);
  auto empty = source.get("/").get();
  CHECK(!empty && empty.error().code == chhttp::Error::internal);
  auto good = destination.get("/").get();
  CHECK(good && good->body == "ok");
}

std::string websocket_upgrade_request() {
  return "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
      "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
}

TEST(websocket_malformed_frame_corpus_is_rejected_and_server_recovers) {
  chhttp::Server server;
  std::atomic_int rejected{0};
  server.websocket("/", [&](const auto &, auto &socket) -> chhttp::Task<void> {
    auto message = co_await socket.read();
    if (!message) ++rejected;
    else co_await socket.send_text(message->data);
  });
  CHECK(server.start("127.0.0.1", 0));
  const std::vector<std::string> invalid{
      masked_websocket_frame(0xc1, "x"), // unsupported RSV
      std::string("\x81\x01x", 3), // unmasked client
      masked_websocket_frame(0x80, "x"), // continuation without first fragment
      masked_websocket_frame(0x83, "x"), // reserved opcode
      masked_websocket_frame(0x09, "x"), // fragmented ping
      masked_websocket_frame(0x88, "x"), // one-byte close code
      masked_websocket_frame(0x81, std::string("\xc0\xaf", 2)), // overlong UTF-8
      masked_websocket_frame(0x81, std::string("\xed\xa0\x80", 3)), // surrogate
      masked_websocket_frame(0x01, "a") + masked_websocket_frame(0x81, "b")};
  for (const auto &frame : invalid) {
    const auto response = raw_http_exchange(server.port(), websocket_upgrade_request() + frame);
    CHECK(response.starts_with("HTTP/1.1 101"));
  }
  CHECK(rejected == static_cast<int>(invalid.size()));
  auto connected = chhttp::AsyncWebSocketClient::connect("ws://127.0.0.1:" + std::to_string(server.port()) + "/").get();
  CHECK(connected && (*connected)->send_text("healthy").get());
  auto echoed = (*connected)->read().get();
  CHECK(echoed && echoed->data == "healthy");
  (*connected)->close().get();
}

TEST(websocket_utf8_codepoint_can_span_fragments_with_interleaved_ping) {
  chhttp::Server server;
  server.websocket("/", [](const auto &, auto &socket) -> chhttp::Task<void> {
    while (auto message = co_await socket.read()) {
      if (message->type == chhttp::WebSocket::MessageType::text) {
        co_await socket.send_text(message->data);
        break;
      }
    }
  });
  CHECK(server.start("127.0.0.1", 0));
  const auto frames = masked_websocket_frame(0x01, std::string("\xe2", 1)) +
      masked_websocket_frame(0x89, "ping") + masked_websocket_frame(0x80, std::string("\x82\xac", 2));
  const auto response = raw_http_exchange(server.port(), websocket_upgrade_request() + frames, 1);
  CHECK(response.find("\xe2\x82\xac") != std::string::npos && response.find("ping") != std::string::npos);
}

TEST(stress_request_registration_mixes_invalid_cancelled_and_successful_requests) {
  chhttp::Server server;
  server.get("/", [](const auto &, auto &response) { response.body = "ok"; });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::ClientOptions options;
  options.max_connections_per_origin = 4;
  chhttp::AsyncClient client("http://127.0.0.1:" + std::to_string(server.port()), options);
  std::atomic_int failures{0};
  std::vector<std::jthread> producers;
  for (int thread = 0; thread < 4; ++thread) {
    producers.emplace_back([&] {
      for (int round = 0; round < 120; ++round) {
        std::stop_source source;
        if (round % 3 == 0) source.request_stop();
        auto response = client.get(round % 3 == 1 ? "http://invalid:0/" : "/", {},
                                    {.stop_token = source.get_token()}).get();
        if (round % 3 == 0 && (response || response.error().code != chhttp::Error::cancelled)) ++failures;
        if (round % 3 == 1 && (response || response.error().code != chhttp::Error::invalid_url)) ++failures;
        if (round % 3 == 2 && (!response || response->body != "ok")) ++failures;
      }
    });
  }
  producers.clear(); // jthread joins before the client-wide cancellation.
  client.cancel();
  auto recovered = client.get("/").get();
  CHECK(failures == 0 && recovered && recovered->body == "ok");
}

TEST(stress_queued_cancellation_permutations_release_every_origin_slot) {
  chhttp::Server server;
  std::atomic_bool release{false};
  std::atomic_int entered{0};
  server.get_async("/", [&](const auto &, auto &response) -> chhttp::Task<void> {
    ++entered;
    while (!release) co_await chhttp::sleep_for(std::chrono::milliseconds(1));
    response.body = "ok";
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::ClientOptions options;
  options.max_connections_per_origin = 1;
  chhttp::AsyncClient client("http://127.0.0.1:" + std::to_string(server.port()), options);
  auto blocker = client.get("/");
  std::vector<std::stop_source> sources(300);
  std::vector<chhttp::Task<chhttp::ResponseResult>> tasks;
  for (auto &source : sources) tasks.push_back(client.get("/", {}, {.stop_token = source.get_token()}));
  for (std::size_t i = 0; i < sources.size(); ++i) sources[(i * 137) % sources.size()].request_stop();
  release = true;
  CHECK(blocker.get());
  for (auto &task : tasks) {
    const auto result = task.get();
    CHECK(!result && result.error().code == chhttp::Error::cancelled);
  }
  auto next = client.get("/").get();
  CHECK(next && next->body == "ok");
}
