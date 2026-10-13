// Exercise transport fallback paths with bounded socket buffers, through libuv
// on all platforms. Peers deliberately do not read until a write is pending.
struct TransportPair {
  std::shared_ptr<chhttp::detail::Runtime> runtime =
      std::make_shared<chhttp::detail::Runtime>();
  std::shared_ptr<chhttp::detail::Listener> listener;
  std::shared_ptr<chhttp::detail::Connection> sender;
  std::shared_ptr<chhttp::detail::Connection> receiver;

  void connect() {
    std::promise<std::shared_ptr<chhttp::detail::Connection>> accepted;
    auto created = chhttp::detail::Listener::create(
        runtime, "127.0.0.1", 0, 16,
        [&](auto connection) { accepted.set_value(std::move(connection)); }, false);
    CHECK(created);
    listener = *created;
    auto connected = chhttp::detail::Connection::connect(
        runtime, "127.0.0.1", listener->port(), std::chrono::seconds(5)).get();
    CHECK(connected);
    sender = *connected;
    receiver = accepted.get_future().get();
  }

  // Called on the loop so no TCP handle is accessed across threads.
  void limit_buffers() {
    int size = 4096;
    CHECK(uv_send_buffer_size(reinterpret_cast<uv_handle_t *>(&sender->tcp_), &size) == 0);
    size = 4096;
    CHECK(uv_recv_buffer_size(reinterpret_cast<uv_handle_t *>(&receiver->tcp_), &size) == 0);
  }

  ~TransportPair() {
    if (sender) sender->close();
    if (receiver) receiver->close();
    if (listener) listener->close();
    runtime->stop();
  }
};

chhttp::Task<chhttp::ErrorInfo> write_transport_chunks(
    std::shared_ptr<chhttp::detail::Connection> connection, std::string payload) {
  for (std::size_t offset = 0; offset < payload.size(); offset += 16 * 1024) {
    auto error = co_await connection->write(payload.substr(offset, 16 * 1024),
                                            std::chrono::seconds(10));
    if (error) co_return error;
  }
  co_return chhttp::ErrorInfo{};
}

TEST(transport_partial_writes_preserve_bytes_and_reject_concurrent_writers) {
  TransportPair pair;
  pair.connect();
  std::string payload(4 * 1024 * 1024, '\0');
  for (std::size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<char>(i);
  auto run = [&]() -> chhttp::Task<void> {
    co_await chhttp::detail::resume_on(pair.runtime);
    pair.limit_buffers();
    auto writing = write_transport_chunks(pair.sender, payload);
    CHECK(!writing.ready());
    const auto concurrent = co_await pair.sender->write("must-not-appear", std::chrono::seconds(1));
    CHECK(concurrent.code == chhttp::Error::write);
    std::string received;
    while (received.size() < payload.size()) {
      auto chunk = co_await pair.receiver->read(std::chrono::seconds(10));
      CHECK(chunk && !chunk->eof);
      received.append(chunk->view());
    }
    CHECK(!(co_await std::move(writing)));
    CHECK(received == payload);
    CHECK(!(co_await pair.sender->write("next", std::chrono::seconds(1))));
    std::string next;
    while (next.size() < 4) {
      auto chunk = co_await pair.receiver->read(std::chrono::seconds(1));
      CHECK(chunk && !chunk->eof);
      next.append(chunk->view());
    }
    CHECK(next == "next");
  };
  run().get();
}

TEST(transport_pending_writes_keep_timeout_and_close_semantics) {
  for (bool close : {false, true}) {
    TransportPair pair;
    pair.connect();
    auto run = [&]() -> chhttp::Task<void> {
      co_await chhttp::detail::resume_on(pair.runtime);
      pair.limit_buffers();
      auto writing = pair.sender->write(std::string(4 * 1024 * 1024, 'x'),
                                         std::chrono::milliseconds(30));
      CHECK(!writing.ready());
      if (close) pair.sender->close();
      auto error = co_await std::move(writing);
      CHECK(error.code == (close ? chhttp::Error::cancelled : chhttp::Error::timeout));
    };
    run().get();
    // Synchronize after the timeout callback finishes closing its TCP handle.
    auto error = pair.sender->write("closed", std::chrono::seconds(1)).get();
    CHECK(error.code == chhttp::Error::write);
  }
}

TEST(transport_immediate_writes_yield_to_loop_timers) {
  for (bool chunked : {false, true}) {
    TransportPair pair;
    pair.connect();
    struct Probe {
      uv_timer_t timer{};
      std::shared_ptr<Probe> keep;
      std::size_t writes{0};
      std::optional<std::size_t> observed;
    };
    auto probe = std::make_shared<Probe>();
    auto run = [&]() -> chhttp::Task<void> {
      co_await chhttp::detail::resume_on(pair.runtime);
      CHECK(uv_timer_init(pair.runtime->loop(), &probe->timer) == 0);
      probe->keep = probe;
      probe->timer.data = probe.get();
      uv_timer_start(&probe->timer, [](uv_timer_t *timer) {
        auto *state = static_cast<Probe *>(timer->data);
        state->observed = state->writes;
        uv_close(reinterpret_cast<uv_handle_t *>(timer), [](uv_handle_t *handle) {
          static_cast<Probe *>(handle->data)->keep.reset();
        });
      }, 0, 0);
      // This small payload fits in the socket without any reader. A producer
      // must nevertheless return control to the loop before it finishes.
      while (probe->writes < 4096 && !probe->observed) {
        const auto error = chunked
            ? co_await chhttp::detail::write_chunk(pair.sender, "x", std::chrono::seconds(1))
            : co_await pair.sender->write("x", std::chrono::seconds(1));
        CHECK(!error);
        ++probe->writes;
      }
      CHECK(probe->observed && *probe->observed < 4096);
    };
    run().get();
  }
}

TEST(chunk_writes_own_frames_and_preserve_hex_boundaries_under_backpressure) {
  TransportPair pair;
  pair.connect();
  auto run = [&]() -> chhttp::Task<void> {
    co_await chhttp::detail::resume_on(pair.runtime);
    pair.limit_buffers();
    // Empty writes must succeed without touching a connection or terminating
    // the message, including with a non-positive timeout.
    CHECK(!(co_await chhttp::detail::write_chunk({}, {}, std::chrono::milliseconds(0))));
    const std::pair<std::size_t, std::string_view> cases[] = {
        {0, ""}, {1, "1"}, {15, "f"}, {16, "10"}, {17, "11"},
        {255, "ff"}, {256, "100"}, {4095, "fff"}, {4096, "1000"},
        {65535, "ffff"}, {65536, "10000"}, {65537, ""}, {131073, ""}};
    for (const auto &[size, hex] : cases) {
      std::string payload(size, '\0');
      for (std::size_t i = 0; i < size; ++i) payload[i] = static_cast<char>(i);
      std::string expected;
      if (size > 65536) {
        // The large-write cases have full 64 KiB chunks and a one-byte tail.
        for (std::size_t offset = 0; offset + 65536 <= size; offset += 65536)
          expected += "10000\r\n" + payload.substr(offset, 65536) + "\r\n";
        expected += "1\r\n" + payload.substr(size - 1) + "\r\n";
      } else if (size != 0) {
        expected = std::string(hex) + "\r\n" + payload + "\r\n";
      }
      auto writing = chhttp::detail::write_chunk(pair.sender, payload, std::chrono::seconds(5));
      if (size <= 65536) {
        // The single-frame path must own its payload before it returns its
        // transport Task, even when the peer has not started reading yet.
        std::string{}.swap(payload);
      }
      if (size == 65536) {
        CHECK(!writing.ready());
        auto concurrent = co_await chhttp::detail::write_chunk(
            pair.sender, "must-not-appear", std::chrono::seconds(1));
        CHECK(concurrent.code == chhttp::Error::write);
      }
      std::string wire;
      while (wire.size() < expected.size()) {
        auto chunk = co_await pair.receiver->read(std::chrono::seconds(5));
        CHECK(chunk && !chunk->eof);
        wire.append(chunk->view());
      }
      CHECK(!(co_await std::move(writing)));
      CHECK(wire == expected);
    }
    CHECK(!(co_await chhttp::detail::write_last_chunk(pair.sender, std::chrono::seconds(1))));
    std::string last;
    while (last.size() < 5) {
      auto chunk = co_await pair.receiver->read(std::chrono::seconds(1));
      CHECK(chunk && !chunk->eof);
      last.append(chunk->view());
    }
    CHECK(last == "0\r\n\r\n");
  };
  run().get();
}

TEST(chunk_writes_keep_timeout_and_close_errors_for_single_and_multiple_frames) {
  for (bool single_frame : {false, true}) {
    for (bool close : {false, true}) {
      TransportPair pair;
      pair.connect();
      auto run = [&]() -> chhttp::Task<void> {
        co_await chhttp::detail::resume_on(pair.runtime);
        pair.limit_buffers();
        const std::string payload(single_frame ? 65536 : 4 * 1024 * 1024, 'x');
        auto produce = [&]() -> chhttp::Task<chhttp::ErrorInfo> {
          // Fill the socket even on platforms that accept the first frame into
          // a kernel buffer. No peer reads, so a later frame must time out.
          for (unsigned i = 0; i < (single_frame ? 64u : 1u); ++i) {
            auto error = co_await chhttp::detail::write_chunk(
                pair.sender, payload, std::chrono::milliseconds(30));
            if (error) co_return error;
          }
          co_return chhttp::ErrorInfo{};
        };
        auto writing = produce();
        CHECK(!writing.ready());
        if (close) pair.sender->close();
        const auto error = co_await std::move(writing);
        CHECK(error.code == (close ? chhttp::Error::cancelled : chhttp::Error::timeout));
      };
      run().get();
      auto error = chhttp::detail::write_chunk(pair.sender, "closed", std::chrono::seconds(1)).get();
      CHECK(error.code == chhttp::Error::write);
    }
  }
}

TEST(base64_known_vectors_and_all_invalid_octets) {
  const std::array<std::pair<std::string_view, std::string_view>, 7> vectors{{
      {"", ""}, {"f", "Zg=="}, {"fo", "Zm8="}, {"foo", "Zm9v"},
      {"foob", "Zm9vYg=="}, {"fooba", "Zm9vYmE="}, {"foobar", "Zm9vYmFy"}}};
  for (auto [plain, encoded] : vectors) {
    CHECK(chhttp::detail::base64_encode(plain) == encoded);
    auto decoded = chhttp::detail::base64_decode(encoded);
    CHECK(decoded && *decoded == plain);
    for (std::size_t split = 0; split <= plain.size(); ++split) {
      chhttp::Base64Encoder encoder;
      const auto bytes = std::as_bytes(std::span(plain.data(), plain.size()));
      auto result = encoder.update(bytes.first(split));
      CHECK(encoder.update({}).empty());
      result += encoder.update(bytes.subspan(split));
      result += encoder.finish();
      CHECK(result == encoded);
    }
  }
  constexpr std::string_view alphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  for (unsigned byte = 0; byte < 256; ++byte) {
    if (alphabet.find(static_cast<char>(byte)) != std::string_view::npos) continue;
    for (std::size_t position = 0; position < 8; ++position) {
      std::string invalid = "AAAAAAAA";
      invalid[position] = static_cast<char>(byte);
      // '=' may terminate the last quartet, but is forbidden everywhere else.
      if (byte == '=' && position == 7) continue;
      CHECK(!chhttp::detail::base64_decode(invalid));
    }
  }
  for (const auto invalid : {"A", "AA", "AAA", "====", "A===", "AA=A", "AA==AAAA"})
    CHECK(!chhttp::detail::base64_decode(invalid));
}

TEST(buffered_chunk_size_fast_path_preserves_limits_and_pipeline) {
  TransportPair pair;
  pair.connect();
  auto run = [&]() -> chhttp::Task<void> {
    co_await chhttp::detail::resume_on(pair.runtime);
    for (const auto &size_line : {std::string("1;v=" + std::string(16380, 'x')),
                                std::string("1;v=" + std::string(16381, 'x')),
                                std::string("fffffffffffffffff"),
                                std::string("1;"), std::string("1;bad\nvalue")}) {
      chhttp::detail::ReadBuffer buffer;
      buffer.append("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" +
                    size_line + "\r\nx\r\n0\r\nX-End: yes\r\n\r\nnext");
      auto response = co_await chhttp::detail::read_response(pair.receiver, buffer, "GET", {});
      if (size_line.size() == 16384) {
        CHECK(response && response->body == "x" && response->headers.get("X-End") == "yes");
        CHECK(buffer.view() == "next");
      } else {
        CHECK(!response && response.error().code == chhttp::Error::protocol);
      }
    }
  };
  run().get();
}

TEST(stress_shutdown_completion_does_not_lose_wakeups) {
  for (int round = 0; round < 32; ++round) {
    chhttp::ServerOptions options;
    options.shutdown_timeout = std::chrono::seconds(3);
    chhttp::Server server(options);
    server.get_async("/", [](const auto &, auto &response) -> chhttp::Task<void> {
      response.body = "ok";
      co_return;
    });
    CHECK(server.start("127.0.0.1", 0));
    chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
    CHECK(client.get("/"));
    // Race idle-session completion against stop's condition-variable wait.
    // There is no handler work to drain; a full grace-period wait is a bug.
    auto stopping = std::async(std::launch::async, [&] { server.stop(); });
    const bool prompt = stopping.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    stopping.get();
    CHECK(prompt);
  }
}

TEST(buffered_http_headers_enforce_exact_limits_and_own_their_fields) {
  const std::string value(70 * 1024, 'x');
  for (bool request : {false, true}) {
    const std::string head = std::string(request ? "GET / HTTP/1.1\r\nHost: localhost\r\n"
                                               : "HTTP/1.1 204 No Content\r\n") +
                             "X-Large: " + value + "\r\n\r\n";
    for (bool valid : {false, true}) {
      chhttp::detail::ReadBuffer buffer;
      buffer.append_view(head);
      chhttp::detail::HttpReadOptions options;
      options.max_header_size = head.size() - (valid ? 0 : 1);
      if (request) {
        auto result = chhttp::detail::read_request_head({}, buffer, options).get();
        CHECK(static_cast<bool>(result) == valid);
        if (valid) CHECK(result->request.get_header("X-Large") == value && buffer.empty());
        else CHECK(result.error().code == chhttp::Error::protocol);
      } else {
        auto result = chhttp::detail::read_response({}, buffer, "GET", options).get();
        CHECK(static_cast<bool>(result) == valid);
        if (valid) CHECK(result->headers.get("X-Large") == value && buffer.empty());
        else CHECK(result.error().code == chhttp::Error::protocol);
      }
    }
  }
}

TEST(parsed_request_controls_preserve_duplicates_case_and_field_order) {
  // Each extension deliberately shares a length with a protocol control name.
  const std::string extensions =
      "Xxxx: extension\r\nXxxxxx: extension\r\nXxxxxxxxxx: extension\r\n"
      "Xxxxxxxxxxxxxx: extension\r\nXxxxxxxxxxxxxxxxx: extension\r\n";
  for (unsigned version : {10u, 11u}) {
    for (bool reversed : {false, true}) {
      for (bool duplicate : {false, true}) {
        for (bool close : {false, true}) {
          std::vector<std::string> fields{
              "hOsT: localhost\r\n", "cOnTeNt-LeNgTh: 4, 4\r\n",
              "CONTENT-LENGTH: 4\r\n", "eXpEcT: extension\r\n",
              "EXPECT: \t100-CoNtInUe, extension\t\r\n",
              "cOnNeCtIoN: upgrade, KEEP-ALIVE\r\n"};
          if (close) fields.push_back("CONNECTION: extension, cLoSe\r\n");
          if (reversed) std::reverse(fields.begin(), fields.end());
          std::string wire = "POST / HTTP/1." + std::to_string(version - 10) + "\r\n";
          for (const auto &field : fields) wire += field;
          wire += extensions + "\r\nbodyNEXT";
          chhttp::detail::ReadBuffer buffer;
          buffer.append(std::move(wire));
          auto parsed = chhttp::detail::read_request_head({}, buffer, {}, duplicate).get();
          CHECK(parsed);
          CHECK(parsed->request.keep_alive == !close);
          CHECK(parsed->body.content_length == 4 && !parsed->body.chunked);
          CHECK(parsed->body.expect_continue);
          CHECK(parsed->body.headers.size() == fields.size() + 5);
          CHECK(parsed->body.headers.get_all("Content-Length").size() == 2);
          CHECK(parsed->request.headers.size() == (duplicate ? fields.size() + 5 : 0));
          CHECK(buffer.view() == "bodyNEXT");
        }
      }
    }
  }
  for (const auto &fields : {
           std::string(""), std::string("Host: a\r\nhOsT: b\r\n"),
           std::string("Host: a,b\r\n"), std::string("Host:\r\n"),
           std::string("Host: a\r\nExpect: extension\r\n"),
           std::string("Host: a\r\nTransfer-Encoding: chunked\r\nContent-Length: 0\r\n"),
           std::string("Host: a\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n"),
           std::string("Host: a\r\nTransfer-Encoding: chunked\r\ntRaNsFeR-EnCoDiNg: chunked\r\n"),
           std::string("Host: a\r\nContent-Length: 4\r\nCONTENT-LENGTH: 5\r\n")}) {
    chhttp::detail::ReadBuffer buffer;
    buffer.append("POST / HTTP/1.1\r\n" + fields + "\r\n");
    auto parsed = chhttp::detail::read_request_head({}, buffer, {}).get();
    CHECK(!parsed && parsed.error().code == chhttp::Error::protocol);
  }
  chhttp::detail::ReadBuffer chunked;
  chunked.append("POST / HTTP/1.1\r\nhost: a\r\ntRaNsFeR-EnCoDiNg: ChUnKeD\r\n\r\n");
  auto parsed = chhttp::detail::read_request_head({}, chunked, {}).get();
  CHECK(parsed && parsed->body.chunked && !parsed->body.content_length);
}

TEST(parsed_response_controls_preserve_connection_tokens_and_host_scope) {
  for (unsigned version : {10u, 11u}) {
    for (const auto &connection : {std::string(""),
                                  std::string("Connection: extension\r\n"),
                                  std::string("CONNECTION: KeEp-AlIvE\r\n"),
                                  std::string("Connection: keep-alive\r\ncOnNeCtIoN: cLoSe\r\n"),
                                  std::string("Connection: close\r\ncOnNeCtIoN: keep-alive\r\n")}) {
      chhttp::detail::ReadBuffer buffer;
      buffer.append("HTTP/1." + std::to_string(version - 10) +
                    " 204 No Content\r\nHost:\r\nhOsT: a,b\r\n" + connection + "\r\nNEXT");
      auto parsed = chhttp::detail::read_response({}, buffer, "GET", {}).get();
      CHECK(parsed);
      CHECK(parsed->keep_alive ==
            (!chhttp::detail::has_token(parsed->headers, "Connection", "close") &&
             (version == 11 || chhttp::detail::has_token(parsed->headers, "Connection", "keep-alive"))));
      CHECK(parsed->headers.get_all("Host").size() == 2);
      CHECK(buffer.view() == "NEXT");
    }
  }
}

TEST(buffered_body_growth_preserves_fragmented_bytes_limits_and_pipeline) {
  TransportPair pair;
  pair.connect();
  auto run = [&]() -> chhttp::Task<void> {
    co_await chhttp::detail::resume_on(pair.runtime);
    for (const std::size_t size : {65535u, 65536u, 65537u, 524287u, 524288u, 524289u}) {
      std::string payload(size, '\0');
      for (std::size_t i = 0; i < size; ++i) payload[i] = static_cast<char>(i);
      for (bool streaming : {false, true}) {
        for (bool request : {false, true}) {
          const std::string next = request ? "GET /next HTTP/1.1\r\nHost: a\r\n\r\n"
                                           : "HTTP/1.1 204 No Content\r\n\r\n";
          const std::string head = request ? "POST / HTTP/1.1\r\nHost: a\r\n"
                                           : "HTTP/1.1 200 OK\r\n";
          chhttp::detail::ReadBuffer buffer;
          // One body byte follows the head; the rest arrives via TCP. This
          // exercises growth independently of how the OS splits read blocks.
          buffer.append(head + "Content-Length: " + std::to_string(size) + "\r\n\r\n" + payload.substr(0, 1));
          auto writing = pair.sender->write(payload.substr(1) + next, std::chrono::seconds(5));
          chhttp::detail::HttpReadOptions options;
          options.max_body_size = size;
          std::string delivered;
          if (streaming) options.on_data = [&](std::string_view data) {
            delivered.append(data);
            return true;
          };
          std::uint64_t progress = 0;
          options.on_progress = [&](const chhttp::TransferProgress &value) {
            CHECK(value.transferred > progress && value.transferred <= size);
            CHECK(value.total == size);
            progress = value.transferred;
            return true;
          };
          std::string body;
          if (request) {
            auto result = co_await chhttp::detail::read_request(pair.receiver, buffer, options);
            CHECK(result);
            body = std::move(result->body);
          } else {
            auto result = co_await chhttp::detail::read_response(pair.receiver, buffer, "GET", options);
            CHECK(result);
            body = std::move(result->body);
          }
          CHECK(progress == size);
          CHECK(streaming ? body.empty() && delivered == payload : body == payload);
          CHECK(!(co_await std::move(writing)));
          if (request) {
            auto result = co_await chhttp::detail::read_request_head(pair.receiver, buffer, {});
            CHECK(result && result->request.path == "/next");
          } else {
            auto result = co_await chhttp::detail::read_response(pair.receiver, buffer, "GET", {});
            CHECK(result && result->status == 204 && result->body.empty());
          }
          CHECK(buffer.empty());
        }
      }
    }
    for (bool cancelled : {false, true}) {
      chhttp::detail::ReadBuffer buffer;
      buffer.append("HTTP/1.1 200 OK\r\nContent-Length: 65537\r\n\r\nx");
      chhttp::detail::HttpReadOptions options;
      options.max_body_size = cancelled ? 65537 : 65536;
      options.cancelled = [cancelled] { return cancelled; };
      auto result = co_await chhttp::detail::read_response(pair.receiver, buffer, "GET", options);
      CHECK(!result && result.error().code ==
            (cancelled ? chhttp::Error::cancelled : chhttp::Error::body_too_large));
      CHECK(buffer.view() == "x");
    }
  };
  run().get();
}
