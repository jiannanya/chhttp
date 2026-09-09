// Deterministic property tests. Every failure prints its seed and case index.
std::uint64_t property_seed = 0x434854545033ULL;
std::size_t property_case = 0;

std::string random_bytes(std::mt19937_64 &random, std::size_t size) {
  std::string value(size, '\0');
  for (auto &byte : value) byte = static_cast<char>(random() & 255);
  return value;
}

TEST(property_binary_url_and_query_round_trips) {
  std::mt19937_64 random(property_seed ^ 1);
  for (property_case = 0; property_case < 1000; ++property_case) {
    auto bytes = random_bytes(random, random() % 1025);
    for (bool plus : {false, true}) {
      auto encoded = chhttp::url_encode(bytes, plus);
      auto decoded = chhttp::url_decode(encoded, plus);
      CHECK(decoded && *decoded == bytes);
    }
    const auto params = chhttp::parse_query("key=" + chhttp::url_encode(bytes, true) + "&key=second&flag");
    CHECK(params.size() == 3 && params[0].second == bytes && params[1].second == "second");
  }
}

TEST(property_base64_random_chunking_matches_one_shot_encoding) {
  std::mt19937_64 random(property_seed ^ 2);
  chhttp::Base64Encoder encoder;
  for (property_case = 0; property_case < 300; ++property_case) {
    auto bytes = random_bytes(random, property_case < 10 ? property_case : random() % 8193);
    encoder.reset();
    std::string encoded;
    for (std::size_t offset = 0; offset < bytes.size();) {
      const auto count = (std::min)(bytes.size() - offset, std::size_t{1} + random() % 97);
      encoded += encoder.update(std::as_bytes(std::span(bytes.data() + offset, count)));
      offset += count;
    }
    encoded += encoder.finish();
    CHECK(encoder.finish().empty());
    CHECK(encoded == chhttp::detail::base64_encode(bytes));
    CHECK(chhttp::base64_encoded_size(bytes.size()) == encoded.size());
    auto decoded = chhttp::detail::base64_decode(encoded);
    CHECK(decoded && *decoded == bytes);
  }
}

TEST(property_headers_match_ordered_reference_model) {
  std::mt19937_64 random(property_seed ^ 3);
  chhttp::Headers headers;
  std::vector<std::pair<std::string, std::string>> model;
  for (property_case = 0; property_case < 5000; ++property_case) {
    const auto key = "x-" + std::to_string(random() % 8);
    auto name = key;
    if (random() % 2) name[0] = 'X';
    const auto value = std::to_string(random());
    switch (random() % 3) {
    case 0:
      headers.add(name, value); model.emplace_back(name, value); break;
    case 1:
      headers.set(name, value);
      std::erase_if(model, [&](const auto &item) { return chhttp::detail::iequals(item.first, key); });
      model.emplace_back(name, value);
      break;
    default:
      headers.erase(name);
      std::erase_if(model, [&](const auto &item) { return chhttp::detail::iequals(item.first, key); });
    }
    CHECK(headers.size() == model.size());
    CHECK(std::equal(headers.begin(), headers.end(), model.begin()));
    std::vector<std::string> expected;
    for (const auto &[entry, data] : model)
      if (chhttp::detail::iequals(entry, key)) expected.push_back(data);
    CHECK(headers.get_all(key) == expected);
  }
}

TEST(property_read_buffer_matches_byte_string_after_moves_and_consumption) {
  std::mt19937_64 random(property_seed ^ 4);
  chhttp::detail::ReadBuffer buffer;
  std::string model;
  for (property_case = 0; property_case < 5000; ++property_case) {
    switch (random() % 5) {
    case 0: {
      auto data = random_bytes(random, random() % 1024);
      model += data;
      buffer.append(std::move(data));
      break;
    }
    case 1: {
      auto data = random_bytes(random, random() % 257);
      model += data;
      buffer.append_view(data);
      break;
    }
    case 2: {
      const auto count = random() % (model.size() + 1);
      buffer.consume(count); model.erase(0, count); break;
    }
    case 3: {
      auto moved = std::move(buffer);
      buffer = std::move(moved);
      break;
    }
    default: if (random() % 8 == 0) { buffer.clear(); model.clear(); }
    }
    CHECK(buffer.view() == model && buffer.size() == model.size());
    const char needle = static_cast<char>(random());
    CHECK(buffer.find(needle) == model.find(needle));
  }
}

TEST(property_sse_random_splits_preserve_multiline_utf8_and_metadata) {
  std::mt19937_64 random(property_seed ^ 5);
  std::vector<chhttp::SseEvent> expected;
  std::string wire = "\xEF\xBB\xBF: comment\r\n";
  for (property_case = 0; property_case < 200; ++property_case) {
    chhttp::SseEvent event;
    event.data = "\xE4\xBD\xA0\xE5\xA5\xBD\n" + std::to_string(random()) + "\n";
    event.id = std::to_string(property_case);
    event.event = property_case % 2 ? "delta" : "message";
    event.retry = std::chrono::milliseconds(random() % 10000);
    wire += chhttp::format_sse(event);
    expected.push_back(std::move(event));
  }
  for (property_case = 0; property_case < 40; ++property_case) {
    std::size_t delivered = 0;
    chhttp::SseParser parser;
    parser.on_message([&](const auto &event) {
      CHECK(delivered < expected.size());
      const auto &reference = expected[delivered++];
      CHECK(event.data == reference.data && event.id == reference.id &&
            event.event == reference.event && event.retry == reference.retry);
    });
    for (std::size_t offset = 0; offset < wire.size();) {
      const auto count = (std::min)(wire.size() - offset, std::size_t{1} + random() % 151);
      CHECK(!parser.feed(std::string_view(wire).substr(offset, count)));
      offset += count;
    }
    CHECK(!parser.finish() && delivered == expected.size());
  }
}

TEST(property_multipart_random_forms_and_splits_preserve_ordered_parts) {
  std::mt19937_64 random(property_seed ^ 6);
  for (property_case = 0; property_case < 200; ++property_case) {
    chhttp::MultipartForm form;
    const auto count = 1 + random() % 8;
    for (std::size_t part = 0; part < count; ++part)
      form.push_back({.name = "name" + std::to_string(part), .filename = "a\"b.bin",
                      .content_type = "application/octet-stream",
                      .content = random_bytes(random, random() % 2049) + "\r\n--randomX"});
    auto [type, wire] = chhttp::make_multipart(form, "random");
    chhttp::MultipartParser parser(type);
    std::vector<std::string> decoded(count);
    std::size_t active = 0;
    bool begun = false;
    for (std::size_t offset = 0; offset < wire.size();) {
      const auto size = (std::min)(wire.size() - offset, std::size_t{1} + random() % 193);
      auto events = parser.feed(std::string_view(wire).substr(offset, size));
      CHECK(events);
      for (const auto &event : *events) {
        CHECK(active < count && event.part_index == active);
        if (event.type == chhttp::MultipartEventType::part_begin) {
          CHECK(!begun && event.part.name == form[active].name && event.part.filename == form[active].filename);
          begun = true;
        } else if (event.type == chhttp::MultipartEventType::part_data) {
          CHECK(begun); decoded[active] += event.data;
        } else {
          CHECK(begun); begun = false; ++active;
        }
      }
      offset += size;
    }
    CHECK(parser.finish() && active == count && !begun);
    for (std::size_t part = 0; part < count; ++part) CHECK(decoded[part] == form[part].content);
  }
}

std::string hex_size(std::size_t value) {
  std::array<char, 32> buffer{};
  const auto result = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value, 16);
  CHECK(result.ec == std::errc{});
  return std::string(buffer.data(), result.ptr);
}

TEST(property_http_chunk_extensions_trailers_and_pipeline_keep_exact_bytes) {
  std::mt19937_64 random(property_seed ^ 7);
  chhttp::ServerOptions options;
  options.auto_compress_response = false;
  chhttp::Server server(options);
  server.post("/", [](const auto &request, auto &response) {
    response.body = request.body + request.headers.get("X-End");
  });
  server.get("/next", [](const auto &, auto &response) { response.body = "NEXT"; });
  CHECK(server.start("127.0.0.1", 0));
  for (property_case = 0; property_case < 60; ++property_case) {
    const auto payload = random_bytes(random, random() % 4097);
    std::string request = "POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n";
    for (std::size_t offset = 0; offset < payload.size();) {
      const auto count = (std::min)(payload.size() - offset, std::size_t{1} + random() % 211);
      request += hex_size(count) + ";random=\"a;b\"\r\n" + payload.substr(offset, count) + "\r\n";
      offset += count;
    }
    request += "0\r\nX-End: END\r\n\r\nGET /next HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    const auto response = raw_http_exchange(server.port(), request, 1 + random() % 113);
    const auto head_end = response.find("\r\n\r\n");
    CHECK(response.starts_with("HTTP/1.1 200") && head_end != std::string::npos);
    CHECK(response.substr(head_end + 4, payload.size() + 3) == payload + "END");
    CHECK(response.substr(head_end + 4 + payload.size() + 3).starts_with("HTTP/1.1 200"));
    CHECK(response.ends_with("\r\n\r\nNEXT"));
  }
}

#ifdef CHHTTP_HAS_COMPRESSION
TEST(property_compressed_binary_responses_match_original_under_chunk_framing) {
  std::mt19937_64 random(property_seed ^ 8);
  for (const std::string encoding : {"gzip", "deflate", "br", "zstd"}) {
    for (property_case = 0; property_case < 12; ++property_case) {
      auto original = random_bytes(random, property_case == 0 ? 0 : random() % 4097);
      auto compressed = chhttp::detail::compress(original, encoding);
      CHECK(compressed);
      std::string wire = "HTTP/1.1 200 OK\r\nContent-Encoding: " + encoding +
          "\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
      for (std::size_t offset = 0; offset < compressed->size();) {
        const auto count = (std::min)(compressed->size() - offset, std::size_t{1} + random() % 31);
        wire += hex_size(count) + "\r\n" + compressed->substr(offset, count) + "\r\n";
        offset += count;
      }
      wire += "0\r\nX-Checksum: done\r\n\r\n";
      auto response = fetch_raw_response(std::move(wire));
      CHECK(response && response->body == original && response->headers.get("X-Checksum") == "done");
    }
  }
}
#endif

TEST(property_websocket_binary_frames_round_trip_length_boundaries) {
  std::mt19937_64 random(property_seed ^ 9);
  chhttp::Server server;
  server.websocket("/", [](const auto &, auto &socket) -> chhttp::Task<void> {
    while (auto message = co_await socket.read())
      if (!co_await socket.send_binary(std::as_bytes(std::span(message->data)))) break;
  });
  CHECK(server.start("127.0.0.1", 0));
  auto connected = chhttp::AsyncWebSocketClient::connect("ws://127.0.0.1:" + std::to_string(server.port()) + "/").get();
  CHECK(connected);
  for (std::size_t length : {0, 1, 124, 125, 126, 127, 65534, 65535, 65536, 131072}) {
    property_case = length;
    auto payload = random_bytes(random, length);
    CHECK((*connected)->send_binary(std::as_bytes(std::span(payload))).get());
    auto received = (*connected)->read().get();
    CHECK(received && received->type == chhttp::WebSocket::MessageType::binary && received->data == payload);
  }
  (*connected)->close().get();
}
