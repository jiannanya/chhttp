// Regressions where upstream transfer APIs intersect the local optimizations.

#ifdef CHHTTP_HAS_COMPRESSION
TEST(merge_compressed_sync_progress_preserves_limits_and_error_context) {
  const std::string body(512 * 1024, 'z');
  auto encoded = chhttp::detail::compress(body, "gzip");
  CHECK(encoded);
  chhttp::ServerOptions server_options;
  server_options.auto_compress_response = false;
  chhttp::Server server(server_options);
  server.get("/", [&](const auto &, auto &response) {
    response.body = *encoded;
    response.headers.set("Content-Encoding", "gzip");
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  for (const bool throws : {false, true}) {
    std::uint64_t received = 0;
    auto response = client.get("/", {},
        {.on_data = [&](std::string_view data) {
           CHECK(data.size() <= 32 * 1024);
           received += data.size();
           return true;
         },
         .on_download_progress = [&](const chhttp::TransferProgress &progress) {
           CHECK(!progress.total && progress.transferred == received);
           if (received < 64 * 1024) return true;
           if (throws) throw std::runtime_error("injected download callback failure");
           return false;
         }});
    CHECK(!response && response.error().code ==
          (throws ? chhttp::Error::internal : chhttp::Error::cancelled));
    CHECK(received == 64 * 1024);
    CHECK(response.error().transfer && response.error().transfer->downloaded == received);
    CHECK(response.error().transfer->response_started &&
          response.error().transfer->phase == chhttp::TransferPhase::response_body);
  }
  auto limited = client.get("/", {}, {.max_response_body_size = std::size_t{16 * 1024}});
  CHECK(!limited && limited.error().code == chhttp::Error::body_too_large);
  auto recovered = client.get("/");
  CHECK(recovered && recovered->body == body);
}
#endif

TEST(merge_request_and_callbacks_survive_caller_scope_and_redirects) {
  std::promise<void> entered, release;
  auto entered_future = entered.get_future();
  auto released = release.get_future().share();
  chhttp::Server server;
  server.post("/start", [&](const auto &, auto &response) {
    entered.set_value();
    CHECK(released.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
    response.set_redirect("/middle", 307);
  });
  server.post("/middle", [](const auto &, auto &response) {
    response.set_redirect("/end", 307);
  });
  server.post("/end", [](const auto &request, auto &response) {
    CHECK(request.get_header("X-Owned") == "preserved");
    response.body = request.body;
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::ClientOptions client_options;
  client_options.max_connections_per_origin = 1;
  chhttp::AsyncClient client("http://127.0.0.1:" + std::to_string(server.port()), client_options);
  std::atomic_int completed_uploads{0};
  constexpr std::size_t size = 128 * 1024 + 7;
  chhttp::Task<chhttp::ResponseResult> pending;
  std::weak_ptr<std::string> lifetime;
  {
    auto owned = std::make_shared<std::string>(size, 'o');
    lifetime = owned;
    chhttp::Request request;
    request.method = "POST";
    request.target = "/start";
    request.headers.set("X-Owned", "preserved");
    request.body = *owned;
    chhttp::RequestOptions options;
    options.on_upload_progress = [owned, &completed_uploads](const auto &progress) {
      CHECK(progress.total == owned->size());
      if (progress.transferred == owned->size()) ++completed_uploads;
      return true;
    };
    pending = client.request(std::move(request), std::move(options));
  }
  CHECK(entered_future.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
  CHECK(!lifetime.expired());
  std::stop_source stopped;
  stopped.request_stop();
  auto cancelled = client.get("/unused", {}, {.stop_token = stopped.get_token()}).get();
  CHECK(!cancelled && cancelled.error().code == chhttp::Error::cancelled);
  CHECK(cancelled.error().transfer && cancelled.error().transfer->phase == chhttp::TransferPhase::none);
  auto expired = client.get("/unused", {},
      {.deadline = std::chrono::steady_clock::now() - std::chrono::seconds(1)}).get();
  CHECK(!expired && expired.error().code == chhttp::Error::timeout);
  CHECK(expired.error().transfer && expired.error().transfer->phase == chhttp::TransferPhase::none);
  release.set_value();
  auto response = pending.get();
  CHECK(response && response->body == std::string(size, 'o'));
  CHECK(completed_uploads == 3);
  CHECK(lifetime.expired());
}

TEST(merge_buffered_upload_progress_preserves_bounded_writes) {
  chhttp::Server server;
  server.post("/", [](const auto &request, auto &response) {
    response.body = request.body;
  });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  for (const std::size_t size : {0u, 1u, 65535u, 65536u, 65537u, 131077u, 2097152u}) {
    const std::string body(size, 'b');
    std::uint64_t uploaded = 0;
    auto response = client.post("/", body, "application/octet-stream", {},
        {.on_upload_progress = [&](const chhttp::TransferProgress &progress) {
          CHECK(progress.total == size);
          CHECK(progress.transferred > uploaded && progress.transferred <= size);
          CHECK(progress.transferred - uploaded <= 64 * 1024);
          uploaded = progress.transferred;
          return true;
        }});
    CHECK(response && response->body == body && uploaded == size);
  }
}

TEST(merge_upload_progress_cancellation_and_exceptions_keep_partial_context) {
  chhttp::Server server;
  server.post("/", [](const auto &request, auto &response) { response.body = request.body; });
  CHECK(server.start("127.0.0.1", 0));
  chhttp::Client client("http://127.0.0.1:" + std::to_string(server.port()));
  const std::string body(2 * 1024 * 1024, 'c');
  for (const bool throws : {false, true}) {
    std::uint64_t uploaded = 0;
    auto response = client.post("/", body, "application/octet-stream", {},
        {.on_upload_progress = [&](const chhttp::TransferProgress &progress) {
          uploaded = progress.transferred;
          if (uploaded < 128 * 1024) return true;
          if (throws) throw std::runtime_error("injected upload callback failure");
          return false;
        }});
    CHECK(!response);
    CHECK(response.error().code == (throws ? chhttp::Error::internal : chhttp::Error::cancelled));
    CHECK(uploaded == 128 * 1024);
    CHECK(response.error().transfer && response.error().transfer->uploaded == uploaded);
    CHECK(response.error().transfer->phase == chhttp::TransferPhase::request_body);
    CHECK(!response.error().transfer->response_started);
    auto recovered = client.post("/", "healthy", "text/plain");
    CHECK(recovered && recovered->body == "healthy");
  }
}

TEST(merge_file_slice_producer_preserves_loop_affinity_and_fixed_size) {
  Round2TestDirectory directory;
  const auto file = directory.path / "slice.bin";
  const std::string contents(192 * 1024 + 17, 'f');
  std::ofstream(file, std::ios::binary) << contents;
  chhttp::Request request;
  CHECK(request.set_file_body(file, {.offset = 7, .length = contents.size() - 13}));
  std::ofstream(file, std::ios::binary | std::ios::app) << "later growth";
  auto runtime = std::make_shared<chhttp::detail::Runtime>();
  struct Sink : chhttp::StreamWriter::Sink {
    chhttp::detail::Runtime *runtime;
    std::string bytes;
    explicit Sink(chhttp::detail::Runtime *owner) : runtime(owner) {}
    chhttp::Task<bool> write(std::string data) override {
      CHECK(runtime->on_loop_thread());
      CHECK(data.size() <= 64 * 1024);
      bytes += data;
      co_return true;
    }
    chhttp::Task<bool> flush() override { co_return true; }
    bool open() const noexcept override { return true; }
  };
  auto sink = std::make_shared<Sink>(runtime.get());
  chhttp::StreamWriter writer(sink);
  const auto produce = [&]() -> chhttp::Task<void> {
    co_await chhttp::detail::resume_on(runtime);
    co_await request.body_stream(writer);
    CHECK(runtime->on_loop_thread());
  };
  produce().get();
  runtime->stop();
  CHECK(sink->bytes == contents.substr(7, contents.size() - 13));
}

TEST(merge_async_file_sink_keeps_origin_loop_alive_through_shutdown) {
  Round2TestDirectory directory;
  const auto file = directory.path / "sink.bin";
  auto opened = chhttp::AsyncFileSink::open(file);
  CHECK(opened);
  auto sink = std::move(*opened);
  auto runtime = std::make_shared<chhttp::detail::Runtime>();
  std::promise<void> queued;
  auto ready = queued.get_future();
  const auto write = [&]() -> chhttp::Task<void> {
    co_await chhttp::detail::resume_on(runtime);
    auto pending = sink.write(std::string(8 * 1024 * 1024, 's'));
    queued.set_value();
    CHECK(!co_await std::move(pending));
    CHECK(runtime->on_loop_thread());
    CHECK(!co_await sink.flush());
    CHECK(runtime->on_loop_thread());
  };
  auto pending = write();
  CHECK(ready.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
  runtime->stop();
  pending.get();
  CHECK(sink.written() == 8 * 1024 * 1024);
  CHECK(std::filesystem::file_size(file) == sink.written());
}
