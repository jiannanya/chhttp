#include "detail.hpp"

#include <charconv>
#include <mutex>

namespace chhttp {

class SseParser::Impl {
public:
  explicit Impl(SseParserOptions value_options)
      : options(std::move(value_options)), last_id(options.last_event_id) {}

  void reset() {
    line.clear();
    data.clear();
    event_type.clear();
    last_id = options.last_event_id;
    retry_delay.reset();
    event_retry.reset();
    first_line = true;
    skip_lf = false;
    failed = {};
  }

  ErrorInfo dispatch() {
    if (data.empty()) {
      event_type.clear();
      event_retry.reset();
      return {};
    }
    if (data.back() == '\n') data.pop_back();
    SseEvent event{.data = std::move(data),
                   .event = std::move(event_type),
                   .id = last_id,
                   .retry = event_retry};
    data.clear();
    event_type.clear();
    event_retry.reset();
    try {
      if (message_handler) message_handler(event);
      const auto found = event_handlers.find(event.event);
      if (found != event_handlers.end()) found->second(event);
    } catch (const std::exception &exception) {
      return {Error::internal,
              "SSE callback failed: " + std::string(exception.what())};
    } catch (...) {
      return {Error::internal, "SSE callback failed"};
    }
    return {};
  }

  ErrorInfo process_line(std::string_view value) {
    if (first_line) {
      first_line = false;
      if (value.starts_with("\xEF\xBB\xBF")) value.remove_prefix(3);
    }
    if (value.empty()) return dispatch();
    if (value.front() == ':') return {};
    const auto colon = value.find(':');
    const auto field = value.substr(0, colon);
    auto field_value = colon == std::string_view::npos
                           ? std::string_view{}
                           : value.substr(colon + 1);
    if (!field_value.empty() && field_value.front() == ' ')
      field_value.remove_prefix(1);
    const auto exceeds_event_limit = [&](std::size_t additional) {
      const auto current = data.size() + event_type.size() + last_id.size();
      return additional > options.max_event_size ||
             current > options.max_event_size - additional;
    };
    if (field == "data") {
      if (exceeds_event_limit(field_value.size() + 1))
        return {Error::body_too_large, "SSE event exceeds configured limit"};
      data.append(field_value);
      data.push_back('\n');
    } else if (field == "event") {
      if (exceeds_event_limit(field_value.size()))
        return {Error::body_too_large, "SSE event exceeds configured limit"};
      event_type = field_value;
    } else if (field == "id" &&
               field_value.find('\0') == std::string_view::npos) {
      if (field_value.size() > options.max_event_size)
        return {Error::body_too_large,
                "SSE event ID exceeds configured limit"};
      last_id = field_value;
    } else if (field == "retry") {
      std::uint64_t milliseconds = 0;
      const auto converted = std::from_chars(
          field_value.data(), field_value.data() + field_value.size(),
          milliseconds);
      if (converted.ec == std::errc{} &&
          converted.ptr == field_value.data() + field_value.size() &&
          milliseconds <= static_cast<std::uint64_t>(
                              std::chrono::milliseconds::max().count())) {
        retry_delay = std::chrono::milliseconds(milliseconds);
        event_retry = retry_delay;
      }
    }
    return {};
  }

  ErrorInfo feed(std::string_view bytes) {
    if (failed) return failed;
    for (const char ch : bytes) {
      if (skip_lf) {
        skip_lf = false;
        if (ch == '\n') continue;
      }
      if (ch == '\r' || ch == '\n') {
        if (auto error = process_line(line); error) {
          failed = std::move(error);
          return failed;
        }
        line.clear();
        skip_lf = ch == '\r';
        continue;
      }
      if (line.size() >= options.max_line_size) {
        failed = {Error::body_too_large, "SSE line exceeds configured limit"};
        return failed;
      }
      line.push_back(ch);
    }
    return {};
  }

  ErrorInfo finish() {
    if (failed) return failed;
    // EOF is not an event delimiter. A final event is dispatched only after an
    // empty line; any partial line or accumulated data is intentionally lost.
    line.clear();
    data.clear();
    event_type.clear();
    event_retry.reset();
    skip_lf = false;
    return {};
  }

  SseParserOptions options;
  MessageHandler message_handler;
  std::unordered_map<std::string, MessageHandler> event_handlers;
  std::string line;
  std::string data;
  std::string event_type;
  std::string last_id;
  std::optional<std::chrono::milliseconds> retry_delay;
  std::optional<std::chrono::milliseconds> event_retry;
  bool first_line{true};
  bool skip_lf{false};
  ErrorInfo failed;
};

SseParser::SseParser(SseParserOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}
SseParser::~SseParser() = default;
SseParser::SseParser(SseParser &&) noexcept = default;
SseParser &SseParser::operator=(SseParser &&) noexcept = default;

SseParser &SseParser::on_message(MessageHandler handler) {
  impl_->message_handler = std::move(handler);
  return *this;
}

SseParser &SseParser::on_event(std::string event, MessageHandler handler) {
  impl_->event_handlers[std::move(event)] = std::move(handler);
  return *this;
}

ErrorInfo SseParser::feed(std::string_view bytes) {
  if (!impl_) return {Error::internal, "Empty SSE parser"};
  return impl_->feed(bytes);
}

ErrorInfo SseParser::finish() {
  if (!impl_) return {Error::internal, "Empty SSE parser"};
  return impl_->finish();
}

void SseParser::reset() {
  if (impl_) impl_->reset();
}

std::string SseParser::last_event_id() const {
  return impl_ ? impl_->last_id : std::string{};
}

std::optional<std::chrono::milliseconds> SseParser::retry() const {
  return impl_ ? impl_->retry_delay : std::nullopt;
}

class SseClient::Impl {
public:
  Impl(AsyncClient &value_client, Request value_request,
       SseClientOptions value_options)
      : client(value_client), request(std::move(value_request)),
        options(std::move(value_options)), retry_delay(options.initial_retry),
        last_id(options.last_event_id) {}

  Task<ErrorInfo> connect() {
    bool expected = false;
    if (!running.compare_exchange_strong(expected, true))
      co_return ErrorInfo{Error::protocol, "SSE client is already running"};
    retry_delay = options.initial_retry;
    while (running) {
      ErrorInfo parser_error;
      ErrorInfo head_error;
      bool stream_head_valid = false;
      SseParser parser({.max_line_size = 1024 * 1024,
                        .max_event_size = 8 * 1024 * 1024,
                        .last_event_id = last_id});
      parser.on_message([this](const SseEvent &event) {
        if (message_handler) message_handler(event);
      });
      for (const auto &[name, handler] : event_handlers)
        parser.on_event(name, handler);

      Request current = request;
      current.headers.set("Accept", "text/event-stream");
      current.headers.set("Cache-Control", "no-cache");
      if (!last_id.empty()) current.headers.set("Last-Event-ID", last_id);
      auto stop = std::make_shared<std::stop_source>();
      {
        std::lock_guard lock(stop_mutex);
        active_stop = stop;
      }
      RequestOptions request_options;
      request_options.stop_token = stop->get_token();
      request_options.on_response_head = [this, &head_error,
                                           &stream_head_valid](
                                              const ResponseHead &head) {
        const bool redirect =
            (head.status == 301 || head.status == 302 || head.status == 303 ||
             head.status == 307 || head.status == 308) &&
            head.headers.contains("Location");
        if (redirect) return true;
        if (head.status != 200) {
          head_error = {Error::protocol,
                        "SSE endpoint returned HTTP " +
                            std::to_string(head.status)};
          return false;
        }
        auto content_type = detail::lower(head.headers.get("Content-Type"));
        if (const auto parameter = content_type.find(';');
            parameter != std::string::npos)
          content_type.resize(parameter);
        if (detail::trim(content_type) != "text/event-stream") {
          head_error = {Error::protocol,
                        "SSE endpoint has an invalid Content-Type"};
          return false;
        }
        stream_head_valid = true;
        try {
          if (open_handler) open_handler(head);
        } catch (const std::exception &exception) {
          head_error = {Error::internal,
                        "SSE open callback failed: " +
                            std::string(exception.what())};
          return false;
        } catch (...) {
          head_error = {Error::internal, "SSE open callback failed"};
          return false;
        }
        return true;
      };
      request_options.on_data = [&](std::string_view bytes) {
        if (!stream_head_valid) return true;
        parser_error = parser.feed(bytes);
        return !parser_error && running;
      };
      auto result = co_await client.request(std::move(current),
                                            std::move(request_options));
      {
        std::lock_guard lock(stop_mutex);
        if (active_stop == stop) active_stop.reset();
      }
      if (!parser_error && running) parser_error = parser.finish();
      last_id = parser.last_event_id();
      if (parser.retry())
        retry_delay = std::min(*parser.retry(), options.max_retry);

      if (!running) {
        running = false;
        co_return ErrorInfo{};
      }
      ErrorInfo error;
      if (head_error)
        error = std::move(head_error);
      else if (parser_error)
        error = std::move(parser_error);
      else if (!result)
        error = result.error();
      else if (!stream_head_valid)
        error = {Error::protocol, "SSE endpoint did not open an event stream"};
      else
        error = {Error::read, "SSE connection ended"};
      if (error_handler) {
        try {
          error_handler(error);
        } catch (...) {
        }
      }
      if (!options.reconnect) {
        running = false;
        co_return error;
      }
      auto remaining = retry_delay;
      while (running && remaining > 0ms) {
        const auto interval = std::min(remaining, 25ms);
        co_await sleep_for(interval);
        remaining -= interval;
      }
      retry_delay = std::min(retry_delay * 2, options.max_retry);
    }
    running = false;
    co_return ErrorInfo{};
  }

  void stop() {
    if (!running.exchange(false)) return;
    std::shared_ptr<std::stop_source> stop;
    {
      std::lock_guard lock(stop_mutex);
      stop = active_stop;
    }
    if (stop) stop->request_stop();
  }

  AsyncClient &client;
  Request request;
  SseClientOptions options;
  MessageHandler message_handler;
  std::unordered_map<std::string, MessageHandler> event_handlers;
  ErrorCallback error_handler;
  OpenHandler open_handler;
  std::atomic_bool running{false};
  std::chrono::milliseconds retry_delay;
  std::string last_id;
  std::mutex stop_mutex;
  std::shared_ptr<std::stop_source> active_stop;
};

SseClient::SseClient(AsyncClient &client, std::string target, Headers headers,
                     SseClientOptions options)
    : SseClient(client,
                Request{.method = "GET",
                        .target = std::move(target),
                        .headers = std::move(headers)},
                std::move(options)) {}

SseClient::SseClient(AsyncClient &client, Request request,
                     SseClientOptions options)
    : impl_(std::make_shared<Impl>(client, std::move(request),
                                   std::move(options))) {}

SseClient::~SseClient() { stop(); }
SseClient::SseClient(SseClient &&) noexcept = default;
SseClient &SseClient::operator=(SseClient &&) noexcept = default;

SseClient &SseClient::on_message(MessageHandler handler) {
  impl_->message_handler = std::move(handler);
  return *this;
}

SseClient &SseClient::on_event(std::string event, MessageHandler handler) {
  impl_->event_handlers[std::move(event)] = std::move(handler);
  return *this;
}

SseClient &SseClient::on_open(OpenHandler handler) {
  impl_->open_handler = std::move(handler);
  return *this;
}

SseClient &SseClient::on_error(ErrorCallback handler) {
  impl_->error_handler = std::move(handler);
  return *this;
}

Task<ErrorInfo> SseClient::connect() {
  if (!impl_) co_return ErrorInfo{Error::internal, "Empty SSE client"};
  auto impl = impl_;
  co_return co_await impl->connect();
}

void SseClient::stop() {
  if (impl_) impl_->stop();
}

bool SseClient::running() const noexcept {
  return impl_ && impl_->running;
}

} // namespace chhttp
