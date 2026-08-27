#include "detail.hpp"

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <charconv>
#include <mutex>

namespace chhttp {

class SseClient::Impl {
public:
  Impl(AsyncClient &value_client, std::string value_target,
       Headers value_headers, SseClientOptions value_options)
      : client(value_client), target(std::move(value_target)),
        headers(std::move(value_headers)), options(std::move(value_options)),
        retry(options.initial_retry), last_id(options.last_event_id) {}

  void reset_event() {
    event = {};
    has_data = false;
  }

  void dispatch_event() {
    if (!has_data) {
      reset_event();
      return;
    }
    if (!event.data.empty() && event.data.back() == '\n') event.data.pop_back();
    if (!event.id.empty()) last_id = event.id;
    if (event.retry) retry = std::min(*event.retry, options.max_retry);
    try {
      if (message_handler) message_handler(event);
      const auto found = event_handlers.find(event.event);
      if (found != event_handlers.end()) found->second(event);
    } catch (const std::exception &exception) {
      callback_error = ErrorInfo{Error::internal,
                                 "SSE callback failed: " +
                                     std::string(exception.what())};
    } catch (...) {
      callback_error = ErrorInfo{Error::internal, "SSE callback failed"};
    }
    reset_event();
  }

  bool process_line(std::string_view line) {
    if (!running) return false;
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    if (first_line) {
      first_line = false;
      if (line.starts_with("\xEF\xBB\xBF")) line.remove_prefix(3);
    }
    if (line.empty()) {
      dispatch_event();
      return !callback_error;
    }
    if (line.front() == ':') return true;
    const auto colon = line.find(':');
    const auto field = line.substr(0, colon);
    auto value = colon == std::string_view::npos ? std::string_view{}
                                                 : line.substr(colon + 1);
    if (!value.empty() && value.front() == ' ') value.remove_prefix(1);
    if (field == "data") {
      event.data.append(value);
      event.data.push_back('\n');
      has_data = true;
    } else if (field == "event") {
      event.event = value;
    } else if (field == "id" && value.find('\0') == std::string_view::npos) {
      event.id = value;
    } else if (field == "retry") {
      std::int64_t milliseconds = 0;
      const auto converted = std::from_chars(
          value.data(), value.data() + value.size(), milliseconds);
      if (converted.ec == std::errc{} &&
          converted.ptr == value.data() + value.size() && milliseconds >= 0) {
        event.retry = std::chrono::milliseconds(milliseconds);
      }
    }
    return true;
  }

  bool process(std::string_view chunk) {
    pending.append(chunk);
    std::size_t begin = 0;
    while (true) {
      const auto newline = pending.find('\n', begin);
      if (newline == std::string::npos) {
        if (begin != 0) pending.erase(0, begin);
        if (pending.size() > 1024 * 1024) {
          callback_error =
              ErrorInfo{Error::protocol, "SSE line exceeds 1 MiB"};
          return false;
        }
        return running;
      }
      if (!process_line(
              std::string_view(pending).substr(begin, newline - begin))) {
        return false;
      }
      begin = newline + 1;
    }
  }

  asio::awaitable<ErrorInfo> connect() {
    bool expected = false;
    if (!running.compare_exchange_strong(expected, true)) {
      co_return ErrorInfo{Error::protocol, "SSE client is already running"};
    }
    retry = options.initial_retry;
    while (running) {
      pending.clear();
      reset_event();
      first_line = true;
      callback_error = {};
      Headers request_headers = headers;
      request_headers.set("Accept", "text/event-stream");
      request_headers.set("Cache-Control", "no-cache");
      if (!last_id.empty()) request_headers.set("Last-Event-ID", last_id);
      RequestOptions request_options;
      request_options.on_data = [this](std::string_view data) {
        return process(data);
      };
      auto result = co_await client.get(target, std::move(request_headers),
                                        std::move(request_options));
      if (!pending.empty() && running) process_line(pending);
      if (has_data) dispatch_event();
      if (callback_error) {
        running = false;
        if (error_handler) error_handler(callback_error);
        co_return callback_error;
      }
      ErrorInfo error;
      if (!result) {
        error = result.error();
      } else if (result->status != 200) {
        error = ErrorInfo{Error::protocol,
                          "SSE endpoint returned HTTP " +
                              std::to_string(result->status)};
      } else if (!detail::lower(result->headers.get("Content-Type"))
                       .starts_with("text/event-stream")) {
        error = ErrorInfo{Error::protocol,
                          "SSE endpoint has an invalid Content-Type"};
      } else {
        error = ErrorInfo{Error::read, "SSE connection ended"};
      }
      if (!running) {
        running = false;
        co_return ErrorInfo{};
      }
      if (error_handler) {
        try { error_handler(error); } catch (...) {}
      }
      if (!options.reconnect) {
        running = false;
        co_return error;
      }
      asio::steady_timer timer(co_await asio::this_coro::executor);
      timer.expires_after(retry);
      boost::system::error_code timer_error;
      co_await timer.async_wait(
          asio::redirect_error(asio::use_awaitable, timer_error));
      retry = std::min(retry * 2, options.max_retry);
    }
    running = false;
    co_return ErrorInfo{};
  }

  AsyncClient &client;
  std::string target;
  Headers headers;
  SseClientOptions options;
  MessageHandler message_handler;
  std::unordered_map<std::string, MessageHandler> event_handlers;
  ErrorCallback error_handler;
  std::atomic_bool running{false};
  std::string pending;
  SseEvent event;
  bool has_data{false};
  bool first_line{true};
  std::chrono::milliseconds retry;
  std::string last_id;
  ErrorInfo callback_error;
};

SseClient::SseClient(AsyncClient &client, std::string target, Headers headers,
                     SseClientOptions options)
    : impl_(std::make_unique<Impl>(client, std::move(target),
                                   std::move(headers), std::move(options))) {}
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

SseClient &SseClient::on_error(ErrorCallback handler) {
  impl_->error_handler = std::move(handler);
  return *this;
}

asio::awaitable<ErrorInfo> SseClient::connect() {
  co_return co_await impl_->connect();
}

void SseClient::stop() {
  if (impl_ && impl_->running.exchange(false)) impl_->client.cancel();
}

bool SseClient::running() const noexcept {
  return impl_ && impl_->running;
}

} // namespace chhttp

