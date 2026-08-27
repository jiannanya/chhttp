#include "detail.hpp"

#ifdef CHHTTP_HAS_TLS
#include <openssl/err.h>
#endif

#ifdef CHHTTP_HAS_COMPRESSION
#include <brotli/decode.h>
#include <zlib.h>
#include <zstd.h>
#endif

#include <charconv>
#include <cstring>
#include <iomanip>
#include <limits>

namespace chhttp::detail {
namespace {

struct ParsedHead {
  std::string start_line;
  Headers headers;
  std::optional<std::uint64_t> content_length;
  bool chunked{false};
};

using TimePoint = std::chrono::steady_clock::time_point;

Task<Result<Connection::ReadChunk>> timed_read(
    const std::shared_ptr<Connection> &connection,
    const HttpReadOptions &options, std::optional<TimePoint> phase_deadline,
    std::string_view phase) {
  if (options.cancelled && options.cancelled())
    co_return ErrorInfo{Error::cancelled, "Request cancelled"};
  const auto now = std::chrono::steady_clock::now();
  auto timeout = options.read_timeout;
  std::string timeout_message = "Socket read timed out";
  const auto apply_deadline = [&](const std::optional<TimePoint> &deadline,
                                  std::string message) -> ErrorInfo {
    if (!deadline) return {};
    if (*deadline <= now)
      return {Error::timeout, std::move(message)};
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        *deadline - now);
    if (remaining < timeout) {
      timeout = std::max(remaining, 1ms);
      timeout_message = std::move(message);
    }
    return {};
  };
  if (auto error = apply_deadline(options.deadline,
                                  "Request deadline exceeded"))
    co_return error;
  if (auto error = apply_deadline(phase_deadline,
                                  std::string(phase) + " timed out"))
    co_return error;
  auto chunk = co_await connection->read(timeout);
  if (!chunk && chunk.error().code == Error::timeout)
    co_return ErrorInfo{Error::timeout, std::move(timeout_message),
                        chunk.error().system_code, chunk.error().tls_code};
  if (options.cancelled && options.cancelled())
    co_return ErrorInfo{Error::cancelled, "Request cancelled"};
  co_return chunk;
}

class StreamingDecoder {
public:
  static Result<std::unique_ptr<StreamingDecoder>>
  create(std::string_view encoding, std::size_t max_output) {
#ifdef CHHTTP_HAS_COMPRESSION
    auto decoder = std::unique_ptr<StreamingDecoder>(
        new StreamingDecoder(max_output));
    const auto normalized = lower(encoding);
    if (normalized == "gzip" || normalized == "deflate") {
      decoder->kind_ = Kind::zlib;
      const int window_bits = normalized == "gzip" ? MAX_WBITS + 16 : MAX_WBITS;
      if (inflateInit2(&decoder->zlib_, window_bits) != Z_OK)
        return ErrorInfo{Error::compression,
                         "Unable to initialize streaming zlib decoder"};
      decoder->zlib_initialized_ = true;
    } else if (normalized == "br") {
      decoder->kind_ = Kind::brotli;
      decoder->brotli_ = BrotliDecoderCreateInstance(nullptr, nullptr, nullptr);
      if (!decoder->brotli_)
        return ErrorInfo{Error::compression,
                         "Unable to initialize streaming Brotli decoder"};
    } else if (normalized == "zstd") {
      decoder->kind_ = Kind::zstd;
      decoder->zstd_ = ZSTD_createDStream();
      if (!decoder->zstd_ || ZSTD_isError(ZSTD_initDStream(decoder->zstd_)))
        return ErrorInfo{Error::compression,
                         "Unable to initialize streaming Zstd decoder"};
    } else {
      return ErrorInfo{Error::compression, "Unsupported content encoding"};
    }
    return decoder;
#else
    (void)encoding;
    (void)max_output;
    return ErrorInfo{Error::compression, "Compression support is disabled"};
#endif
  }

  ~StreamingDecoder() {
#ifdef CHHTTP_HAS_COMPRESSION
    if (zlib_initialized_) inflateEnd(&zlib_);
    if (brotli_) BrotliDecoderDestroyInstance(brotli_);
    if (zstd_) ZSTD_freeDStream(zstd_);
#endif
  }

  Result<std::string> feed(std::string_view input, bool finish) {
#ifdef CHHTTP_HAS_COMPRESSION
    if (finished_) {
      if (!input.empty())
        return ErrorInfo{Error::compression,
                         "Compressed stream contains trailing data"};
      return std::string{};
    }
    std::string output;
    std::array<char, 32 * 1024> buffer{};
    if (kind_ == Kind::zlib) {
      zlib_.next_in = reinterpret_cast<Bytef *>(
          const_cast<char *>(input.data()));
      zlib_.avail_in = static_cast<uInt>(input.size());
      for (;;) {
        zlib_.next_out = reinterpret_cast<Bytef *>(buffer.data());
        zlib_.avail_out = static_cast<uInt>(buffer.size());
        const int status = inflate(&zlib_, finish ? Z_FINISH : Z_NO_FLUSH);
        if (auto error = append(output, buffer.data(),
                                buffer.size() - zlib_.avail_out); error)
          return error;
        if (status == Z_STREAM_END) {
          if (zlib_.avail_in != 0)
            return ErrorInfo{Error::compression,
                             "Compressed stream contains trailing data"};
          finished_ = true;
          break;
        }
        if (status == Z_BUF_ERROR && zlib_.avail_in == 0) break;
        if (status != Z_OK)
          return ErrorInfo{Error::compression,
                           zlib_.msg ? zlib_.msg : "Invalid zlib stream"};
        if (zlib_.avail_in == 0 && zlib_.avail_out != 0) break;
      }
    } else if (kind_ == Kind::brotli) {
      const auto *next_input =
          reinterpret_cast<const std::uint8_t *>(input.data());
      std::size_t available_input = input.size();
      for (;;) {
        auto *next_output = reinterpret_cast<std::uint8_t *>(buffer.data());
        std::size_t available_output = buffer.size();
        const auto status = BrotliDecoderDecompressStream(
            brotli_, &available_input, &next_input, &available_output,
            &next_output, nullptr);
        if (auto error = append(output, buffer.data(),
                                buffer.size() - available_output); error)
          return error;
        if (status == BROTLI_DECODER_RESULT_SUCCESS) {
          if (available_input != 0)
            return ErrorInfo{Error::compression,
                             "Compressed stream contains trailing data"};
          finished_ = true;
          break;
        }
        if (status == BROTLI_DECODER_RESULT_ERROR)
          return ErrorInfo{Error::compression, "Invalid Brotli stream"};
        if (status == BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT &&
            available_input == 0)
          break;
      }
    } else if (kind_ == Kind::zstd) {
      ZSTD_inBuffer source{input.data(), input.size(), 0};
      std::size_t remaining = 1;
      do {
        ZSTD_outBuffer target{buffer.data(), buffer.size(), 0};
        remaining = ZSTD_decompressStream(zstd_, &target, &source);
        if (ZSTD_isError(remaining))
          return ErrorInfo{Error::compression, ZSTD_getErrorName(remaining)};
        if (auto error = append(output, buffer.data(), target.pos); error)
          return error;
        if (remaining == 0) finished_ = true;
      } while (source.pos < source.size);
    }
    if (finish && !finished_)
      return ErrorInfo{Error::compression, "Truncated compressed stream"};
    return output;
#else
    (void)input;
    (void)finish;
    return ErrorInfo{Error::compression, "Compression support is disabled"};
#endif
  }

private:
#ifdef CHHTTP_HAS_COMPRESSION
  enum class Kind { none, zlib, brotli, zstd };
#endif
  explicit StreamingDecoder(std::size_t max_output)
      : max_output_(max_output) {}

  ErrorInfo append(std::string &output, const char *data, std::size_t count) {
    if (count > max_output_ || total_output_ > max_output_ - count)
      return {Error::body_too_large, "Decompressed body exceeds limit"};
    output.append(data, count);
    total_output_ += count;
    return {};
  }

  std::size_t max_output_{0};
  std::size_t total_output_{0};
#ifdef CHHTTP_HAS_COMPRESSION
  Kind kind_{Kind::none};
  bool finished_{false};
  z_stream zlib_{};
  bool zlib_initialized_{false};
  BrotliDecoderState *brotli_{nullptr};
  ZSTD_DStream *zstd_{nullptr};
#endif
};

Result<std::vector<std::string>> framing_values(std::string_view input,
                                                std::string_view field) {
  std::vector<std::string> values;
  std::size_t start = 0;
  while (start <= input.size()) {
    const auto end = input.find(',', start);
    auto value = trim(input.substr(start, end == std::string_view::npos
                                             ? std::string_view::npos
                                             : end - start));
    if (value.empty())
      return ErrorInfo{Error::protocol,
                       "Empty value in " + std::string(field)};
    values.push_back(std::move(value));
    if (end == std::string_view::npos) break;
    start = end + 1;
  }
  return values;
}

Task<Result<std::string>> read_head_bytes(
    const std::shared_ptr<Connection> &connection, std::string &buffer,
    const HttpReadOptions &options) {
  const auto header_deadline = options.header_timeout
      ? std::optional<TimePoint>{std::chrono::steady_clock::now() +
                                 *options.header_timeout}
      : std::nullopt;
  for (;;) {
    const auto end = buffer.find("\r\n\r\n");
    if (end != std::string::npos) {
      if (end + 4 > options.max_header_size)
        co_return ErrorInfo{Error::protocol, "HTTP headers exceed configured limit"};
      std::string head = buffer.substr(0, end + 2);
      buffer.erase(0, end + 4);
      co_return head;
    }
    if (buffer.size() >= options.max_header_size)
      co_return ErrorInfo{Error::protocol, "HTTP headers exceed configured limit"};
    auto chunk = co_await timed_read(connection, options, header_deadline,
                                     "Response header");
    if (!chunk) co_return chunk.error();
    if (chunk->eof)
      co_return ErrorInfo{Error::protocol, "Connection closed during HTTP headers"};
    buffer += chunk->data;
  }
}

Result<ParsedHead> parse_head(std::string_view input) {
  ParsedHead result;
  const auto first_end = input.find("\r\n");
  if (first_end == std::string_view::npos || first_end == 0)
    return ErrorInfo{Error::protocol, "Malformed HTTP start line"};
  result.start_line = std::string(input.substr(0, first_end));
  if (!valid_header_value(result.start_line))
    return ErrorInfo{Error::protocol, "Invalid character in HTTP start line"};
  std::size_t cursor = first_end + 2;
  while (cursor < input.size()) {
    const auto line_end = input.find("\r\n", cursor);
    if (line_end == std::string_view::npos)
      return ErrorInfo{Error::protocol, "Malformed HTTP header termination"};
    const auto line = input.substr(cursor, line_end - cursor);
    cursor = line_end + 2;
    if (line.empty()) break;
    if (line.front() == ' ' || line.front() == '\t')
      return ErrorInfo{Error::protocol, "Obsolete folded HTTP header rejected"};
    const auto colon = line.find(':');
    if (colon == std::string_view::npos)
      return ErrorInfo{Error::protocol, "HTTP header has no colon"};
    const auto name = line.substr(0, colon);
    auto value = std::string_view(line).substr(colon + 1);
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
      value.remove_prefix(1);
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t'))
      value.remove_suffix(1);
    if (!valid_header_name(name) || !valid_header_value(value))
      return ErrorInfo{Error::protocol, "Invalid HTTP header field"};
    result.headers.add(std::string(name), std::string(value));
  }

  std::optional<std::uint64_t> content_length;
  for (const auto &line : result.headers.get_all("Content-Length")) {
    auto values = framing_values(line, "Content-Length");
    if (!values) return values.error();
    for (const auto &value : *values) {
      std::uint64_t parsed = 0;
      const auto conversion =
          std::from_chars(value.data(), value.data() + value.size(), parsed);
      if (conversion.ec != std::errc{} || conversion.ptr != value.data() + value.size())
        return ErrorInfo{Error::protocol, "Invalid Content-Length"};
      if (content_length && *content_length != parsed)
        return ErrorInfo{Error::protocol, "Conflicting Content-Length headers"};
      content_length = parsed;
    }
  }
  result.content_length = content_length;

  std::vector<std::string> transfer_codings;
  for (const auto &line : result.headers.get_all("Transfer-Encoding")) {
    auto codings = framing_values(line, "Transfer-Encoding");
    if (!codings) return codings.error();
    for (auto coding : *codings) {
      const auto semicolon = coding.find(';');
      if (semicolon != std::string::npos)
        return ErrorInfo{Error::protocol,
                         "Transfer-Encoding parameters are not supported"};
      transfer_codings.push_back(lower(trim(coding)));
    }
  }
  if (!transfer_codings.empty()) {
    if (content_length)
      return ErrorInfo{Error::protocol,
                       "Transfer-Encoding with Content-Length is rejected"};
    if (transfer_codings.back() != "chunked")
      return ErrorInfo{Error::protocol,
                       "Final HTTP transfer coding is not chunked"};
    if (transfer_codings.size() != 1)
      return ErrorInfo{Error::protocol, "Unsupported HTTP transfer coding"};
    result.chunked = true;
  }
  return result;
}

bool parse_version(std::string_view value, unsigned &version) {
  if (value == "HTTP/1.1") {
    version = 11;
    return true;
  }
  if (value == "HTTP/1.0") {
    version = 10;
    return true;
  }
  return false;
}

bool keep_alive(unsigned version, const Headers &headers) {
  if (has_token(headers, "Connection", "close")) return false;
  return version >= 11 || has_token(headers, "Connection", "keep-alive");
}

Task<Result<std::string>> read_line(
    const std::shared_ptr<Connection> &connection, std::string &buffer,
    std::size_t max_length, const HttpReadOptions &options,
    std::optional<TimePoint> phase_deadline,
    std::string_view phase) {
  for (;;) {
    const auto end = buffer.find("\r\n");
    if (end != std::string::npos) {
      if (end > max_length)
        co_return ErrorInfo{Error::protocol, "HTTP line exceeds configured limit"};
      std::string line = buffer.substr(0, end);
      buffer.erase(0, end + 2);
      co_return line;
    }
    if (buffer.find('\n') != std::string::npos)
      co_return ErrorInfo{Error::protocol, "Bare LF in HTTP message"};
    if (buffer.size() > max_length)
      co_return ErrorInfo{Error::protocol, "HTTP line exceeds configured limit"};
    auto chunk = co_await timed_read(connection, options, phase_deadline, phase);
    if (!chunk) co_return chunk.error();
    if (chunk->eof)
      co_return ErrorInfo{Error::protocol, "Unexpected end of HTTP line"};
    buffer += chunk->data;
  }
}

Task<Result<std::string>> read_body(
    const std::shared_ptr<Connection> &connection, std::string &buffer,
    ParsedHead &head, const HttpReadOptions &options, bool body_until_eof,
    bool no_body) {
  std::string body;
  std::uint64_t received = 0;
  const auto started = std::chrono::steady_clock::now();
  std::optional<TimePoint> first_body_deadline =
      options.first_body_byte_timeout
          ? std::optional<TimePoint>{started + *options.first_body_byte_timeout}
          : std::nullopt;
  std::optional<TimePoint> idle_deadline;
  const auto body_deadline = [&]() {
    return received == 0 ? first_body_deadline : idle_deadline;
  };
  const auto read_more = [&]() -> Task<Result<Connection::ReadChunk>> {
    co_return co_await timed_read(
        connection, options, body_deadline(),
        received == 0 ? "First response body byte" : "Response body idle");
  };
  const auto deliver = [&](std::string_view data,
                           std::uint64_t total) -> Task<ErrorInfo> {
    if (data.empty()) co_return ErrorInfo{};
    if (options.cancelled && options.cancelled())
      co_return ErrorInfo{Error::cancelled, "Request cancelled"};
    if (options.deadline &&
        std::chrono::steady_clock::now() >= *options.deadline)
      co_return ErrorInfo{Error::timeout, "Request deadline exceeded"};
    if (data.size() > options.max_body_size ||
        received > options.max_body_size - data.size())
      co_return ErrorInfo{Error::body_too_large,
                          "HTTP body exceeds configured limit"};
    received += data.size();
    try {
      if (options.on_data) {
        if (!options.on_data(data))
          co_return ErrorInfo{Error::cancelled,
                              "HTTP body callback cancelled the transfer"};
      } else if (options.on_data_async) {
        if (!co_await options.on_data_async(data))
          co_return ErrorInfo{Error::cancelled,
                              "HTTP async body callback cancelled the transfer"};
      } else {
        body.append(data);
      }
      if (options.on_progress && !options.on_progress(received, total))
        co_return ErrorInfo{Error::cancelled,
                            "HTTP progress callback cancelled the transfer"};
      if (options.deadline &&
          std::chrono::steady_clock::now() >= *options.deadline)
        co_return ErrorInfo{Error::timeout, "Request deadline exceeded"};
      // Consumer backpressure is application work, not network idleness. Start
      // the next idle interval only after the awaited consumer is ready again.
      if (options.idle_timeout)
        idle_deadline = std::chrono::steady_clock::now() + *options.idle_timeout;
    } catch (const std::exception &exception) {
      co_return ErrorInfo{Error::internal,
                          "HTTP body callback failed: " +
                              std::string(exception.what())};
    } catch (...) {
      co_return ErrorInfo{Error::internal, "HTTP body callback failed"};
    }
    co_return ErrorInfo{};
  };
  if (no_body) co_return body;

  if (head.chunked) {
    for (;;) {
      auto line = co_await read_line(connection, buffer, 16 * 1024, options,
                                     body_deadline(),
                                     received == 0 ? "First response body byte"
                                                   : "Response body idle");
      if (!line) co_return line.error();
      auto size_text = std::string_view(*line);
      const auto extension = size_text.find(';');
      if (extension != std::string_view::npos) {
        const auto extension_text = size_text.substr(extension + 1);
        if (extension_text.empty() || !valid_header_value(extension_text))
          co_return ErrorInfo{Error::protocol, "Invalid chunk extension"};
        size_text = size_text.substr(0, extension);
      }
      std::uint64_t size = 0;
      const auto conversion = std::from_chars(size_text.data(),
                                               size_text.data() + size_text.size(),
                                               size, 16);
      if (size_text.empty() || conversion.ec != std::errc{} ||
          conversion.ptr != size_text.data() + size_text.size())
        co_return ErrorInfo{Error::protocol, "Invalid chunk size"};
      if (size == 0) {
        std::size_t trailer_bytes = 0;
        for (;;) {
          auto trailer = co_await read_line(connection, buffer,
                                            options.max_header_size,
                                            options, std::nullopt,
                                            "Response trailer");
          if (!trailer) co_return trailer.error();
          if (trailer->size() > options.max_header_size ||
              trailer_bytes > options.max_header_size - trailer->size() ||
              options.max_header_size - trailer_bytes - trailer->size() < 2)
            co_return ErrorInfo{Error::protocol,
                                "HTTP trailers exceed configured limit"};
          trailer_bytes += trailer->size() + 2;
          if (trailer->empty()) break;
          const auto colon = trailer->find(':');
          if (colon == std::string::npos)
            co_return ErrorInfo{Error::protocol, "Malformed HTTP trailer"};
          auto name = std::string_view(*trailer).substr(0, colon);
          auto value = trim(std::string_view(*trailer).substr(colon + 1));
          if (!valid_header_name(name) || !valid_header_value(value))
            co_return ErrorInfo{Error::protocol, "Invalid HTTP trailer"};
          if (iequals(name, "Content-Length") ||
              iequals(name, "Transfer-Encoding") || iequals(name, "Host"))
            co_return ErrorInfo{Error::protocol,
                                "Forbidden framing field in HTTP trailer"};
          head.headers.add(std::string(name), std::move(value));
        }
        break;
      }
      if (size > options.max_body_size || received > options.max_body_size - size)
        co_return ErrorInfo{Error::body_too_large,
                            "Chunked HTTP body exceeds configured limit"};
      if (size > std::numeric_limits<std::size_t>::max())
        co_return ErrorInfo{Error::body_too_large,
                            "Chunked HTTP body is too large"};
      std::uint64_t remaining = size;
      while (remaining > 0) {
        if (buffer.empty()) {
          auto chunk = co_await read_more();
          if (!chunk) co_return chunk.error();
          if (chunk->eof)
            co_return ErrorInfo{Error::protocol,
                                "Unexpected end of HTTP chunk"};
          buffer += chunk->data;
        }
        const auto count = static_cast<std::size_t>(
            std::min<std::uint64_t>(remaining, buffer.size()));
        auto error = co_await deliver(
            std::string_view(buffer).substr(0, count), 0);
        if (error) co_return error;
        buffer.erase(0, count);
        remaining -= count;
      }
      while (buffer.size() < 2) {
        auto chunk = co_await read_more();
        if (!chunk) co_return chunk.error();
        if (chunk->eof)
          co_return ErrorInfo{Error::protocol,
                              "Unexpected end after HTTP chunk"};
        buffer += chunk->data;
      }
      if (buffer[0] != '\r' || buffer[1] != '\n')
        co_return ErrorInfo{Error::protocol, "Chunk payload lacks CRLF"};
      buffer.erase(0, 2);
    }
    co_return body;
  }

  if (head.content_length) {
    const auto total = *head.content_length;
    if (total > options.max_body_size)
      co_return ErrorInfo{Error::body_too_large,
                          "HTTP body exceeds configured limit"};
    std::uint64_t remaining = total;
    while (remaining > 0) {
      if (buffer.empty()) {
        auto chunk = co_await read_more();
        if (!chunk) co_return chunk.error();
        if (chunk->eof)
          co_return ErrorInfo{Error::protocol, "Truncated HTTP body"};
        buffer += chunk->data;
      }
      const auto count = static_cast<std::size_t>(
          std::min<std::uint64_t>(remaining, buffer.size()));
      auto error = co_await deliver(
          std::string_view(buffer).substr(0, count), total);
      if (error) co_return error;
      buffer.erase(0, count);
      remaining -= count;
    }
    co_return body;
  }

  if (body_until_eof) {
    if (!buffer.empty()) {
      auto error = co_await deliver(buffer, 0);
      buffer.clear();
      if (error) co_return error;
    }
    for (;;) {
      auto chunk = co_await read_more();
      if (!chunk) co_return chunk.error();
      if (chunk->eof) break;
      auto error = co_await deliver(chunk->data, 0);
      if (error) co_return error;
    }
  }
  co_return body;
}

Result<std::string> serialize_head(std::string start_line,
                                   const Headers &headers) {
  if (start_line.find_first_of("\r\n") != std::string::npos)
    return ErrorInfo{Error::protocol, "Invalid HTTP start line"};
  std::string output = std::move(start_line) + "\r\n";
  for (const auto &[name, value] : headers) {
    if (!valid_header_name(name) || !valid_header_value(value))
      return ErrorInfo{Error::protocol, "Invalid outbound HTTP header"};
    output += name;
    output += ": ";
    output += value;
    output += "\r\n";
  }
  output += "\r\n";
  return output;
}

bool status_has_no_body(int status) {
  return (status >= 100 && status < 200) || status == 204 || status == 205 ||
         status == 304;
}

bool valid_utf8(std::string_view value) noexcept {
  std::size_t index = 0;
  while (index < value.size()) {
    const auto first = static_cast<unsigned char>(value[index++]);
    if (first <= 0x7f) continue;
    unsigned continuation_count = 0;
    std::uint32_t codepoint = 0;
    if (first >= 0xc2 && first <= 0xdf) {
      continuation_count = 1;
      codepoint = first & 0x1f;
    } else if (first >= 0xe0 && first <= 0xef) {
      continuation_count = 2;
      codepoint = first & 0x0f;
    } else if (first >= 0xf0 && first <= 0xf4) {
      continuation_count = 3;
      codepoint = first & 0x07;
    } else {
      return false;
    }
    if (index + continuation_count > value.size()) return false;
    for (unsigned count = 0; count < continuation_count; ++count) {
      const auto next = static_cast<unsigned char>(value[index++]);
      if ((next & 0xc0) != 0x80) return false;
      codepoint = (codepoint << 6) | (next & 0x3f);
    }
    if ((continuation_count == 2 && codepoint < 0x800) ||
        (continuation_count == 3 && codepoint < 0x10000) ||
        codepoint > 0x10ffff || (codepoint >= 0xd800 && codepoint <= 0xdfff))
      return false;
  }
  return true;
}

bool valid_close_code(std::uint16_t code) noexcept {
  if (code < 1000 || code >= 5000) return false;
  if (code == 1004 || code == 1005 || code == 1006 || code == 1015)
    return false;
  return code <= 1014 || code >= 3000;
}

std::string random_websocket_key() {
  std::array<unsigned char, 16> bytes{};
  std::random_device random;
  for (auto &byte : bytes) byte = static_cast<unsigned char>(random());
  return base64_encode(std::string_view(reinterpret_cast<const char *>(bytes.data()),
                                        bytes.size()));
}

class UvWebSocketChannel final : public WebSocket::Channel {
public:
  UvWebSocketChannel(std::shared_ptr<Connection> connection,
                     std::string buffered, bool client_side,
                     std::string protocol, std::chrono::milliseconds timeout)
      : connection_(std::move(connection)), buffer_(std::move(buffered)),
        client_side_(client_side), protocol_(std::move(protocol)),
        timeout_(timeout), runtime_(client_side_ ? connection_->runtime()
                                                : nullptr) {}

  ~UvWebSocketChannel() override {
    if (client_side_ && runtime_) {
      connection_->close();
      stop_runtime(std::move(runtime_));
    }
  }

  Task<Result<WebSocket::Message>> read() override {
    for (;;) {
      auto ready = co_await need(2);
      if (ready) co_return ready;
      const auto first = static_cast<unsigned char>(buffer_[0]);
      const auto second = static_cast<unsigned char>(buffer_[1]);
      const bool final = (first & 0x80) != 0;
      if ((first & 0x70) != 0) {
        co_await close(1002, "RSV bits are not supported");
        co_return ErrorInfo{Error::protocol, "Invalid WebSocket RSV bits"};
      }
      const unsigned opcode = first & 0x0f;
      const bool masked = (second & 0x80) != 0;
      if (masked == client_side_) {
        co_await close(1002, "Invalid masking direction");
        co_return ErrorInfo{Error::protocol, "Invalid WebSocket mask direction"};
      }
      std::uint64_t length = second & 0x7f;
      std::size_t cursor = 2;
      if (length == 126) {
        ready = co_await need(4);
        if (ready) co_return ready;
        length = (static_cast<unsigned char>(buffer_[2]) << 8) |
                 static_cast<unsigned char>(buffer_[3]);
        if (length < 126)
          co_return ErrorInfo{Error::protocol,
                              "Non-minimal WebSocket frame length"};
        cursor = 4;
      } else if (length == 127) {
        ready = co_await need(10);
        if (ready) co_return ready;
        if ((static_cast<unsigned char>(buffer_[2]) & 0x80) != 0)
          co_return ErrorInfo{Error::protocol, "Invalid WebSocket length"};
        length = 0;
        for (int index = 0; index < 8; ++index)
          length = (length << 8) | static_cast<unsigned char>(buffer_[2 + index]);
        if (length <= 65535)
          co_return ErrorInfo{Error::protocol,
                              "Non-minimal WebSocket frame length"};
        cursor = 10;
      }
      const bool control = opcode >= 8;
      if ((control && (!final || length > 125)) || length > 64 * 1024 * 1024)
        co_return ErrorInfo{Error::protocol, "Invalid WebSocket frame length"};
      std::array<unsigned char, 4> mask{};
      if (masked) {
        ready = co_await need(cursor + 4);
        if (ready) co_return ready;
        std::memcpy(mask.data(), buffer_.data() + cursor, 4);
        cursor += 4;
      }
      if (length > std::numeric_limits<std::size_t>::max() - cursor)
        co_return ErrorInfo{Error::body_too_large, "WebSocket frame is too large"};
      ready = co_await need(cursor + static_cast<std::size_t>(length));
      if (ready) co_return ready;
      std::string payload = buffer_.substr(cursor, static_cast<std::size_t>(length));
      buffer_.erase(0, cursor + static_cast<std::size_t>(length));
      if (masked) {
        for (std::size_t index = 0; index < payload.size(); ++index)
          payload[index] ^= static_cast<char>(mask[index % 4]);
      }

      if (opcode == 8) {
        std::uint16_t code = 1000;
        std::string reason;
        if (payload.size() == 1)
          co_return ErrorInfo{Error::protocol, "Invalid WebSocket close frame"};
        if (payload.size() >= 2) {
          code = static_cast<std::uint16_t>(
              (static_cast<unsigned char>(payload[0]) << 8) |
              static_cast<unsigned char>(payload[1]));
          reason = payload.substr(2);
          if (!valid_close_code(code) || !valid_utf8(reason))
            co_return ErrorInfo{Error::protocol,
                                "Invalid WebSocket close payload"};
        }
        if (open_.exchange(false)) co_await send_frame(8, payload);
        connection_->close();
        co_return ErrorInfo{Error::websocket_closed,
                            "WebSocket closed with code " + std::to_string(code) +
                                (reason.empty() ? "" : ": " + reason)};
      }
      if (opcode == 9) {
        co_await send_frame(10, payload);
        co_return WebSocket::Message{WebSocket::MessageType::ping,
                                     std::move(payload)};
      }
      if (opcode == 10)
        co_return WebSocket::Message{WebSocket::MessageType::pong,
                                     std::move(payload)};
      if (opcode == 0) {
        if (!fragmented_active_)
          co_return ErrorInfo{Error::protocol, "Unexpected WebSocket continuation"};
        if (payload.size() > 64 * 1024 * 1024 - fragmented_.size())
          co_return ErrorInfo{Error::body_too_large, "WebSocket message is too large"};
        fragmented_ += payload;
        if (final) {
          fragmented_active_ = false;
          if (fragmented_type_ == WebSocket::MessageType::text &&
              !valid_utf8(fragmented_))
            co_return ErrorInfo{Error::protocol, "Invalid WebSocket UTF-8"};
          auto message = WebSocket::Message{fragmented_type_,
                                             std::move(fragmented_)};
          fragmented_.clear();
          co_return message;
        }
        continue;
      }
      if (opcode != 1 && opcode != 2)
        co_return ErrorInfo{Error::protocol, "Unsupported WebSocket opcode"};
      if (fragmented_active_)
        co_return ErrorInfo{Error::protocol,
                            "New WebSocket data frame during fragmentation"};
      const auto type = opcode == 1 ? WebSocket::MessageType::text
                                    : WebSocket::MessageType::binary;
      if (final) {
        if (type == WebSocket::MessageType::text && !valid_utf8(payload))
          co_return ErrorInfo{Error::protocol, "Invalid WebSocket UTF-8"};
        co_return WebSocket::Message{type, std::move(payload)};
      }
      fragmented_active_ = true;
      fragmented_type_ = type;
      fragmented_ = std::move(payload);
    }
  }

  Task<bool> send(std::string data, bool binary) override {
    if (!open_) co_return false;
    if (data.size() > 64 * 1024 * 1024 || (!binary && !valid_utf8(data)))
      co_return false;
    co_return !(co_await send_frame(binary ? 2 : 1, std::move(data)));
  }

  Task<void> ping(std::string data) override {
    if (data.size() <= 125) co_await send_frame(9, std::move(data));
  }

  Task<void> close(std::uint16_t code, std::string reason) override {
    if (!open_.exchange(false)) co_return;
    if (!valid_close_code(code) || !valid_utf8(reason)) {
      code = 1002;
      reason = "Invalid close payload";
    }
    if (reason.size() > 123) reason.resize(123);
    while (!valid_utf8(reason) && !reason.empty()) reason.pop_back();
    std::string payload;
    payload.push_back(static_cast<char>(code >> 8));
    payload.push_back(static_cast<char>(code & 0xff));
    payload += reason;
    co_await send_frame(8, std::move(payload));
    connection_->close();
  }

  bool open() const noexcept override {
    return open_ && connection_ && connection_->open();
  }
  std::string subprotocol() const override { return protocol_; }

private:
  Task<ErrorInfo> need(std::size_t count) {
    while (buffer_.size() < count) {
      auto chunk = co_await connection_->read(timeout_);
      if (!chunk) co_return chunk.error();
      if (chunk->eof) {
        open_ = false;
        co_return ErrorInfo{Error::websocket_closed, "WebSocket transport closed"};
      }
      buffer_ += chunk->data;
    }
    co_return ErrorInfo{};
  }

  Task<ErrorInfo> send_frame(unsigned opcode, std::string payload) {
    if (!connection_ || !connection_->open())
      co_return ErrorInfo{Error::websocket_closed, "WebSocket is closed"};
    while (writing_.exchange(true)) co_await chhttp::sleep_for(1ms);
    struct Unlock {
      std::atomic_bool &value;
      ~Unlock() { value = false; }
    } unlock{writing_};
    std::string frame;
    frame.push_back(static_cast<char>(0x80 | opcode));
    const unsigned char mask_bit = client_side_ ? 0x80 : 0;
    if (payload.size() <= 125) {
      frame.push_back(static_cast<char>(mask_bit | payload.size()));
    } else if (payload.size() <= 65535) {
      frame.push_back(static_cast<char>(mask_bit | 126));
      frame.push_back(static_cast<char>((payload.size() >> 8) & 0xff));
      frame.push_back(static_cast<char>(payload.size() & 0xff));
    } else {
      frame.push_back(static_cast<char>(mask_bit | 127));
      const auto length = static_cast<std::uint64_t>(payload.size());
      for (int shift = 56; shift >= 0; shift -= 8)
        frame.push_back(static_cast<char>((length >> shift) & 0xff));
    }
    if (client_side_) {
      std::array<unsigned char, 4> mask{};
      std::random_device random;
      for (auto &byte : mask) byte = static_cast<unsigned char>(random());
      frame.append(reinterpret_cast<const char *>(mask.data()), mask.size());
      for (std::size_t index = 0; index < payload.size(); ++index)
        payload[index] ^= static_cast<char>(mask[index % 4]);
    }
    frame += payload;
    co_return co_await connection_->write(std::move(frame), timeout_);
  }

  std::shared_ptr<Connection> connection_;
  std::string buffer_;
  bool client_side_{false};
  std::string protocol_;
  std::chrono::milliseconds timeout_;
  std::atomic_bool open_{true};
  std::atomic_bool writing_{false};
  std::string fragmented_;
  WebSocket::MessageType fragmented_type_{WebSocket::MessageType::text};
  bool fragmented_active_{false};
  std::shared_ptr<Runtime> runtime_;
};

} // namespace

Task<Result<RequestHeadResult>>
read_request_head(const std::shared_ptr<Connection> &connection,
                  std::string &buffer, const HttpReadOptions &options) {
  auto bytes = co_await read_head_bytes(connection, buffer, options);
  if (!bytes) co_return bytes.error();
  auto head = parse_head(*bytes);
  if (!head) co_return head.error();
  Request request;
  const auto first_space = head->start_line.find(' ');
  const auto last_space = head->start_line.rfind(' ');
  if (first_space == std::string::npos || last_space == first_space ||
      head->start_line.find(' ', first_space + 1) != last_space)
    co_return ErrorInfo{Error::protocol, "Malformed HTTP request line"};
  request.method = head->start_line.substr(0, first_space);
  request.target = head->start_line.substr(first_space + 1,
                                            last_space - first_space - 1);
  const bool invalid_target = std::ranges::any_of(
      request.target, [](unsigned char ch) { return ch <= 0x20 || ch == 0x7f; });
  if (!valid_header_name(request.method) || request.target.empty() || invalid_target)
    co_return ErrorInfo{Error::protocol, "Invalid HTTP request line"};
  if (!parse_version(std::string_view(head->start_line).substr(last_space + 1),
                     request.version))
    co_return ErrorInfo{Error::protocol, "Unsupported HTTP version"};
  if (request.version == 11 && head->headers.get_all("Host").size() != 1)
    co_return ErrorInfo{Error::protocol,
                        "HTTP/1.1 request must have exactly one Host header"};
  if (request.version == 11 &&
      (trim(head->headers.get("Host")).empty() ||
       head->headers.get("Host").find(',') != std::string::npos))
    co_return ErrorInfo{Error::protocol, "Invalid HTTP Host header"};
  request.headers = head->headers;
  request.keep_alive = keep_alive(request.version, request.headers);
  std::string routing_target = request.target;
  if (routing_target.find("://") != std::string::npos) {
    auto absolute = parse_url(routing_target);
    if (!absolute)
      co_return ErrorInfo{Error::protocol,
                          "Invalid absolute HTTP request target: " +
                              absolute.error().message};
    routing_target = absolute->target;
  }
  const auto query = routing_target.find('?');
  request.path = routing_target.substr(0, query);
  if (request.path.empty()) request.path = "/";
  auto decoded_path = url_decode(request.path);
  if (!decoded_path)
    co_return ErrorInfo{Error::protocol,
                        "Invalid encoded HTTP request path: " +
                            decoded_path.error().message};
  request.path = std::move(*decoded_path);
  if (query != std::string::npos)
    request.query = parse_query(std::string_view(routing_target).substr(query + 1));
  bool expect_continue = false;
  if (head->headers.contains("Expect")) {
    if (!has_token(head->headers, "Expect", "100-continue"))
      co_return ErrorInfo{Error::protocol, "Unsupported HTTP expectation"};
    expect_continue = true;
  }
  RequestBodyState body_state{
      .headers = std::move(head->headers),
      .content_length = head->content_length,
      .chunked = head->chunked,
      .expect_continue = expect_continue};
  request.headers = body_state.headers;
  co_return RequestHeadResult{std::move(request), std::move(body_state)};
}

Task<Result<std::string>>
read_request_body(const std::shared_ptr<Connection> &connection,
                  std::string &buffer, RequestBodyState &state,
                  const HttpReadOptions &options) {
  if (state.expect_continue && !state.continue_sent) {
    auto continued = co_await connection->write("HTTP/1.1 100 Continue\r\n\r\n",
                                                 options.read_timeout);
    if (continued) co_return continued;
    state.continue_sent = true;
  }
  ParsedHead head{
      .headers = state.headers,
      .content_length = state.content_length,
      .chunked = state.chunked};
  HttpReadOptions body_options = options;
  const auto encoding = state.headers.get("Content-Encoding");
  const bool decode = options.auto_decompress && !encoding.empty();
  std::unique_ptr<StreamingDecoder> decoder;
  std::string decoded_body;
  std::uint64_t decoded_received = 0;
  ErrorInfo decode_error;
  const auto consume_decoded = [&](std::string_view data) -> Task<ErrorInfo> {
    if (data.empty()) co_return ErrorInfo{};
    if (data.size() > options.max_body_size ||
        decoded_received > options.max_body_size - data.size())
      co_return ErrorInfo{Error::body_too_large,
                          "Decoded request body exceeds configured limit"};
    decoded_received += data.size();
    try {
      if (options.on_data) {
        if (!options.on_data(data))
          co_return ErrorInfo{Error::cancelled,
                              "Request body callback cancelled the transfer"};
      } else if (options.on_data_async) {
        if (!co_await options.on_data_async(data))
          co_return ErrorInfo{
              Error::cancelled,
              "Request async body callback cancelled the transfer"};
      } else {
        decoded_body.append(data);
      }
      if (options.on_progress &&
          !options.on_progress(decoded_received, 0))
        co_return ErrorInfo{Error::cancelled,
                            "Request progress callback cancelled the transfer"};
    } catch (const std::exception &exception) {
      co_return ErrorInfo{Error::internal,
                          "Request body callback failed: " +
                              std::string(exception.what())};
    } catch (...) {
      co_return ErrorInfo{Error::internal, "Request body callback failed"};
    }
    co_return ErrorInfo{};
  };
  if (decode) {
    auto created = StreamingDecoder::create(encoding, options.max_body_size);
    if (!created) co_return created.error();
    decoder = std::move(*created);
    body_options.on_data = {};
    body_options.on_data_async = [&](std::string_view data) -> Task<bool> {
      auto decoded = decoder->feed(data, false);
      if (!decoded) {
        decode_error = decoded.error();
        co_return false;
      }
      auto consumed = co_await consume_decoded(*decoded);
      if (consumed) {
        decode_error = std::move(consumed);
        co_return false;
      }
      co_return true;
    };
    body_options.on_progress = {};
  }
  auto body = co_await read_body(connection, buffer, head, body_options, false,
                                 false);
  if (!body) co_return decode_error ? decode_error : body.error();
  state.headers = std::move(head.headers);
  if (decode) {
    auto tail = decoder->feed({}, true);
    if (!tail) co_return tail.error();
    if (auto consumed = co_await consume_decoded(*tail); consumed)
      co_return consumed;
    state.headers.erase("Content-Encoding");
    state.headers.erase("Content-Length");
    co_return decoded_body;
  }
  co_return body;
}

Task<Result<Request>> read_request(const std::shared_ptr<Connection> &connection,
                                   std::string &buffer,
                                   const HttpReadOptions &options) {
  auto head = co_await read_request_head(connection, buffer, options);
  if (!head) co_return head.error();
  auto body = co_await read_request_body(connection, buffer, head->body, options);
  if (!body) co_return body.error();
  auto request = std::move(head->request);
  request.body = std::move(*body);
  request.headers = std::move(head->body.headers);
  co_return request;
}

Task<ResponseResult> read_response(
    const std::shared_ptr<Connection> &connection, std::string &buffer,
    std::string_view request_method, const HttpReadOptions &options) {
  for (;;) {
    auto bytes = co_await read_head_bytes(connection, buffer, options);
    if (!bytes) co_return bytes.error();
    auto head = parse_head(*bytes);
    if (!head) co_return head.error();
    const auto first_space = head->start_line.find(' ');
    const auto second_space = head->start_line.find(' ', first_space + 1);
    if (first_space == std::string::npos)
      co_return ErrorInfo{Error::protocol, "Malformed HTTP status line"};
    Response response;
    if (!parse_version(std::string_view(head->start_line).substr(0, first_space),
                       response.version))
      co_return ErrorInfo{Error::protocol, "Unsupported HTTP response version"};
    const auto status_text = std::string_view(head->start_line).substr(
        first_space + 1, second_space == std::string::npos
                             ? std::string::npos
                             : second_space - first_space - 1);
    const auto conversion = std::from_chars(status_text.data(),
                                             status_text.data() + status_text.size(),
                                             response.status);
    if (status_text.size() != 3 || conversion.ec != std::errc{} ||
        conversion.ptr != status_text.data() + status_text.size() ||
        response.status < 100)
      co_return ErrorInfo{Error::protocol, "Invalid HTTP response status"};
    response.headers = head->headers;
    response.keep_alive = keep_alive(response.version, response.headers);
    const bool no_body = iequals(request_method, "HEAD") ||
                         (iequals(request_method, "CONNECT") &&
                          response.status >= 200 && response.status < 300) ||
                         status_has_no_body(response.status);
    const bool until_eof = !no_body && !head->chunked && !head->content_length;
    if (until_eof) response.keep_alive = false;
    const bool informational = response.status >= 100 && response.status < 200 &&
                               response.status != 101;
    if (!informational && options.on_response_head) {
      try {
        if (!options.on_response_head(ResponseHead{
                .status = response.status,
                .version = response.version,
                .headers = response.headers,
                .keep_alive = response.keep_alive}))
          co_return ErrorInfo{Error::cancelled,
                              "HTTP response head callback rejected the response"};
        if (options.deadline &&
            std::chrono::steady_clock::now() >= *options.deadline)
          co_return ErrorInfo{Error::timeout, "Request deadline exceeded"};
      } catch (const std::exception &exception) {
        co_return ErrorInfo{Error::internal,
                            "HTTP response head callback failed: " +
                                std::string(exception.what())};
      } catch (...) {
        co_return ErrorInfo{Error::internal,
                            "HTTP response head callback failed"};
      }
    }
    HttpReadOptions body_options = options;
    const auto encoding = response.headers.get("Content-Encoding");
    const bool decode = options.auto_decompress && !encoding.empty();
    std::unique_ptr<StreamingDecoder> decoder;
    std::string decoded_body;
    std::uint64_t decoded_received = 0;
    ErrorInfo decode_error;
    const auto consume_decoded = [&](std::string_view data) -> Task<ErrorInfo> {
      if (data.empty()) co_return ErrorInfo{};
      if (options.deadline &&
          std::chrono::steady_clock::now() >= *options.deadline)
        co_return ErrorInfo{Error::timeout, "Request deadline exceeded"};
      decoded_received += data.size();
      try {
        if (options.on_data) {
          if (!options.on_data(data))
            co_return ErrorInfo{Error::cancelled,
                                "HTTP body callback cancelled the transfer"};
        } else if (options.on_data_async) {
          if (!co_await options.on_data_async(data))
            co_return ErrorInfo{
                Error::cancelled,
                "HTTP async body callback cancelled the transfer"};
        } else {
          decoded_body.append(data);
        }
        if (options.on_progress &&
            !options.on_progress(decoded_received, 0))
          co_return ErrorInfo{
              Error::cancelled,
              "HTTP progress callback cancelled the transfer"};
        if (options.deadline &&
            std::chrono::steady_clock::now() >= *options.deadline)
          co_return ErrorInfo{Error::timeout, "Request deadline exceeded"};
      } catch (const std::exception &exception) {
        co_return ErrorInfo{Error::internal,
                            "HTTP body callback failed: " +
                                std::string(exception.what())};
      } catch (...) {
        co_return ErrorInfo{Error::internal, "HTTP body callback failed"};
      }
      co_return ErrorInfo{};
    };
    if (decode) {
      auto created = StreamingDecoder::create(encoding, options.max_body_size);
      if (!created) co_return created.error();
      decoder = std::move(*created);
      body_options.on_data = {};
      body_options.on_data_async =
          [&](std::string_view data) -> Task<bool> {
        auto decoded = decoder->feed(data, false);
        if (!decoded) {
          decode_error = decoded.error();
          co_return false;
        }
        auto consumed = co_await consume_decoded(*decoded);
        if (consumed) {
          decode_error = std::move(consumed);
          co_return false;
        }
        co_return true;
      };
      body_options.on_progress = {};
    }
    auto body = co_await read_body(connection, buffer, *head, body_options,
                                   until_eof, no_body);
    if (!body) co_return decode_error ? decode_error : body.error();
    response.body = std::move(*body);
    response.headers = std::move(head->headers);
    if (decode) {
      auto tail = decoder->feed({}, true);
      if (!tail) co_return tail.error();
      if (auto consumed = co_await consume_decoded(*tail); consumed)
        co_return consumed;
      response.body = std::move(decoded_body);
      response.headers.erase("Content-Encoding");
      if (options.on_data || options.on_data_async)
        response.headers.erase("Content-Length");
      else
        response.headers.set("Content-Length",
                             std::to_string(response.body.size()));
    }
    if (informational) continue;
    co_return response;
  }
}

namespace {

class RequestBodySink final : public StreamWriter::Sink {
public:
  RequestBodySink(std::shared_ptr<Connection> connection, bool chunked,
                  std::optional<std::uint64_t> length,
                  std::chrono::milliseconds timeout)
      : connection_(std::move(connection)), chunked_(chunked),
        remaining_(length), timeout_(timeout) {}

  Task<bool> write(std::string data) override {
    if (error_ || finished_ || !connection_->open()) co_return false;
    if (data.empty()) co_return true;
    if (remaining_ && data.size() > *remaining_) {
      error_ = {Error::protocol,
                "Streamed request body exceeds declared Content-Length"};
      co_return false;
    }
    const auto size = data.size();
    auto error = chunked_
                     ? co_await write_chunk(connection_, data, timeout_)
                     : co_await connection_->write(std::move(data), timeout_);
    if (error) {
      error_ = std::move(error);
      co_return false;
    }
    if (remaining_) *remaining_ -= size;
    co_return true;
  }

  Task<bool> flush() override { co_return open(); }

  bool open() const noexcept override {
    return !error_ && !finished_ && connection_ && connection_->open();
  }

  Task<ErrorInfo> finish() {
    if (finished_) co_return error_;
    finished_ = true;
    if (error_) co_return error_;
    if (remaining_ && *remaining_ != 0)
      co_return ErrorInfo{
          Error::protocol,
          "Streamed request body is shorter than declared Content-Length"};
    if (chunked_) co_return co_await write_last_chunk(connection_, timeout_);
    co_return ErrorInfo{};
  }

private:
  std::shared_ptr<Connection> connection_;
  bool chunked_{false};
  std::optional<std::uint64_t> remaining_;
  std::chrono::milliseconds timeout_;
  ErrorInfo error_;
  bool finished_{false};
};

} // namespace

Task<ErrorInfo> write_request(const std::shared_ptr<Connection> &connection,
                              const Request &request,
                              std::string_view wire_target,
                              std::chrono::milliseconds timeout) {
  const bool invalid_target = std::ranges::any_of(
      wire_target, [](unsigned char ch) { return ch <= 0x20 || ch == 0x7f; });
  if (!valid_header_name(request.method) || wire_target.empty() ||
      invalid_target || (request.version != 10 && request.version != 11))
    co_return ErrorInfo{Error::protocol, "Invalid outbound HTTP request line"};
  if (request.body_stream && !request.body.empty())
    co_return ErrorInfo{Error::invalid_argument,
                        "Configure either a buffered or streamed request body"};
  const bool streaming = static_cast<bool>(request.body_stream);
  const bool chunked = streaming && !request.body_stream_length;
  if (chunked && request.version == 10)
    co_return ErrorInfo{
        Error::invalid_argument,
        "HTTP/1.0 streamed request bodies require a Content-Length"};

  Headers headers = request.headers;
  headers.erase("Transfer-Encoding");
  if (chunked) {
    headers.erase("Content-Length");
    headers.set("Transfer-Encoding", "chunked");
  } else if (streaming) {
    headers.set("Content-Length", std::to_string(*request.body_stream_length));
  } else {
    headers.set("Content-Length", std::to_string(request.body.size()));
  }
  if (!headers.contains("Connection"))
    headers.set("Connection", request.keep_alive ? "keep-alive" : "close");
  if (!headers.contains("User-Agent")) headers.set("User-Agent", "chhttp/0.4");
  auto output = serialize_head(request.method + " " + std::string(wire_target) +
                                   (request.version == 10 ? " HTTP/1.0" : " HTTP/1.1"),
                               headers);
  if (!output) co_return output.error();
  if (streaming) {
    if (auto error = co_await connection->write(std::move(*output), timeout);
        error)
      co_return error;
    auto sink = std::make_shared<RequestBodySink>(
        connection, chunked, request.body_stream_length, timeout);
    StreamWriter writer(sink);
    try {
      co_await request.body_stream(writer);
    } catch (const std::exception &exception) {
      co_return ErrorInfo{Error::internal,
                          "Streamed request producer failed: " +
                              std::string(exception.what())};
    } catch (...) {
      co_return ErrorInfo{Error::internal,
                          "Streamed request producer failed"};
    }
    co_return co_await sink->finish();
  }
  *output += request.body;
  co_return co_await connection->write(std::move(*output), timeout);
}

Task<ErrorInfo> write_response_head(
    const std::shared_ptr<Connection> &connection, const Request &request,
    const Response &response, bool chunked,
    std::optional<std::uint64_t> content_length,
    std::chrono::milliseconds timeout) {
  if ((response.version != 10 && response.version != 11) ||
      response.status < 100 || response.status > 999)
    co_return ErrorInfo{Error::protocol, "Invalid outbound HTTP response line"};
  Headers headers = response.headers;
  if (!headers.contains("Server")) headers.set("Server", "chhttp/0.4");
  const bool keep = request.keep_alive && response.keep_alive;
  headers.set("Connection", keep ? "keep-alive" : "close");
  if ((response.status >= 100 && response.status < 200) ||
      response.status == 204) {
    headers.erase("Content-Length");
    headers.erase("Transfer-Encoding");
  } else if (response.status == 205) {
    headers.erase("Transfer-Encoding");
    headers.set("Content-Length", "0");
  } else if (chunked) {
    headers.erase("Content-Length");
    headers.set("Transfer-Encoding", "chunked");
  } else if (content_length) {
    headers.erase("Transfer-Encoding");
    headers.set("Content-Length", std::to_string(*content_length));
  } else {
    headers.erase("Transfer-Encoding");
    headers.erase("Content-Length");
  }
  auto output = serialize_head(
      std::string(response.version == 10 ? "HTTP/1.0 " : "HTTP/1.1 ") +
          std::to_string(response.status) + " " + status_reason(response.status),
      headers);
  if (!output) co_return output.error();
  co_return co_await connection->write(std::move(*output), timeout);
}

Task<ErrorInfo> write_response(const std::shared_ptr<Connection> &connection,
                               const Request &request, const Response &response,
                               std::chrono::milliseconds timeout) {
  const bool omit = iequals(request.method, "HEAD") ||
                    status_has_no_body(response.status);
  auto error = co_await write_response_head(connection, request, response, false,
                                             response.body.size(), timeout);
  if (error || omit || response.body.empty()) co_return error;
  co_return co_await connection->write(response.body, timeout);
}

Task<ErrorInfo> write_chunk(const std::shared_ptr<Connection> &connection,
                            std::string_view data,
                            std::chrono::milliseconds timeout) {
  std::ostringstream size;
  size << std::hex << data.size();
  co_return co_await connection->write(size.str() + "\r\n" + std::string(data) +
                                            "\r\n",
                                        timeout);
}

Task<ErrorInfo> write_last_chunk(
    const std::shared_ptr<Connection> &connection,
    std::chrono::milliseconds timeout) {
  co_return co_await connection->write("0\r\n\r\n", timeout);
}

bool is_websocket_upgrade(const Request &request) {
  return iequals(request.method, "GET") &&
         has_token(request.headers, "Connection", "upgrade") &&
         has_token(request.headers, "Upgrade", "websocket") &&
         request.headers.get_all("Sec-WebSocket-Version").size() == 1 &&
         request.headers.get_all("Sec-WebSocket-Key").size() == 1 &&
         request.headers.get("Sec-WebSocket-Version") == "13" &&
         request.headers.contains("Sec-WebSocket-Key");
}

Task<ErrorInfo> websocket_server_handshake(
    const std::shared_ptr<Connection> &connection, const Request &request,
    std::string_view subprotocol, std::chrono::milliseconds timeout) {
  const auto key = request.headers.get("Sec-WebSocket-Key");
  auto decoded = base64_decode(key);
  if (!decoded || decoded->size() != 16)
    co_return ErrorInfo{Error::websocket_handshake,
                        "Invalid Sec-WebSocket-Key"};
  Headers headers{{"Upgrade", "websocket"},
                  {"Connection", "Upgrade"},
                  {"Sec-WebSocket-Accept", websocket_accept(key)}};
  if (!subprotocol.empty())
    headers.add("Sec-WebSocket-Protocol", std::string(subprotocol));
  auto response = serialize_head("HTTP/1.1 101 Switching Protocols", headers);
  if (!response) co_return response.error();
  co_return co_await connection->write(std::move(*response), timeout);
}

std::shared_ptr<WebSocket::Channel> make_websocket_channel(
    std::shared_ptr<Connection> connection, std::string buffered,
    bool client_side, std::string subprotocol,
    std::chrono::milliseconds timeout) {
  return std::make_shared<UvWebSocketChannel>(
      std::move(connection), std::move(buffered), client_side,
      std::move(subprotocol), timeout);
}

Task<Result<std::shared_ptr<WebSocket>>> websocket_client_connect(
    std::shared_ptr<Runtime> runtime, std::string url, Headers headers,
    ClientOptions options) {
  auto parsed = parse_url(url);
  if (!parsed) co_return parsed.error();
  if (parsed->scheme != "ws" && parsed->scheme != "wss")
    co_return ErrorInfo{Error::invalid_url, "WebSocket URL must use ws or wss"};
  auto connected = co_await Connection::connect(runtime, parsed->host,
                                                 parsed->port,
                                                 options.connect_timeout);
  if (!connected) co_return connected.error();
  auto connection = *connected;
  if (auto error = connection->set_no_delay(options.tcp_no_delay)) {
    connection->close();
    co_return error;
  }
#ifdef CHHTTP_HAS_TLS
  if (parsed->secure) {
    ErrorInfo context_error;
    SSL_CTX *context = create_client_tls_context(options.tls, context_error);
    if (!context) {
      connection->close();
      co_return context_error;
    }
    auto tls_error = connection->enable_tls(
        context, false,
        options.tls.server_name.empty() ? parsed->host : options.tls.server_name);
    SSL_CTX_free(context);
    if (!tls_error) tls_error = co_await connection->handshake(options.connect_timeout);
    if (tls_error) {
      connection->close();
      co_return tls_error;
    }
  }
#else
  if (parsed->secure) {
    connection->close();
    co_return ErrorInfo{Error::tls_unavailable, "TLS support is disabled"};
  }
#endif
  const std::string key = random_websocket_key();
  std::vector<std::string> requested_protocols;
  for (const auto &value : headers.get_all("Sec-WebSocket-Protocol")) {
    auto protocols = split_tokens(value, ',');
    requested_protocols.insert(requested_protocols.end(),
                               protocols.begin(), protocols.end());
  }
  if (std::ranges::any_of(requested_protocols,
                          [](const std::string &protocol) {
                            return !valid_header_name(protocol);
                          })) {
    connection->close();
    co_return ErrorInfo{Error::websocket_handshake,
                        "Invalid requested WebSocket subprotocol"};
  }
  Request request;
  request.method = "GET";
  request.target = parsed->target;
  request.headers = std::move(headers);
  request.headers.set("Host", parsed->authority());
  request.headers.set("Upgrade", "websocket");
  request.headers.set("Connection", "Upgrade");
  request.headers.set("Sec-WebSocket-Version", "13");
  request.headers.set("Sec-WebSocket-Key", key);
  auto written = co_await write_request(connection, request, parsed->target,
                                         options.write_timeout);
  if (written) {
    connection->close();
    co_return written;
  }
  std::string buffered;
  auto response = co_await read_response(
      connection, buffered, "GET",
      {.max_header_size = 64 * 1024,
       .max_body_size = options.max_response_body_size,
       .read_timeout = options.read_timeout});
  if (!response || response->status != 101 ||
      !has_token(response->headers, "Upgrade", "websocket") ||
      !has_token(response->headers, "Connection", "upgrade") ||
      response->headers.get_all("Sec-WebSocket-Accept").size() != 1 ||
      response->headers.get_all("Sec-WebSocket-Protocol").size() > 1 ||
      response->headers.get("Sec-WebSocket-Accept") != websocket_accept(key)) {
    connection->close();
    co_return response
                  ? ErrorInfo{Error::websocket_handshake,
                              "WebSocket upgrade was rejected"}
                  : response.error();
  }
  const auto selected_protocol =
      response->headers.get("Sec-WebSocket-Protocol");
  if (!selected_protocol.empty() &&
      (!valid_header_name(selected_protocol) ||
       std::ranges::find(requested_protocols, selected_protocol) ==
           requested_protocols.end())) {
    connection->close();
    co_return ErrorInfo{Error::websocket_handshake,
                        "Server selected an unrequested WebSocket subprotocol"};
  }
  auto channel = make_websocket_channel(
      connection, std::move(buffered), true,
      selected_protocol, options.read_timeout);
  co_return std::make_shared<WebSocket>(std::move(channel));
}

} // namespace chhttp::detail
