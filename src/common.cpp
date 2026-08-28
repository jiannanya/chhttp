#include "detail.hpp"

#ifdef CHHTTP_HAS_CRYPTO
#include <openssl/evp.h>
#include <openssl/sha.h>
#endif

#ifdef CHHTTP_HAS_COMPRESSION
#include <brotli/decode.h>
#include <brotli/encode.h>
#include <zlib.h>
#include <zstd.h>
#endif

#include <array>
#include <charconv>
#include <fstream>
#include <iomanip>
#include <limits>
#include <stdexcept>

namespace chhttp {

Headers::Headers(std::initializer_list<value_type> values) : values_(values) {}

void Headers::add(std::string name, std::string value) {
  values_.emplace_back(std::move(name), std::move(value));
}

void Headers::set(std::string name, std::string value) {
  erase(name);
  add(std::move(name), std::move(value));
}

bool Headers::erase(std::string_view name) {
  const auto old_size = values_.size();
  std::erase_if(values_, [&](const value_type &item) {
    return detail::iequals(item.first, name);
  });
  return values_.size() != old_size;
}

bool Headers::contains(std::string_view name) const {
  return std::ranges::any_of(values_, [&](const value_type &item) {
    return detail::iequals(item.first, name);
  });
}

std::string Headers::get(std::string_view name, std::string_view fallback) const {
  const auto found = std::ranges::find_if(values_, [&](const value_type &item) {
    return detail::iequals(item.first, name);
  });
  return found == values_.end() ? std::string(fallback) : found->second;
}

std::vector<std::string> Headers::get_all(std::string_view name) const {
  std::vector<std::string> result;
  for (const auto &[key, value] : values_) {
    if (detail::iequals(key, name)) {
      result.push_back(value);
    }
  }
  return result;
}

bool Request::has_header(std::string_view name) const {
  return headers.contains(name);
}

std::string Request::get_header(std::string_view name,
                                std::string_view fallback) const {
  return headers.get(name, fallback);
}

bool Request::has_param(std::string_view name) const {
  return std::ranges::any_of(query, [&](const auto &item) {
    return item.first == name;
  });
}

std::string Request::get_param(std::string_view name,
                               std::string_view fallback) const {
  const auto found = std::ranges::find_if(query, [&](const auto &item) {
    return item.first == name;
  });
  return found == query.end() ? std::string(fallback) : found->second;
}

void Request::set_stream_body(
    StreamHandler handler,
    std::optional<std::uint64_t> content_length) {
  body.clear();
  body_stream = std::move(handler);
  body_stream_length = content_length;
}

bool Request::is_streaming_body() const noexcept {
  return static_cast<bool>(body_stream);
}

Task<bool> StreamWriter::write(std::string_view data) {
  if (!sink_) {
    co_return false;
  }
  co_return co_await sink_->write(std::string(data));
}

Task<bool> StreamWriter::write(std::span<const std::byte> data) {
  const auto *ptr = reinterpret_cast<const char *>(data.data());
  co_return co_await write(std::string_view(ptr, data.size()));
}

Task<bool> StreamWriter::flush() {
  if (!sink_) {
    co_return false;
  }
  co_return co_await sink_->flush();
}

bool StreamWriter::open() const noexcept { return sink_ && sink_->open(); }

Task<bool> SseWriter::send(const SseEvent &event) {
  co_return co_await writer_.write(format_sse(event));
}

Task<bool> SseWriter::data(std::string_view value) {
  co_return co_await send(SseEvent{.data = std::string(value)});
}

Task<bool> SseWriter::comment(std::string_view value) {
  std::string line = ": ";
  line.append(value);
  line.append("\n\n");
  co_return co_await writer_.write(line);
}

void Response::set_header(std::string name, std::string value) {
  headers.set(std::move(name), std::move(value));
}

void Response::set_content(std::string value, std::string_view content_type) {
  body = std::move(value);
  file_path_.reset();
  stream_handler_ = {};
  headers.set("Content-Type", std::string(content_type));
}

void Response::set_redirect(std::string location, int redirect_status) {
  status = redirect_status;
  headers.set("Location", std::move(location));
}

void Response::set_file(std::filesystem::path path,
                        std::string_view content_type) {
  file_path_ = std::move(path);
  body.clear();
  stream_handler_ = {};
  headers.set("Content-Type", content_type.empty() ? mime_type(*file_path_)
                                                   : std::string(content_type));
}

void Response::set_stream(std::string content_type, StreamHandler handler) {
  body.clear();
  file_path_.reset();
  stream_handler_ = std::move(handler);
  headers.set("Content-Type", std::move(content_type));
}

void Response::set_sse(SseHandler handler) {
  set_stream("text/event-stream; charset=utf-8",
             [handler = std::move(handler)](StreamWriter &writer)
                 -> Task<void> {
               SseWriter sse(writer);
               co_await handler(sse);
             });
  headers.set("Cache-Control", "no-cache");
  headers.set("X-Accel-Buffering", "no");
}

bool Response::is_streaming() const noexcept {
  return static_cast<bool>(stream_handler_);
}

Task<Result<WebSocket::Message>> WebSocket::read() {
  if (!channel_) {
    co_return ErrorInfo{Error::websocket_closed, "WebSocket is not connected"};
  }
  co_return co_await channel_->read();
}

Task<bool> WebSocket::send_text(std::string_view data) {
  if (!channel_) co_return false;
  co_return co_await channel_->send(std::string(data), false);
}

Task<bool>
WebSocket::send_binary(std::span<const std::byte> data) {
  if (!channel_) co_return false;
  const auto *ptr = reinterpret_cast<const char *>(data.data());
  co_return co_await channel_->send(std::string(ptr, data.size()), true);
}

Task<void> WebSocket::ping(std::string_view data) {
  if (channel_) {
    co_await channel_->ping(std::string(data));
  }
}

Task<void> WebSocket::close(std::uint16_t code, std::string_view reason) {
  if (channel_) {
    co_await channel_->close(code, std::string(reason));
  }
}

bool WebSocket::open() const noexcept { return channel_ && channel_->open(); }

std::string WebSocket::subprotocol() const {
  return channel_ ? channel_->subprotocol() : std::string{};
}

std::string url_encode(std::string_view value, bool space_as_plus) {
  constexpr char hex[] = "0123456789ABCDEF";
  std::string output;
  output.reserve(value.size() * 3 / 2);
  for (const unsigned char ch : value) {
    if (std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
      output.push_back(static_cast<char>(ch));
    } else if (space_as_plus && ch == ' ') {
      output.push_back('+');
    } else {
      output.push_back('%');
      output.push_back(hex[ch >> 4]);
      output.push_back(hex[ch & 0x0f]);
    }
  }
  return output;
}

Result<std::string> url_decode(std::string_view value, bool plus_as_space) {
  std::string output;
  output.reserve(value.size());
  const auto hex_value = [](char ch) -> int {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
  };
  for (std::size_t index = 0; index < value.size(); ++index) {
    if (value[index] == '%') {
      if (index + 2 >= value.size())
        return ErrorInfo{Error::invalid_url, "Truncated percent escape"};
      const int high = hex_value(value[index + 1]);
      const int low = hex_value(value[index + 2]);
      if (high < 0 || low < 0) {
        return ErrorInfo{Error::invalid_url, "Invalid percent escape"};
      }
      output.push_back(static_cast<char>((high << 4) | low));
      index += 2;
    } else if (plus_as_space && value[index] == '+') {
      output.push_back(' ');
    } else {
      output.push_back(value[index]);
    }
  }
  return output;
}

Params parse_query(std::string_view query) {
  Params result;
  if (!query.empty() && query.front() == '?') query.remove_prefix(1);
  std::size_t start = 0;
  while (start <= query.size()) {
    const auto end = query.find('&', start);
    const auto part = query.substr(start, end == std::string_view::npos
                                             ? query.size() - start
                                             : end - start);
    if (!part.empty()) {
      const auto equal = part.find('=');
      const auto key = url_decode(part.substr(0, equal), true);
      const auto value = equal == std::string_view::npos
                             ? Result<std::string>(std::string{})
                             : url_decode(part.substr(equal + 1), true);
      if (key && value) result.emplace_back(*key, *value);
    }
    if (end == std::string_view::npos) break;
    start = end + 1;
  }
  return result;
}

std::string make_query(const Params &params) {
  std::string output;
  for (const auto &[key, value] : params) {
    if (!output.empty()) output.push_back('&');
    output += url_encode(key, true);
    output.push_back('=');
    output += url_encode(value, true);
  }
  return output;
}

namespace {

std::vector<std::string> split_header_parameters(std::string_view value) {
  std::vector<std::string> parameters;
  std::size_t start = 0;
  bool quoted = false;
  bool escaped = false;
  for (std::size_t index = 0; index <= value.size(); ++index) {
    const char ch = index < value.size() ? value[index] : ';';
    if (escaped) {
      escaped = false;
      continue;
    }
    if (quoted && ch == '\\') {
      escaped = true;
      continue;
    }
    if (ch == '"') quoted = !quoted;
    if (ch == ';' && !quoted) {
      parameters.push_back(detail::trim(value.substr(start, index - start)));
      start = index + 1;
    }
  }
  return parameters;
}

Result<std::string> decode_header_parameter(std::string_view value) {
  auto trimmed = detail::trim(value);
  if (trimmed.empty() || trimmed.front() != '"') return trimmed;
  if (trimmed.size() < 2 || trimmed.back() != '"')
    return ErrorInfo{Error::multipart, "Unterminated quoted parameter"};
  std::string decoded;
  for (std::size_t index = 1; index + 1 < trimmed.size(); ++index) {
    if (trimmed[index] == '\\' && index + 2 < trimmed.size()) ++index;
    decoded.push_back(trimmed[index]);
  }
  return decoded;
}

bool valid_multipart_boundary(std::string_view boundary) noexcept {
  if (boundary.empty() || boundary.size() > 70 || boundary.back() == ' ')
    return false;
  constexpr std::string_view punctuation = "'()+_,-./:=? ";
  return std::ranges::all_of(boundary, [&](unsigned char ch) {
    return std::isalnum(ch) || punctuation.find(static_cast<char>(ch)) !=
                                   std::string_view::npos;
  });
}

std::string quote_multipart_parameter(std::string_view value) {
  std::string quoted;
  quoted.reserve(value.size());
  for (const char ch : value) {
    if (ch == '\r' || ch == '\n') continue;
    if (ch == '\\' || ch == '"') quoted.push_back('\\');
    quoted.push_back(ch);
  }
  return quoted;
}

} // namespace

Result<MultipartForm> parse_multipart(std::string_view body,
                                     std::string_view content_type,
                                     std::size_t max_parts) {
  std::string boundary;
  const auto parameters = split_header_parameters(content_type);
  if (parameters.empty() ||
      !detail::iequals(parameters.front(), "multipart/form-data"))
    return ErrorInfo{Error::multipart, "Content-Type is not multipart/form-data"};
  for (std::size_t index = 1; index < parameters.size(); ++index) {
    const auto equal = parameters[index].find('=');
    if (equal == std::string::npos ||
        !detail::iequals(detail::trim(std::string_view(parameters[index])
                                         .substr(0, equal)),
                         "boundary"))
      continue;
    auto decoded = decode_header_parameter(
        std::string_view(parameters[index]).substr(equal + 1));
    if (!decoded) return decoded.error();
    boundary = std::move(*decoded);
    break;
  }
  if (boundary.empty()) {
    return ErrorInfo{Error::multipart, "Missing multipart boundary"};
  }
  if (!valid_multipart_boundary(boundary)) {
    return ErrorInfo{Error::multipart, "Invalid multipart boundary"};
  }
  const std::string marker = "--" + boundary;
  MultipartForm parts;
  std::size_t cursor = 0;
  bool closed = false;
  while (true) {
    auto begin = body.find(marker, cursor);
    if (begin == std::string_view::npos) break;
    begin += marker.size();
    if (body.substr(begin, 2) == "--") {
      closed = true;
      break;
    }
    if (body.substr(begin, 2) != "\r\n") {
      return ErrorInfo{Error::multipart, "Malformed multipart delimiter"};
    }
    begin += 2;
    const auto header_end = body.find("\r\n\r\n", begin);
    if (header_end == std::string_view::npos) {
      return ErrorInfo{Error::multipart, "Incomplete multipart headers"};
    }
    MultipartPart part;
    std::size_t line_start = begin;
    while (line_start < header_end) {
      const auto line_end = body.find("\r\n", line_start);
      const auto effective_end = std::min(line_end, header_end);
      const auto line = body.substr(line_start, effective_end - line_start);
      const auto colon = line.find(':');
      if (colon == std::string_view::npos) {
        return ErrorInfo{Error::multipart, "Malformed multipart header"};
      }
      auto name = detail::trim(line.substr(0, colon));
      auto value = detail::trim(line.substr(colon + 1));
      if (!detail::valid_header_name(name) ||
          !detail::valid_header_value(value))
        return ErrorInfo{Error::multipart, "Invalid multipart header"};
      part.headers.add(name, value);
      if (detail::iequals(name, "Content-Type")) part.content_type = value;
      if (detail::iequals(name, "Content-Disposition")) {
        const auto disposition = split_header_parameters(value);
        for (std::size_t index = 1; index < disposition.size(); ++index) {
          const auto &token = disposition[index];
          const auto equals = token.find('=');
          if (equals == std::string::npos) continue;
          auto key = detail::trim(std::string_view(token).substr(0, equals));
          auto val = decode_header_parameter(
              std::string_view(token).substr(equals + 1));
          if (!val) return val.error();
          if (detail::iequals(key, "name")) part.name = *val;
          if (detail::iequals(key, "filename")) part.filename = *val;
        }
      }
      if (line_end == std::string_view::npos || line_end >= header_end) break;
      line_start = line_end + 2;
    }
    const auto content_begin = header_end + 4;
    const auto next = body.find("\r\n" + marker, content_begin);
    if (next == std::string_view::npos) {
      return ErrorInfo{Error::multipart, "Incomplete multipart body"};
    }
    part.content.assign(body.substr(content_begin, next - content_begin));
    parts.push_back(std::move(part));
    if (parts.size() > max_parts) {
      return ErrorInfo{Error::multipart, "Too many multipart parts"};
    }
    cursor = next + 2;
  }
  if (!closed)
    return ErrorInfo{Error::multipart, "Multipart body has no closing delimiter"};
  return parts;
}

std::pair<std::string, std::string>
make_multipart(const MultipartForm &parts, std::string boundary) {
  if (!valid_multipart_boundary(boundary)) boundary = detail::random_boundary();
  std::string body;
  for (const auto &part : parts) {
    body += "--" + boundary + "\r\n";
    body += "Content-Disposition: form-data; name=\"" +
            quote_multipart_parameter(part.name) + "\"";
    if (!part.filename.empty())
      body += "; filename=\"" + quote_multipart_parameter(part.filename) +
              "\"";
    body += "\r\n";
    if (!part.content_type.empty()) {
      body += "Content-Type: " + part.content_type + "\r\n";
    }
    for (const auto &[name, value] : part.headers) {
      if (!detail::iequals(name, "Content-Disposition") &&
          !detail::iequals(name, "Content-Type") &&
          detail::valid_header_name(name) &&
          detail::valid_header_value(value)) {
        body += name + ": " + value + "\r\n";
      }
    }
    body += "\r\n" + part.content + "\r\n";
  }
  body += "--" + boundary + "--\r\n";
  return {"multipart/form-data; boundary=" + boundary, std::move(body)};
}

std::string mime_type(const std::filesystem::path &path) {
  static const std::unordered_map<std::string, std::string> types{
      {".html", "text/html; charset=utf-8"}, {".htm", "text/html; charset=utf-8"},
      {".css", "text/css; charset=utf-8"},   {".js", "text/javascript; charset=utf-8"},
      {".json", "application/json"},        {".xml", "application/xml"},
      {".txt", "text/plain; charset=utf-8"},{".csv", "text/csv; charset=utf-8"},
      {".svg", "image/svg+xml"},            {".png", "image/png"},
      {".jpg", "image/jpeg"},               {".jpeg", "image/jpeg"},
      {".gif", "image/gif"},                {".webp", "image/webp"},
      {".ico", "image/x-icon"},             {".pdf", "application/pdf"},
      {".wasm", "application/wasm"},        {".zip", "application/zip"},
      {".gz", "application/gzip"},          {".mp3", "audio/mpeg"},
      {".mp4", "video/mp4"},                {".woff", "font/woff"},
      {".woff2", "font/woff2"}};
  auto extension = detail::lower(path.extension().string());
  const auto found = types.find(extension);
  return found == types.end() ? "application/octet-stream" : found->second;
}

std::string status_reason(int status) {
  switch (status) {
  case 100: return "Continue";
  case 101: return "Switching Protocols";
  case 102: return "Processing";
  case 103: return "Early Hints";
  case 200: return "OK";
  case 201: return "Created";
  case 202: return "Accepted";
  case 203: return "Non-Authoritative Information";
  case 204: return "No Content";
  case 205: return "Reset Content";
  case 206: return "Partial Content";
  case 207: return "Multi-Status";
  case 208: return "Already Reported";
  case 226: return "IM Used";
  case 300: return "Multiple Choices";
  case 301: return "Moved Permanently";
  case 302: return "Found";
  case 303: return "See Other";
  case 304: return "Not Modified";
  case 305: return "Use Proxy";
  case 307: return "Temporary Redirect";
  case 308: return "Permanent Redirect";
  case 400: return "Bad Request";
  case 401: return "Unauthorized";
  case 402: return "Payment Required";
  case 403: return "Forbidden";
  case 404: return "Not Found";
  case 405: return "Method Not Allowed";
  case 406: return "Not Acceptable";
  case 407: return "Proxy Authentication Required";
  case 408: return "Request Timeout";
  case 409: return "Conflict";
  case 410: return "Gone";
  case 411: return "Length Required";
  case 412: return "Precondition Failed";
  case 413: return "Payload Too Large";
  case 414: return "URI Too Long";
  case 415: return "Unsupported Media Type";
  case 416: return "Range Not Satisfiable";
  case 417: return "Expectation Failed";
  case 418: return "I'm a teapot";
  case 421: return "Misdirected Request";
  case 422: return "Unprocessable Content";
  case 423: return "Locked";
  case 424: return "Failed Dependency";
  case 425: return "Too Early";
  case 426: return "Upgrade Required";
  case 428: return "Precondition Required";
  case 429: return "Too Many Requests";
  case 431: return "Request Header Fields Too Large";
  case 451: return "Unavailable For Legal Reasons";
  case 500: return "Internal Server Error";
  case 501: return "Not Implemented";
  case 502: return "Bad Gateway";
  case 503: return "Service Unavailable";
  case 504: return "Gateway Timeout";
  case 505: return "HTTP Version Not Supported";
  case 506: return "Variant Also Negotiates";
  case 507: return "Insufficient Storage";
  case 508: return "Loop Detected";
  case 510: return "Not Extended";
  case 511: return "Network Authentication Required";
  default: return "Unknown";
  }
}

Result<ContentRange> parse_content_range(std::string_view value) {
  const auto trim_ows = [](std::string_view text) {
    while (!text.empty() && (text.front() == ' ' || text.front() == '\t'))
      text.remove_prefix(1);
    while (!text.empty() && (text.back() == ' ' || text.back() == '\t'))
      text.remove_suffix(1);
    return text;
  };
  const auto parse_number = [](std::string_view text, std::uint64_t &number) {
    if (text.empty())
      return false;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), number);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
  };

  value = trim_ows(value);
  const auto space = value.find(' ');
  if (space == std::string_view::npos || space == 0)
    return ErrorInfo{Error::protocol, "Malformed Content-Range"};
  const auto unit = value.substr(0, space);
  if (!detail::valid_header_name(unit))
    return ErrorInfo{Error::protocol, "Invalid Content-Range unit"};
  auto range = value.substr(space + 1);
  if (range.empty() || range.front() == ' ' || range.front() == '\t' ||
      range.find_first_of(" \t") != std::string_view::npos)
    return ErrorInfo{Error::protocol, "Invalid Content-Range whitespace"};
  const auto slash = range.find('/');
  if (slash == std::string_view::npos || slash == 0 ||
      range.find('/', slash + 1) != std::string_view::npos)
    return ErrorInfo{Error::protocol, "Malformed Content-Range bounds"};

  ContentRange result;
  result.unit = std::string(unit);
  const auto bounds = range.substr(0, slash);
  const auto total_text = range.substr(slash + 1);
  if (total_text.empty())
    return ErrorInfo{Error::protocol, "Content-Range total is empty"};
  if (total_text != "*") {
    std::uint64_t total = 0;
    if (!parse_number(total_text, total))
      return ErrorInfo{Error::protocol, "Invalid Content-Range total"};
    result.total = total;
  }

  if (bounds == "*") {
    if (!result.total)
      return ErrorInfo{Error::protocol,
                       "Unsatisfied Content-Range requires a total"};
    return result;
  }
  const auto dash = bounds.find('-');
  if (dash == std::string_view::npos || dash == 0 ||
      dash + 1 == bounds.size() ||
      bounds.find('-', dash + 1) != std::string_view::npos)
    return ErrorInfo{Error::protocol, "Malformed Content-Range interval"};
  std::uint64_t first = 0;
  std::uint64_t last = 0;
  if (!parse_number(bounds.substr(0, dash), first) ||
      !parse_number(bounds.substr(dash + 1), last))
    return ErrorInfo{Error::protocol, "Invalid Content-Range interval"};
  if (first > last)
    return ErrorInfo{Error::protocol, "Content-Range begins after it ends"};
  if (result.total && last >= *result.total)
    return ErrorInfo{Error::protocol, "Content-Range exceeds its total"};
  result.first = first;
  result.last = last;
  return result;
}

Result<std::string> format_byte_range(std::uint64_t first,
                                      std::optional<std::uint64_t> last) {
  if (last && first > *last)
    return ErrorInfo{Error::invalid_argument,
                     "Byte range begins after it ends"};
  return "bytes=" + std::to_string(first) + "-" +
         (last ? std::to_string(*last) : std::string{});
}

Result<std::string> format_content_range(std::uint64_t first,
                                         std::uint64_t last,
                                         std::optional<std::uint64_t> total) {
  if (first > last)
    return ErrorInfo{Error::invalid_argument,
                     "Content range begins after it ends"};
  if (total && last >= *total)
    return ErrorInfo{Error::invalid_argument,
                     "Content range exceeds its total"};
  return "bytes " + std::to_string(first) + "-" + std::to_string(last) + "/" +
         (total ? std::to_string(*total) : std::string{"*"});
}

Result<void> set_byte_range(Headers &headers, std::uint64_t first,
                            std::optional<std::uint64_t> last,
                            std::string_view if_range) {
  auto range = format_byte_range(first, last);
  if (!range)
    return range.error();
  if (!if_range.empty()) {
    if (!detail::valid_header_value(if_range))
      return ErrorInfo{Error::invalid_argument,
                       "If-Range contains invalid characters"};
    if (if_range.starts_with("W/") || if_range.starts_with("w/"))
      return ErrorInfo{Error::invalid_argument,
                       "If-Range requires a strong entity tag or HTTP date"};
  }
  headers.set("Range", std::move(*range));
  if (!if_range.empty())
    headers.set("If-Range", std::string(if_range));
  else
    headers.erase("If-Range");
  return {};
}

std::string basic_auth(std::string_view username, std::string_view password) {
  return "Basic " + detail::base64_encode(std::string(username) + ":" +
                                          std::string(password));
}

std::string bearer_auth(std::string_view token) {
  return "Bearer " + std::string(token);
}

Result<DigestChallenge> parse_digest_challenge(std::string_view value) {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front())))
    value.remove_prefix(1);
  const auto first_space = value.find(' ');
  if (first_space == std::string_view::npos ||
      !detail::iequals(value.substr(0, first_space), "Digest")) {
    return ErrorInfo{Error::protocol, "Authentication challenge is not Digest"};
  }
  value.remove_prefix(first_space + 1);
  std::unordered_map<std::string, std::string> parameters;
  std::size_t cursor = 0;
  while (cursor < value.size()) {
    while (cursor < value.size() &&
           (value[cursor] == ',' ||
            std::isspace(static_cast<unsigned char>(value[cursor]))))
      ++cursor;
    const auto key_begin = cursor;
    while (cursor < value.size() && value[cursor] != '=' &&
           value[cursor] != ',')
      ++cursor;
    if (cursor >= value.size() || value[cursor] != '=') {
      return ErrorInfo{Error::protocol, "Malformed Digest challenge"};
    }
    auto key = detail::lower(detail::trim(value.substr(key_begin,
                                                       cursor - key_begin)));
    ++cursor;
    std::string parameter;
    if (cursor < value.size() && value[cursor] == '"') {
      ++cursor;
      bool closed = false;
      while (cursor < value.size()) {
        const char ch = value[cursor++];
        if (ch == '"') {
          closed = true;
          break;
        }
        if (ch == '\\' && cursor < value.size()) parameter.push_back(value[cursor++]);
        else parameter.push_back(ch);
      }
      if (!closed)
        return ErrorInfo{Error::protocol, "Unterminated Digest parameter"};
    } else {
      const auto end = value.find(',', cursor);
      parameter = detail::trim(value.substr(
          cursor, end == std::string_view::npos ? value.size() - cursor
                                                : end - cursor));
      cursor = end == std::string_view::npos ? value.size() : end;
    }
    parameters[std::move(key)] = std::move(parameter);
  }
  DigestChallenge challenge;
  challenge.realm = parameters["realm"];
  challenge.nonce = parameters["nonce"];
  challenge.opaque = parameters["opaque"];
  if (parameters.contains("algorithm"))
    challenge.algorithm = parameters["algorithm"];
  challenge.qop = parameters["qop"];
  challenge.stale = detail::iequals(parameters["stale"], "true");
  if (challenge.realm.empty() || challenge.nonce.empty()) {
    return ErrorInfo{Error::protocol,
                     "Digest challenge is missing realm or nonce"};
  }
  return challenge;
}

Result<std::string> digest_auth(
    std::string_view method, std::string_view uri, std::string_view username,
    std::string_view password, const DigestChallenge &challenge,
    std::uint32_t nonce_count, std::string cnonce) {
#ifndef CHHTTP_HAS_CRYPTO
  (void)method;
  (void)uri;
  (void)username;
  (void)password;
  (void)challenge;
  (void)nonce_count;
  (void)cnonce;
  return ErrorInfo{Error::tls_unavailable,
                   "Digest authentication requires OpenSSL"};
#else
  auto algorithm = detail::lower(challenge.algorithm);
  bool session_algorithm = false;
  if (algorithm.ends_with("-sess")) {
    session_algorithm = true;
    algorithm.resize(algorithm.size() - 5);
  }
  const char *digest_name = nullptr;
  if (algorithm == "md5") digest_name = "MD5";
  else if (algorithm == "sha-256") digest_name = "SHA256";
  else if (algorithm == "sha-512-256") digest_name = "SHA512-256";
  else
    return ErrorInfo{Error::protocol,
                     "Unsupported Digest algorithm: " + challenge.algorithm};

  EVP_MD *digest = EVP_MD_fetch(nullptr, digest_name, nullptr);
  if (!digest)
    return ErrorInfo{Error::internal, "Unable to load Digest hash algorithm"};
  struct DigestGuard {
    EVP_MD *value;
    ~DigestGuard() { EVP_MD_free(value); }
  } guard{digest};
  const auto hash = [&](std::string_view input) -> Result<std::string> {
    EVP_MD_CTX *context = EVP_MD_CTX_new();
    if (!context)
      return ErrorInfo{Error::internal, "Unable to allocate Digest context"};
    std::array<unsigned char, EVP_MAX_MD_SIZE> output{};
    unsigned output_size = 0;
    const bool ok = EVP_DigestInit_ex(context, digest, nullptr) == 1 &&
                    EVP_DigestUpdate(context, input.data(), input.size()) == 1 &&
                    EVP_DigestFinal_ex(context, output.data(), &output_size) == 1;
    EVP_MD_CTX_free(context);
    if (!ok) return ErrorInfo{Error::internal, "Digest hash failed"};
    constexpr char hex[] = "0123456789abcdef";
    std::string encoded(output_size * 2, '\0');
    for (unsigned index = 0; index < output_size; ++index) {
      encoded[index * 2] = hex[output[index] >> 4];
      encoded[index * 2 + 1] = hex[output[index] & 0x0f];
    }
    return encoded;
  };
  if (cnonce.empty()) cnonce = detail::random_boundary();
  auto ha1 = hash(std::string(username) + ":" + challenge.realm + ":" +
                  std::string(password));
  if (!ha1) return ha1.error();
  if (session_algorithm) {
    ha1 = hash(*ha1 + ":" + challenge.nonce + ":" + cnonce);
    if (!ha1) return ha1.error();
  }
  auto ha2 = hash(std::string(method) + ":" + std::string(uri));
  if (!ha2) return ha2.error();

  std::string selected_qop;
  if (!challenge.qop.empty()) {
    for (const auto &candidate : detail::split_tokens(challenge.qop, ',')) {
      if (detail::iequals(candidate, "auth")) {
        selected_qop = "auth";
        break;
      }
    }
    if (selected_qop.empty())
      return ErrorInfo{Error::protocol,
                       "Digest qop=auth-int is not supported for streamed bodies"};
  }
  std::ostringstream nonce_stream;
  nonce_stream << std::hex << std::setw(8) << std::setfill('0') << nonce_count;
  const std::string nonce_value = nonce_stream.str();
  const std::string response_input =
      selected_qop.empty()
          ? *ha1 + ":" + challenge.nonce + ":" + *ha2
          : *ha1 + ":" + challenge.nonce + ":" + nonce_value + ":" +
                cnonce + ":" + selected_qop + ":" + *ha2;
  auto response = hash(response_input);
  if (!response) return response.error();
  const auto quote = [](std::string_view input) {
    std::string output;
    output.reserve(input.size() + 2);
    output.push_back('"');
    for (const char ch : input) {
      if (ch == '"' || ch == '\\') output.push_back('\\');
      output.push_back(ch);
    }
    output.push_back('"');
    return output;
  };
  std::string authorization =
      "Digest username=" + quote(username) + ", realm=" + quote(challenge.realm) +
      ", nonce=" + quote(challenge.nonce) + ", uri=" + quote(uri) +
      ", response=" + quote(*response) + ", algorithm=" + challenge.algorithm;
  if (!challenge.opaque.empty())
    authorization += ", opaque=" + quote(challenge.opaque);
  if (!selected_qop.empty())
    authorization += ", qop=" + selected_qop + ", nc=" + nonce_value +
                     ", cnonce=" + quote(cnonce);
  return authorization;
#endif
}

std::string format_sse(const SseEvent &event) {
  std::string output;
  if (!event.event.empty()) output += "event: " + event.event + "\n";
  if (!event.id.empty()) output += "id: " + event.id + "\n";
  if (event.retry) output += "retry: " + std::to_string(event.retry->count()) + "\n";
  std::size_t start = 0;
  do {
    const auto end = event.data.find('\n', start);
    output += "data: ";
    output.append(event.data, start,
                  end == std::string::npos ? std::string::npos : end - start);
    output.push_back('\n');
    if (end == std::string::npos) break;
    start = end + 1;
  } while (start <= event.data.size());
  output.push_back('\n');
  return output;
}

} // namespace chhttp

namespace chhttp::detail {

bool iequals(std::string_view lhs, std::string_view rhs) noexcept {
  return lhs.size() == rhs.size() &&
         std::ranges::equal(lhs, rhs, [](char left, char right) {
           return std::tolower(static_cast<unsigned char>(left)) ==
                  std::tolower(static_cast<unsigned char>(right));
         });
}

std::string lower(std::string_view value) {
  std::string output(value);
  std::ranges::transform(output, output.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return output;
}

std::string trim(std::string_view value) {
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())))
    value.remove_prefix(1);
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())))
    value.remove_suffix(1);
  return std::string(value);
}

std::vector<std::string> split_tokens(std::string_view value, char separator) {
  std::vector<std::string> result;
  std::size_t start = 0;
  while (start <= value.size()) {
    const auto end = value.find(separator, start);
    auto part = trim(value.substr(start, end == std::string_view::npos
                                            ? value.size() - start
                                            : end - start));
    if (!part.empty()) result.push_back(std::move(part));
    if (end == std::string_view::npos) break;
    start = end + 1;
  }
  return result;
}

std::string random_boundary() {
  static thread_local std::mt19937_64 generator(std::random_device{}());
  std::ostringstream stream;
  stream << "chhttp-" << std::hex << generator() << generator();
  return stream.str();
}

Result<ParsedUrl> parse_url(std::string_view input, std::string_view base) {
  std::string complete(input);
  if (input.find("://") == std::string_view::npos) {
    if (base.empty()) return ErrorInfo{Error::invalid_url, "URL has no scheme"};
    auto resolved = resolve_url(base, input);
    if (!resolved) return resolved.error();
    complete = std::move(*resolved);
  }
  for (unsigned char ch : complete) {
    if (ch <= 0x20 || ch == 0x7f)
      return ErrorInfo{Error::invalid_url, "URL contains whitespace or control characters"};
  }
  const auto scheme_end = complete.find("://");
  if (scheme_end == std::string::npos || scheme_end == 0)
    return ErrorInfo{Error::invalid_url, "Malformed URL scheme"};
  ParsedUrl result;
  result.scheme = lower(std::string_view(complete).substr(0, scheme_end));
  result.secure = result.scheme == "https" || result.scheme == "wss";
  if (result.scheme != "http" && result.scheme != "https" &&
      result.scheme != "ws" && result.scheme != "wss") {
    return ErrorInfo{Error::invalid_url, "Unsupported URL scheme: " + result.scheme};
  }
  const auto authority_begin = scheme_end + 3;
  const auto target_begin = complete.find_first_of("/?#", authority_begin);
  auto authority = std::string_view(complete).substr(
      authority_begin, target_begin == std::string::npos
                           ? std::string::npos
                           : target_begin - authority_begin);
  if (authority.empty() || authority.find('@') != std::string_view::npos)
    return ErrorInfo{Error::invalid_url, "URL has an invalid authority"};
  std::string_view port_text;
  if (authority.front() == '[') {
    const auto bracket = authority.find(']');
    if (bracket == std::string_view::npos || bracket == 1)
      return ErrorInfo{Error::invalid_url, "Malformed IPv6 URL host"};
    result.host = std::string(authority.substr(1, bracket - 1));
    if (bracket + 1 < authority.size()) {
      if (authority[bracket + 1] != ':')
        return ErrorInfo{Error::invalid_url, "Malformed URL authority"};
      port_text = authority.substr(bracket + 2);
    }
  } else {
    const auto colon = authority.rfind(':');
    if (colon != std::string_view::npos) {
      if (authority.find(':') != colon)
        return ErrorInfo{Error::invalid_url, "IPv6 URL hosts must use brackets"};
      result.host = std::string(authority.substr(0, colon));
      port_text = authority.substr(colon + 1);
    } else {
      result.host = std::string(authority);
    }
  }
  if (result.host.empty()) return ErrorInfo{Error::invalid_url, "URL has no host"};
  if (!port_text.empty()) {
    unsigned value = 0;
    const auto conversion = std::from_chars(port_text.data(),
                                            port_text.data() + port_text.size(), value);
    if (conversion.ec != std::errc{} ||
        conversion.ptr != port_text.data() + port_text.size() ||
        value > 65535 || value == 0) {
      return ErrorInfo{Error::invalid_url, "Invalid URL port"};
    }
    result.port = static_cast<std::uint16_t>(value);
  } else if (!authority.empty() && authority.back() == ':') {
    return ErrorInfo{Error::invalid_url, "URL port is empty"};
  } else {
    result.port = result.secure ? 443 : 80;
  }
  if (target_begin != std::string::npos && complete[target_begin] != '#') {
    const auto fragment = complete.find('#', target_begin);
    result.target = complete.substr(target_begin, fragment == std::string::npos
                                                      ? std::string::npos
                                                      : fragment - target_begin);
  }
  if (result.target.empty()) result.target = "/";
  if (result.target.front() == '?') result.target.insert(result.target.begin(), '/');
  return result;
}

std::string ParsedUrl::authority() const {
  std::string value = host.find(':') == std::string::npos ? host : "[" + host + "]";
  const bool default_port = (secure && port == 443) || (!secure && port == 80);
  if (!default_port) value += ":" + std::to_string(port);
  return value;
}

std::string ParsedUrl::origin() const {
  return scheme + "://" + authority();
}

Result<std::string> resolve_url(std::string_view base,
                                std::string_view reference) {
  if (reference.find("://") != std::string_view::npos)
    return std::string(reference);
  auto parsed_base = parse_url(base);
  if (!parsed_base) return parsed_base.error();
  if (reference.starts_with("//"))
    return parsed_base->scheme + ":" + std::string(reference);
  const auto origin = parsed_base->origin();
  if (reference.empty() || reference.front() == '#')
    return origin + parsed_base->target;
  if (reference.front() == '?') {
    const auto query = parsed_base->target.find('?');
    return origin + parsed_base->target.substr(0, query) + std::string(reference);
  }
  std::string combined;
  if (reference.front() == '/') {
    combined = std::string(reference);
  } else {
    auto path = parsed_base->target.substr(0, parsed_base->target.find('?'));
    const auto slash = path.rfind('/');
    combined = path.substr(0, slash == std::string::npos ? 0 : slash + 1) +
               std::string(reference);
  }
  const auto query_at = combined.find('?');
  const auto query = query_at == std::string::npos ? std::string{} : combined.substr(query_at);
  auto path = combined.substr(0, query_at);
  std::vector<std::string> segments;
  for (const auto &segment : split_tokens(path, '/')) {
    if (segment == ".") continue;
    if (segment == "..") {
      if (!segments.empty()) segments.pop_back();
    } else {
      segments.push_back(segment);
    }
  }
  path = "/";
  for (std::size_t index = 0; index < segments.size(); ++index) {
    if (index) path += '/';
    path += segments[index];
  }
  if (combined.ends_with('/') && !path.ends_with('/')) path += '/';
  return origin + path + query;
}

bool valid_header_name(std::string_view value) noexcept {
  if (value.empty()) return false;
  constexpr std::string_view separators = "()<>@,;:\\\"/[]?={} \t";
  return std::ranges::all_of(value, [&](unsigned char ch) {
    return ch > 0x20 && ch < 0x7f && separators.find(static_cast<char>(ch)) ==
                                           std::string_view::npos;
  });
}

bool valid_header_value(std::string_view value) noexcept {
  return std::ranges::all_of(value, [](unsigned char ch) {
    return ch == '\t' || (ch >= 0x20 && ch != 0x7f);
  });
}

bool has_token(const Headers &headers, std::string_view name,
               std::string_view token) {
  for (const auto &value : headers.get_all(name)) {
    for (const auto &candidate : split_tokens(value, ',')) {
      if (iequals(candidate, token)) return true;
    }
  }
  return false;
}

std::string base64_encode(std::string_view value) {
  static constexpr std::string_view alphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string output;
  output.reserve((value.size() + 2) / 3 * 4);
  for (std::size_t offset = 0; offset < value.size(); offset += 3) {
    const auto first = static_cast<unsigned char>(value[offset]);
    const auto second = offset + 1 < value.size()
                            ? static_cast<unsigned char>(value[offset + 1])
                            : 0;
    const auto third = offset + 2 < value.size()
                           ? static_cast<unsigned char>(value[offset + 2])
                           : 0;
    const std::uint32_t bits = (static_cast<std::uint32_t>(first) << 16) |
                               (static_cast<std::uint32_t>(second) << 8) | third;
    output.push_back(alphabet[(bits >> 18) & 0x3f]);
    output.push_back(alphabet[(bits >> 12) & 0x3f]);
    output.push_back(offset + 1 < value.size() ? alphabet[(bits >> 6) & 0x3f] : '=');
    output.push_back(offset + 2 < value.size() ? alphabet[bits & 0x3f] : '=');
  }
  return output;
}

Result<std::string> base64_decode(std::string_view value) {
  static constexpr std::string_view alphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  if (value.size() % 4 != 0)
    return ErrorInfo{Error::protocol, "Invalid Base64 length"};
  std::string output;
  output.reserve(value.size() / 4 * 3);
  for (std::size_t offset = 0; offset < value.size(); offset += 4) {
    std::uint32_t bits = 0;
    int padding = 0;
    for (int index = 0; index < 4; ++index) {
      const char ch = value[offset + index];
      if (ch == '=') {
        if (index < 2 || offset + 4 != value.size())
          return ErrorInfo{Error::protocol, "Invalid Base64 padding"};
        ++padding;
        bits <<= 6;
      } else {
        const auto position = alphabet.find(ch);
        if (position == std::string_view::npos || padding)
          return ErrorInfo{Error::protocol, "Invalid Base64 character"};
        bits = (bits << 6) | static_cast<std::uint32_t>(position);
      }
    }
    output.push_back(static_cast<char>((bits >> 16) & 0xff));
    if (padding < 2) output.push_back(static_cast<char>((bits >> 8) & 0xff));
    if (padding == 0) output.push_back(static_cast<char>(bits & 0xff));
  }
  return output;
}

std::string sha1_base64(std::string_view value) {
#ifdef CHHTTP_HAS_CRYPTO
  std::array<unsigned char, SHA_DIGEST_LENGTH> digest{};
  SHA1(reinterpret_cast<const unsigned char *>(value.data()), value.size(),
       digest.data());
  return base64_encode(std::string_view(
      reinterpret_cast<const char *>(digest.data()), digest.size()));
#else
  (void)value;
  return {};
#endif
}

std::string websocket_accept(std::string_view key) {
  return sha1_base64(std::string(key) + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
}

ErrorInfo make_error(Error code, std::string message, int system_code,
                     long tls_code) {
  return ErrorInfo{code, std::move(message), system_code, tls_code};
}

bool path_is_within(const std::filesystem::path &root,
                    const std::filesystem::path &candidate) {
  const auto normalized_root = std::filesystem::weakly_canonical(root);
  const auto normalized_candidate = std::filesystem::weakly_canonical(candidate);
  auto root_it = normalized_root.begin();
  auto candidate_it = normalized_candidate.begin();
  for (; root_it != normalized_root.end(); ++root_it, ++candidate_it) {
    if (candidate_it == normalized_candidate.end() || *root_it != *candidate_it)
      return false;
  }
  return true;
}

#ifdef CHHTTP_HAS_COMPRESSION
static Result<std::string> zlib_transform(std::string_view input, bool encode,
                                          int window_bits,
                                          std::size_t max_output) {
  z_stream stream{};
  int init = encode ? deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                                   window_bits, 8, Z_DEFAULT_STRATEGY)
                    : inflateInit2(&stream, window_bits);
  if (init != Z_OK) return ErrorInfo{Error::compression, "zlib initialization failed"};
  struct Guard {
    z_stream *stream;
    bool encode;
    ~Guard() { encode ? deflateEnd(stream) : inflateEnd(stream); }
  } guard{&stream, encode};
  stream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(input.data()));
  stream.avail_in = static_cast<uInt>(std::min<std::size_t>(input.size(), UINT_MAX));
  std::string output;
  std::array<char, 32 * 1024> buffer{};
  int result = Z_OK;
  do {
    stream.next_out = reinterpret_cast<Bytef *>(buffer.data());
    stream.avail_out = static_cast<uInt>(buffer.size());
    result = encode ? deflate(&stream, Z_FINISH) : inflate(&stream, Z_NO_FLUSH);
    if (result != Z_OK && result != Z_STREAM_END &&
        !(encode && result == Z_BUF_ERROR)) {
      return ErrorInfo{Error::compression, stream.msg ? stream.msg : "zlib failure"};
    }
    output.append(buffer.data(), buffer.size() - stream.avail_out);
    if (output.size() > max_output) {
      return ErrorInfo{Error::body_too_large, "Decompressed body exceeds limit"};
    }
  } while (result != Z_STREAM_END);
  return output;
}
#endif

Result<std::string> compress(std::string_view input, std::string_view encoding) {
#ifdef CHHTTP_HAS_COMPRESSION
  const auto normalized = lower(encoding);
  if (normalized == "gzip") {
    return zlib_transform(input, true, MAX_WBITS + 16,
                          std::numeric_limits<std::size_t>::max());
  }
  if (normalized == "deflate") {
    return zlib_transform(input, true, MAX_WBITS,
                          std::numeric_limits<std::size_t>::max());
  }
  if (normalized == "br") {
    size_t output_size = BrotliEncoderMaxCompressedSize(input.size());
    std::string output(output_size, '\0');
    if (BrotliEncoderCompress(BROTLI_DEFAULT_QUALITY, BROTLI_DEFAULT_WINDOW,
                              BROTLI_MODE_GENERIC, input.size(),
                              reinterpret_cast<const uint8_t *>(input.data()),
                              &output_size,
                              reinterpret_cast<uint8_t *>(output.data())) != BROTLI_TRUE) {
      return ErrorInfo{Error::compression, "Brotli compression failed"};
    }
    output.resize(output_size);
    return output;
  }
  if (normalized == "zstd") {
    std::string output(ZSTD_compressBound(input.size()), '\0');
    const auto size = ZSTD_compress(output.data(), output.size(), input.data(), input.size(), 3);
    if (ZSTD_isError(size)) return ErrorInfo{Error::compression, ZSTD_getErrorName(size)};
    output.resize(size);
    return output;
  }
#else
  (void)input;
  (void)encoding;
#endif
  return ErrorInfo{Error::compression, "Unsupported content encoding"};
}

Result<std::string> decompress(std::string_view input, std::string_view encoding,
                               std::size_t max_output) {
#ifdef CHHTTP_HAS_COMPRESSION
  const auto normalized = lower(encoding);
  if (normalized == "gzip") return zlib_transform(input, false, MAX_WBITS + 16, max_output);
  if (normalized == "deflate") return zlib_transform(input, false, MAX_WBITS, max_output);
  if (normalized == "br") {
    BrotliDecoderState *state = BrotliDecoderCreateInstance(nullptr, nullptr, nullptr);
    if (!state) return ErrorInfo{Error::compression, "Brotli allocation failed"};
    struct Guard { BrotliDecoderState *state; ~Guard() { BrotliDecoderDestroyInstance(state); } } guard{state};
    const uint8_t *next_in = reinterpret_cast<const uint8_t *>(input.data());
    size_t available_in = input.size();
    std::string output;
    std::array<uint8_t, 32 * 1024> buffer{};
    while (true) {
      uint8_t *next_out = buffer.data();
      size_t available_out = buffer.size();
      const auto result = BrotliDecoderDecompressStream(
          state, &available_in, &next_in, &available_out, &next_out, nullptr);
      output.append(reinterpret_cast<char *>(buffer.data()),
                    buffer.size() - available_out);
      if (output.size() > max_output)
        return ErrorInfo{Error::body_too_large, "Decompressed body exceeds limit"};
      if (result == BROTLI_DECODER_RESULT_SUCCESS) return output;
      if (result == BROTLI_DECODER_RESULT_ERROR ||
          (result == BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT && available_in == 0))
        return ErrorInfo{Error::compression, "Invalid Brotli body"};
    }
  }
  if (normalized == "zstd") {
    const auto declared = ZSTD_getFrameContentSize(input.data(), input.size());
    if (declared != ZSTD_CONTENTSIZE_UNKNOWN && declared != ZSTD_CONTENTSIZE_ERROR) {
      if (declared > max_output)
        return ErrorInfo{Error::body_too_large, "Decompressed body exceeds limit"};
      std::string output(static_cast<std::size_t>(declared), '\0');
      const auto size = ZSTD_decompress(output.data(), output.size(), input.data(), input.size());
      if (ZSTD_isError(size)) return ErrorInfo{Error::compression, ZSTD_getErrorName(size)};
      output.resize(size);
      return output;
    }
    return ErrorInfo{Error::compression, "Zstd frame has no declared size"};
  }
#else
  (void)input;
  (void)encoding;
  (void)max_output;
#endif
  return ErrorInfo{Error::compression, "Unsupported content encoding"};
}

std::string select_encoding(std::string_view accept_encoding) {
  constexpr std::array<std::string_view, 4> supported{
      "br", "zstd", "gzip", "deflate"};
  std::array<double, supported.size()> explicit_quality{};
  explicit_quality.fill(-1.0);
  double wildcard_quality = -1.0;
  for (const auto &item : split_tokens(accept_encoding, ',')) {
    const auto semicolon = item.find(';');
    const auto coding = lower(trim(std::string_view(item).substr(0, semicolon)));
    double quality = 1.0;
    std::size_t parameter = semicolon;
    while (parameter != std::string::npos) {
      const auto next = item.find(';', parameter + 1);
      const auto value = trim(std::string_view(item).substr(
          parameter + 1, next == std::string::npos
                             ? std::string_view::npos
                             : next - parameter - 1));
      const auto equal = value.find('=');
      if (equal != std::string::npos &&
          iequals(trim(std::string_view(value).substr(0, equal)), "q")) {
        const auto number = trim(std::string_view(value).substr(equal + 1));
        const auto parsed = std::from_chars(number.data(),
                                            number.data() + number.size(),
                                            quality);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != number.data() + number.size() || quality < 0.0 ||
            quality > 1.0)
          quality = 0.0;
      }
      parameter = next;
    }
    if (coding == "*") wildcard_quality = std::max(wildcard_quality, quality);
    for (std::size_t index = 0; index < supported.size(); ++index)
      if (coding == supported[index])
        explicit_quality[index] =
            std::max(explicit_quality[index], quality);
  }
  double selected_quality = 0.0;
  std::string selected;
  for (std::size_t index = 0; index < supported.size(); ++index) {
    const double quality = explicit_quality[index] >= 0.0
                               ? explicit_quality[index]
                               : wildcard_quality;
    if (quality > selected_quality) {
      selected_quality = quality;
      selected = supported[index];
    }
  }
  return selected;
}

} // namespace chhttp::detail
