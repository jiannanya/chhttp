#include "detail.hpp"

#include <boost/beast/core/detail/base64.hpp>
#include <boost/url.hpp>

#ifdef CHHTTP_HAS_TLS
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

asio::awaitable<bool> StreamWriter::write(std::string_view data) {
  if (!sink_) {
    co_return false;
  }
  co_return co_await sink_->write(std::string(data));
}

asio::awaitable<bool> StreamWriter::write(std::span<const std::byte> data) {
  const auto *ptr = reinterpret_cast<const char *>(data.data());
  co_return co_await write(std::string_view(ptr, data.size()));
}

asio::awaitable<bool> StreamWriter::flush() {
  if (!sink_) {
    co_return false;
  }
  co_return co_await sink_->flush();
}

bool StreamWriter::open() const noexcept { return sink_ && sink_->open(); }

asio::awaitable<bool> SseWriter::send(const SseEvent &event) {
  co_return co_await writer_.write(format_sse(event));
}

asio::awaitable<bool> SseWriter::data(std::string_view value) {
  co_return co_await send(SseEvent{.data = std::string(value)});
}

asio::awaitable<bool> SseWriter::comment(std::string_view value) {
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
                 -> asio::awaitable<void> {
               SseWriter sse(writer);
               co_await handler(sse);
             });
  headers.set("Cache-Control", "no-cache");
  headers.set("X-Accel-Buffering", "no");
}

bool Response::is_streaming() const noexcept {
  return static_cast<bool>(stream_handler_);
}

asio::awaitable<Result<WebSocket::Message>> WebSocket::read() {
  if (!channel_) {
    co_return ErrorInfo{Error::websocket_closed, "WebSocket is not connected"};
  }
  co_return co_await channel_->read();
}

asio::awaitable<bool> WebSocket::send_text(std::string_view data) {
  if (!channel_) co_return false;
  co_return co_await channel_->send(std::string(data), false);
}

asio::awaitable<bool>
WebSocket::send_binary(std::span<const std::byte> data) {
  if (!channel_) co_return false;
  const auto *ptr = reinterpret_cast<const char *>(data.data());
  co_return co_await channel_->send(std::string(ptr, data.size()), true);
}

asio::awaitable<void> WebSocket::ping(std::string_view data) {
  if (channel_) {
    co_await channel_->ping(std::string(data));
  }
}

asio::awaitable<void> WebSocket::close(std::uint16_t code,
                                       std::string_view reason) {
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
    if (value[index] == '%' && index + 2 < value.size()) {
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

Result<MultipartForm> parse_multipart(std::string_view body,
                                     std::string_view content_type,
                                     std::size_t max_parts) {
  const auto boundary_pos = detail::lower(content_type).find("boundary=");
  if (boundary_pos == std::string::npos) {
    return ErrorInfo{Error::multipart, "Missing multipart boundary"};
  }
  auto boundary = detail::trim(content_type.substr(boundary_pos + 9));
  if (!boundary.empty() && boundary.front() == '"' && boundary.back() == '"') {
    boundary = boundary.substr(1, boundary.size() - 2);
  }
  if (boundary.empty() || boundary.size() > 70 ||
      boundary.find_first_of("\r\n") != std::string::npos) {
    return ErrorInfo{Error::multipart, "Invalid multipart boundary"};
  }
  const std::string marker = "--" + boundary;
  MultipartForm parts;
  std::size_t cursor = 0;
  while (true) {
    auto begin = body.find(marker, cursor);
    if (begin == std::string_view::npos) break;
    begin += marker.size();
    if (body.substr(begin, 2) == "--") break;
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
      part.headers.add(name, value);
      if (detail::iequals(name, "Content-Type")) part.content_type = value;
      if (detail::iequals(name, "Content-Disposition")) {
        for (const auto &token : detail::split_tokens(value, ';')) {
          const auto equals = token.find('=');
          if (equals == std::string::npos) continue;
          auto key = detail::trim(std::string_view(token).substr(0, equals));
          auto val = detail::trim(std::string_view(token).substr(equals + 1));
          if (val.size() >= 2 && val.front() == '"' && val.back() == '"') {
            val = val.substr(1, val.size() - 2);
          }
          if (detail::iequals(key, "name")) part.name = val;
          if (detail::iequals(key, "filename")) part.filename = val;
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
  return parts;
}

std::pair<std::string, std::string>
make_multipart(const MultipartForm &parts, std::string boundary) {
  if (boundary.empty()) boundary = detail::random_boundary();
  std::string body;
  for (const auto &part : parts) {
    body += "--" + boundary + "\r\n";
    body += "Content-Disposition: form-data; name=\"" + part.name + "\"";
    if (!part.filename.empty()) body += "; filename=\"" + part.filename + "\"";
    body += "\r\n";
    if (!part.content_type.empty()) {
      body += "Content-Type: " + part.content_type + "\r\n";
    }
    for (const auto &[name, value] : part.headers) {
      if (!detail::iequals(name, "Content-Disposition") &&
          !detail::iequals(name, "Content-Type")) {
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
  return std::string(boost::beast::http::obsolete_reason(
      static_cast<boost::beast::http::status>(status)));
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
#ifndef CHHTTP_HAS_TLS
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
    if (base.empty()) {
      return ErrorInfo{Error::invalid_url, "URL has no scheme"};
    }
    if (!complete.empty() && complete.front() != '/') complete.insert(0, "/");
    complete = std::string(base) + complete;
  }
  auto parsed = boost::urls::parse_uri(complete);
  if (!parsed || !parsed->has_authority()) {
    return ErrorInfo{Error::invalid_url, "Invalid URL: " + complete};
  }
  ParsedUrl result;
  result.scheme = lower(parsed->scheme());
  result.secure = result.scheme == "https" || result.scheme == "wss";
  if (result.scheme != "http" && result.scheme != "https" &&
      result.scheme != "ws" && result.scheme != "wss") {
    return ErrorInfo{Error::invalid_url, "Unsupported URL scheme: " + result.scheme};
  }
  result.host = std::string(parsed->host());
  if (result.host.empty()) return ErrorInfo{Error::invalid_url, "URL has no host"};
  if (parsed->has_port()) {
    unsigned value = 0;
    const auto port_text = parsed->port();
    const auto conversion = std::from_chars(port_text.data(),
                                            port_text.data() + port_text.size(), value);
    if (conversion.ec != std::errc{} || value > 65535 || value == 0) {
      return ErrorInfo{Error::invalid_url, "Invalid URL port"};
    }
    result.port = static_cast<std::uint16_t>(value);
  } else {
    result.port = result.secure ? 443 : 80;
  }
  result.target = parsed->encoded_path().empty() ? "/" : std::string(parsed->encoded_path());
  if (parsed->has_query()) result.target += "?" + std::string(parsed->encoded_query());
  return result;
}

Headers from_beast_headers(const http::fields &fields) {
  Headers result;
  for (const auto &field : fields) {
    result.add(std::string(field.name_string()), std::string(field.value()));
  }
  return result;
}

void to_beast_headers(const Headers &headers, http::fields &fields) {
  for (const auto &[name, value] : headers) fields.insert(name, value);
}

Request from_beast_request(const http::request<http::string_body> &source) {
  Request result;
  result.method = std::string(source.method_string());
  result.target = std::string(source.target());
  const auto query_at = result.target.find('?');
  result.path = query_at == std::string::npos ? result.target
                                              : result.target.substr(0, query_at);
  if (result.path.empty()) result.path = "/";
  if (query_at != std::string::npos) result.query = parse_query(result.target.substr(query_at + 1));
  result.version = source.version();
  result.headers = from_beast_headers(source.base());
  result.body = source.body();
  result.keep_alive = source.keep_alive();
  return result;
}

Response from_beast_response(const http::response<http::string_body> &source) {
  Response result;
  result.status = source.result_int();
  result.version = source.version();
  result.headers = from_beast_headers(source.base());
  result.body = source.body();
  result.keep_alive = source.keep_alive();
  return result;
}

std::string base64_encode(std::string_view value) {
  std::string output;
  output.resize(beast::detail::base64::encoded_size(value.size()));
  const auto written = beast::detail::base64::encode(output.data(), value.data(), value.size());
  output.resize(written);
  return output;
}

std::string sha1_base64(std::string_view value) {
#ifdef CHHTTP_HAS_TLS
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

ErrorInfo make_error(Error code, std::string message,
                     const boost::system::error_code &ec) {
  if (ec) message += ": " + ec.message();
  return ErrorInfo{code, std::move(message), ec.value(), 0};
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
  const auto normalized = lower(accept_encoding);
  for (const auto *encoding : {"br", "zstd", "gzip", "deflate"}) {
    const auto at = normalized.find(encoding);
    if (at != std::string::npos) {
      const auto q = normalized.find("q=0", at);
      const auto comma = normalized.find(',', at);
      if (q == std::string::npos || (comma != std::string::npos && q > comma))
        return encoding;
    }
  }
  return {};
}

} // namespace chhttp::detail
