#pragma once

#include <chhttp/chhttp.hpp>

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <cctype>
#include <random>
#include <sstream>

namespace chhttp::detail {

namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

struct ParsedUrl {
  std::string scheme;
  std::string host;
  std::uint16_t port{0};
  std::string target{"/"};
  bool secure{false};
};

Result<ParsedUrl> parse_url(std::string_view input,
                            std::string_view base = {});
bool iequals(std::string_view lhs, std::string_view rhs) noexcept;
std::string lower(std::string_view value);
std::string trim(std::string_view value);
std::vector<std::string> split_tokens(std::string_view value, char separator);
std::string random_boundary();

Headers from_beast_headers(const http::fields &fields);
void to_beast_headers(const Headers &headers, http::fields &fields);
Request from_beast_request(const http::request<http::string_body> &request);
Response from_beast_response(const http::response<http::string_body> &response);

Result<std::string> compress(std::string_view input,
                             std::string_view encoding);
Result<std::string> decompress(std::string_view input,
                               std::string_view encoding,
                               std::size_t max_output);
std::string select_encoding(std::string_view accept_encoding);

std::string base64_encode(std::string_view value);
std::string sha1_base64(std::string_view value);
std::string websocket_accept(std::string_view key);

ErrorInfo make_error(Error code, std::string message,
                     const boost::system::error_code &ec = {});
bool path_is_within(const std::filesystem::path &root,
                    const std::filesystem::path &candidate);

} // namespace chhttp::detail

namespace chhttp {
struct ServerAccess {
  static std::optional<std::filesystem::path> &file(Response &response) {
    return response.file_path_;
  }
  static StreamHandler &stream(Response &response) {
    return response.stream_handler_;
  }
};
} // namespace chhttp

