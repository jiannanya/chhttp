#include <chhttp/chhttp.hpp>

#include <filesystem>
#include <iostream>
#include <unordered_map>

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "usage: chhttp_agent_multipart_stream <artifact-file>\n";
    return 1;
  }
  const std::filesystem::path artifact = argv[1];

  // The gateway incrementally parses multipart metadata and data. The awaited
  // callbacks are where an agent can wait for a bounded queue, object store,
  // antivirus scanner, or asynchronous file writer.
  chhttp::Server gateway;
  gateway.post_stream(
      "/v1/agent-artifacts",
      [](chhttp::Request &request, chhttp::RequestBodyStream &body,
         chhttp::Response &response) -> chhttp::Task<void> {
        std::string current_name;
        std::unordered_map<std::string, std::uint64_t> sizes;
        auto error = co_await chhttp::consume_multipart(
            body, request.get_header("Content-Type"),
            {.on_part_begin = [&](const chhttp::MultipartPart &part)
                                  -> chhttp::Task<bool> {
               current_name = part.name;
               sizes.try_emplace(current_name, 0);
               co_return true;
             },
             .on_part_data = [&](std::string_view chunk)
                                 -> chhttp::Task<bool> {
               sizes[current_name] += chunk.size();
               co_return true;
             }});
        if (error) {
          response.status = error.code == chhttp::Error::body_too_large ? 413
                                                                        : 400;
          response.set_content(error.message);
          co_return;
        }
        response.set_content(
            R"({"metadata_bytes":)" + std::to_string(sizes["metadata"]) +
                R"(,"artifact_bytes":)" +
                std::to_string(sizes["artifact"]) + "}",
            "application/json");
      },
      {.max_body_size = 128 * 1024 * 1024});
  if (!gateway.start("127.0.0.1", 0)) return 2;

  // Known-size fields and files produce Content-Length. add_stream() can be
  // used for generated data; omit its size to select HTTP/1.1 chunked framing.
  chhttp::MultipartWriter multipart;
  try {
    multipart.add_field("metadata", R"({"agent":"tokmon","kind":"trace"})",
                        "application/json")
        .add_file("artifact", artifact);
  } catch (const std::exception &exception) {
    std::cerr << exception.what() << '\n';
    gateway.stop();
    return 3;
  }
  chhttp::Request request;
  request.method = "POST";
  request.target = "/v1/agent-artifacts";
  multipart.apply(request);

  chhttp::AsyncClient client("http://127.0.0.1:" +
                             std::to_string(gateway.port()));
  auto response = client.request(std::move(request)).get();
  gateway.stop();
  if (!response) {
    std::cerr << response.error().message << '\n';
    return 4;
  }
  std::cout << response->body << '\n';
  return response->status == 200 ? 0 : 5;
}
