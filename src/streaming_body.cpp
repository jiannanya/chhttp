#include "detail.hpp"

#include <array>
#include <cerrno>
#include <fstream>
#include <limits>
#include <system_error>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace chhttp {
namespace {

class FileIoPool {
public:
  FileIoPool() {
    const auto count =
        std::clamp<unsigned>(std::thread::hardware_concurrency(), 2, 4);
    workers_.reserve(count);
    for (unsigned index = 0; index < count; ++index) {
      workers_.emplace_back([this](std::stop_token token) {
        for (;;) {
          std::function<void()> job;
          {
            std::unique_lock lock(mutex_);
            ready_.wait(lock, token, [&] { return !jobs_.empty(); });
            if (token.stop_requested() && jobs_.empty())
              return;
            job = std::move(jobs_.front());
            jobs_.pop_front();
          }
          job();
        }
      });
    }
  }

  ~FileIoPool() {
    for (auto &worker : workers_)
      worker.request_stop();
    ready_.notify_all();
  }

  void submit(std::function<void()> job) {
    {
      std::lock_guard lock(mutex_);
      jobs_.push_back(std::move(job));
    }
    ready_.notify_one();
  }

  static FileIoPool &instance() {
    static FileIoPool pool;
    return pool;
  }

private:
  std::mutex mutex_;
  std::condition_variable_any ready_;
  std::deque<std::function<void()>> jobs_;
  std::vector<std::jthread> workers_;
};

struct FileReadResult {
  std::string data;
  bool failed{false};
};

struct FileReadAwaiter {
  struct State {
    std::ifstream *input{nullptr};
    std::size_t size{0};
    std::coroutine_handle<> continuation{};
    FileReadResult result;
  };

  std::shared_ptr<State> state;
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> continuation) {
    state->continuation = continuation;
    FileIoPool::instance().submit([state = state] {
      state->result.data.resize(state->size);
      state->input->read(state->result.data.data(),
                         static_cast<std::streamsize>(state->size));
      const auto count = state->input->gcount();
      state->result.data.resize(count > 0 ? static_cast<std::size_t>(count)
                                          : 0);
      state->result.failed = state->input->bad();
      state->continuation.resume();
    });
  }
  FileReadResult await_resume() { return std::move(state->result); }
};

FileReadAwaiter async_file_read(std::ifstream &input, std::size_t chunk_size) {
  return FileReadAwaiter{std::make_shared<FileReadAwaiter::State>(
      FileReadAwaiter::State{.input = &input, .size = chunk_size})};
}

Result<std::uint64_t> validate_file_slice(const std::filesystem::path &path,
                                          FileBodyOptions options) {
  std::error_code error;
  if (!std::filesystem::is_regular_file(path, error) || error)
    return ErrorInfo{Error::invalid_argument,
                     "File body path is not a readable regular file"};
  const auto size = std::filesystem::file_size(path, error);
  if (error)
    return ErrorInfo{Error::read, "Unable to determine file body size"};
  if (options.offset > size)
    return ErrorInfo{Error::invalid_argument,
                     "File body offset exceeds the file size"};
  const auto available = size - options.offset;
  const auto length = options.length.value_or(available);
  if (length > available)
    return ErrorInfo{Error::invalid_argument,
                     "File body length exceeds the available file data"};
  if (options.offset >
      static_cast<std::uint64_t>((std::numeric_limits<std::streamoff>::max)()))
    return ErrorInfo{Error::invalid_argument,
                     "File body offset exceeds the stream limit"};
  return length;
}

StreamHandler make_file_producer(std::filesystem::path path,
                                 std::uint64_t offset, std::uint64_t length) {
  return [path = std::move(path), offset,
          length](StreamWriter &writer) -> Task<void> {
    if (length == 0)
      co_return;
    std::ifstream input(path, std::ios::binary);
    if (!input)
      throw detail::StreamError(
          {Error::read, "Unable to open file request body"});
    input.seekg(static_cast<std::streamoff>(offset));
    if (!input)
      throw detail::StreamError(
          {Error::read, "Unable to seek file request body"});
    std::uint64_t remaining = length;
    while (remaining != 0) {
      const auto requested = static_cast<std::size_t>(
          std::min<std::uint64_t>(remaining, 64 * 1024));
      auto chunk = co_await async_file_read(input, requested);
      if (chunk.failed)
        throw detail::StreamError(
            {Error::read, "Unable to read file request body"});
      if (chunk.data.empty())
        throw detail::StreamError(
            {Error::read, "File request body ended before its fixed length"});
      if (!co_await writer.write(chunk.data))
        co_return;
      remaining -= chunk.data.size();
    }
  };
}

std::vector<std::string> split_parameters(std::string_view value) {
  std::vector<std::string> output;
  std::size_t start = 0;
  bool quoted = false;
  bool escaped = false;
  for (std::size_t index = 0; index <= value.size(); ++index) {
    const char ch = index == value.size() ? ';' : value[index];
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
      output.push_back(detail::trim(value.substr(start, index - start)));
      start = index + 1;
    }
  }
  return output;
}

Result<std::string> decode_parameter(std::string_view value) {
  auto text = detail::trim(value);
  if (text.empty() || text.front() != '"') return text;
  if (text.size() < 2 || text.back() != '"')
    return ErrorInfo{Error::multipart, "Unterminated quoted parameter"};
  std::string output;
  output.reserve(text.size() - 2);
  for (std::size_t index = 1; index + 1 < text.size(); ++index) {
    if (text[index] == '\\' && index + 2 < text.size()) ++index;
    output.push_back(text[index]);
  }
  return output;
}

bool valid_boundary(std::string_view boundary) noexcept {
  if (boundary.empty() || boundary.size() > 70 || boundary.back() == ' ')
    return false;
  constexpr std::string_view punctuation = "'()+_,-./:=? ";
  return std::ranges::all_of(boundary, [&](unsigned char ch) {
    return std::isalnum(ch) ||
           punctuation.find(static_cast<char>(ch)) != std::string_view::npos;
  });
}

Result<std::string> multipart_boundary(std::string_view content_type) {
  auto parameters = split_parameters(content_type);
  if (parameters.empty() ||
      !detail::iequals(parameters.front(), "multipart/form-data"))
    return ErrorInfo{Error::multipart,
                     "Content-Type is not multipart/form-data"};
  for (std::size_t index = 1; index < parameters.size(); ++index) {
    const auto equal = parameters[index].find('=');
    if (equal == std::string::npos ||
        !detail::iequals(detail::trim(std::string_view(parameters[index])
                                         .substr(0, equal)),
                         "boundary"))
      continue;
    auto boundary = decode_parameter(
        std::string_view(parameters[index]).substr(equal + 1));
    if (!boundary) return boundary.error();
    if (!valid_boundary(*boundary))
      return ErrorInfo{Error::multipart, "Invalid multipart boundary"};
    return boundary;
  }
  return ErrorInfo{Error::multipart, "Missing multipart boundary"};
}

std::string quote_parameter(std::string_view value) {
  std::string output;
  output.reserve(value.size());
  for (char ch : value) {
    if (ch == '\r' || ch == '\n') continue;
    if (ch == '\\' || ch == '"') output.push_back('\\');
    output.push_back(ch);
  }
  return output;
}

Result<MultipartPart> parse_part_head(std::string_view input) {
  MultipartPart part;
  std::size_t cursor = 0;
  while (cursor < input.size()) {
    const auto end = input.find("\r\n", cursor);
    const auto line_end = end == std::string_view::npos ? input.size() : end;
    const auto line = input.substr(cursor, line_end - cursor);
    if (line.find('\n') != std::string_view::npos)
      return ErrorInfo{Error::multipart, "Bare LF in multipart header"};
    const auto colon = line.find(':');
    if (colon == std::string_view::npos)
      return ErrorInfo{Error::multipart, "Malformed multipart header"};
    auto name = detail::trim(line.substr(0, colon));
    auto value = detail::trim(line.substr(colon + 1));
    if (!detail::valid_header_name(name) || !detail::valid_header_value(value))
      return ErrorInfo{Error::multipart, "Invalid multipart header"};
    part.headers.add(name, value);
    if (detail::iequals(name, "Content-Type")) part.content_type = value;
    if (detail::iequals(name, "Content-Disposition")) {
      auto parameters = split_parameters(value);
      if (parameters.empty() ||
          !detail::iequals(parameters.front(), "form-data"))
        return ErrorInfo{Error::multipart,
                         "Unsupported multipart content disposition"};
      for (std::size_t index = 1; index < parameters.size(); ++index) {
        const auto equal = parameters[index].find('=');
        if (equal == std::string::npos) continue;
        auto key = detail::trim(
            std::string_view(parameters[index]).substr(0, equal));
        auto decoded = decode_parameter(
            std::string_view(parameters[index]).substr(equal + 1));
        if (!decoded) return decoded.error();
        if (detail::iequals(key, "name")) part.name = *decoded;
        if (detail::iequals(key, "filename")) part.filename = *decoded;
      }
    }
    if (end == std::string_view::npos) break;
    cursor = end + 2;
  }
  if (part.headers.get_all("Content-Disposition").size() != 1)
    return ErrorInfo{Error::multipart,
                     "Multipart part requires one Content-Disposition"};
  if (part.headers.get_all("Content-Type").size() > 1)
    return ErrorInfo{Error::multipart,
                     "Multipart part has ambiguous Content-Type headers"};
  return part;
}

void validate_part_metadata(const MultipartPart &part) {
  if (part.name.empty() || part.name.find_first_of("\r\n") != std::string::npos)
    throw std::invalid_argument("Multipart part name is invalid");
  if (part.filename.find_first_of("\r\n") != std::string::npos)
    throw std::invalid_argument("Multipart filename is invalid");
  if (!part.content_type.empty() &&
      !detail::valid_header_value(part.content_type))
    throw std::invalid_argument("Multipart content type is invalid");
  for (const auto &[name, value] : part.headers) {
    if (!detail::valid_header_name(name) || !detail::valid_header_value(value))
      throw std::invalid_argument("Multipart custom header is invalid");
  }
}

std::string serialize_part_head(const MultipartPart &part) {
  std::string output = "Content-Disposition: form-data; name=\"" +
                       quote_parameter(part.name) + "\"";
  if (!part.filename.empty())
    output += "; filename=\"" + quote_parameter(part.filename) + "\"";
  output += "\r\n";
  if (!part.content_type.empty())
    output += "Content-Type: " + part.content_type + "\r\n";
  for (const auto &[name, value] : part.headers) {
    if (detail::iequals(name, "Content-Disposition") ||
        detail::iequals(name, "Content-Type"))
      continue;
    if (detail::valid_header_name(name) && detail::valid_header_value(value))
      output += name + ": " + value + "\r\n";
  }
  output += "\r\n";
  return output;
}

std::string json_escape(std::string_view value) {
  constexpr char hex[] = "0123456789abcdef";
  std::string output;
  output.reserve(value.size());
  for (unsigned char ch : value) {
    switch (ch) {
    case '"': output += "\\\""; break;
    case '\\': output += "\\\\"; break;
    case '\b': output += "\\b"; break;
    case '\f': output += "\\f"; break;
    case '\n': output += "\\n"; break;
    case '\r': output += "\\r"; break;
    case '\t': output += "\\t"; break;
    default:
      if (ch < 0x20) {
        output += "\\u00";
        output.push_back(hex[ch >> 4]);
        output.push_back(hex[ch & 0x0f]);
      } else {
        output.push_back(static_cast<char>(ch));
      }
    }
  }
  return output;
}

} // namespace

Result<void> Request::set_file_body(std::filesystem::path path,
                                    FileBodyOptions options) {
  auto length = validate_file_slice(path, options);
  if (!length)
    return length.error();
  set_stream_body(make_file_producer(std::move(path), options.offset, *length),
                  *length);
  return {};
}

class AsyncFileSink::Impl
    : public std::enable_shared_from_this<AsyncFileSink::Impl> {
public:
  struct ActionState {
    std::function<ErrorInfo(Impl &)> action;
    std::coroutine_handle<> continuation{};
    ErrorInfo result;
  };

  struct ActionAwaiter {
    std::shared_ptr<Impl> impl;
    std::shared_ptr<ActionState> state;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> continuation) {
      state->continuation = continuation;
      impl->submit(state);
    }
    ErrorInfo await_resume() { return std::move(state->result); }
  };

  ActionAwaiter enqueue(std::function<ErrorInfo(Impl &)> action) {
    return ActionAwaiter{
        shared_from_this(),
        std::make_shared<ActionState>(ActionState{.action = std::move(action)})};
  }

  void submit(std::shared_ptr<ActionState> state) {
    bool start_worker = false;
    {
      std::lock_guard lock(queue_mutex);
      pending.push_back(std::move(state));
      if (!worker_running) {
        worker_running = true;
        start_worker = true;
      }
    }
    if (start_worker) {
      auto self = shared_from_this();
      FileIoPool::instance().submit(
          [self = std::move(self)] { self->process(); });
    }
  }

  void process() {
    for (;;) {
      std::shared_ptr<ActionState> state;
      {
        std::lock_guard lock(queue_mutex);
        if (pending.empty()) {
          worker_running = false;
          return;
        }
        state = std::move(pending.front());
        pending.pop_front();
      }

      try {
        state->result = state->action(*this);
      } catch (const std::exception &exception) {
        state->result = {Error::internal,
                         "Asynchronous file operation failed: " +
                             std::string(exception.what())};
      } catch (...) {
        state->result = {Error::internal,
                         "Asynchronous file operation failed"};
      }
      state->continuation.resume();
    }
  }

  std::fstream output;
  std::filesystem::path path;
  std::atomic<std::uint64_t> written{0};
  std::atomic<std::uint64_t> position{0};

private:
  std::mutex queue_mutex;
  std::deque<std::shared_ptr<ActionState>> pending;
  bool worker_running{false};
};

Result<AsyncFileSink> AsyncFileSink::open(std::filesystem::path path,
                                          FileSinkOptions options) {
  if (path.empty())
    return ErrorInfo{Error::invalid_argument,
                     "Asynchronous file sink path is empty"};
  if (options.offset >
      static_cast<std::uint64_t>((std::numeric_limits<std::streamoff>::max)()))
    return ErrorInfo{Error::invalid_argument,
                     "Asynchronous file sink offset exceeds the stream limit"};
  if (options.mode != FileSinkMode::write_at && options.offset != 0)
    return ErrorInfo{Error::invalid_argument,
                     "File sink offset is only valid in write_at mode"};

  auto impl = std::make_shared<Impl>();
  impl->path = std::move(path);
  const auto read_write = std::ios::binary | std::ios::in | std::ios::out;
  if (options.mode == FileSinkMode::truncate) {
    impl->output.open(impl->path, read_write | std::ios::trunc);
  } else {
    impl->output.open(impl->path, read_write);
    if (!impl->output) {
      impl->output.clear();
      std::ofstream create(impl->path, std::ios::binary | std::ios::trunc);
      create.close();
      impl->output.open(impl->path, read_write);
    }
  }
  if (!impl->output)
    return ErrorInfo{Error::write, "Unable to open asynchronous file sink"};

  std::uint64_t position = options.offset;
  if (options.mode == FileSinkMode::append) {
    impl->output.seekp(0, std::ios::end);
    const auto end = impl->output.tellp();
    if (end < 0)
      return ErrorInfo{Error::write,
                       "Unable to determine asynchronous file sink size"};
    position = static_cast<std::uint64_t>(end);
  } else {
    impl->output.seekp(static_cast<std::streamoff>(position));
  }
  if (!impl->output)
    return ErrorInfo{Error::write, "Unable to seek asynchronous file sink"};
  impl->position.store(position, std::memory_order_relaxed);
  return AsyncFileSink(std::move(impl));
}

AsyncFileSink::~AsyncFileSink() = default;
AsyncFileSink::AsyncFileSink(AsyncFileSink &&) noexcept = default;
AsyncFileSink &AsyncFileSink::operator=(AsyncFileSink &&) noexcept = default;

Task<ErrorInfo> AsyncFileSink::write(std::string_view data) {
  if (!impl_)
    co_return ErrorInfo{Error::invalid_argument,
                        "Asynchronous file sink is not open"};
  std::string owned(data);
  co_return co_await impl_->enqueue([owned = std::move(owned)](
                                        Impl &impl) -> ErrorInfo {
    if (owned.empty())
      return {};
    if (owned.size() >
        static_cast<std::size_t>((std::numeric_limits<std::streamsize>::max)()))
      return {Error::invalid_argument,
              "Asynchronous file write exceeds the stream limit"};
    impl.output.write(owned.data(), static_cast<std::streamsize>(owned.size()));
    if (!impl.output)
      return {Error::write, "Unable to write asynchronous file sink"};
    impl.written.fetch_add(owned.size(), std::memory_order_relaxed);
    impl.position.fetch_add(owned.size(), std::memory_order_relaxed);
    return {};
  });
}

Task<ErrorInfo> AsyncFileSink::flush() {
  if (!impl_)
    co_return ErrorInfo{Error::invalid_argument,
                        "Asynchronous file sink is not open"};
  co_return co_await impl_->enqueue([](Impl &impl) -> ErrorInfo {
    impl.output.flush();
    if (!impl.output)
      return {Error::write, "Unable to flush asynchronous file sink"};
    return {};
  });
}

Task<ErrorInfo> AsyncFileSink::sync() {
  if (!impl_)
    co_return ErrorInfo{Error::invalid_argument,
                        "Asynchronous file sink is not open"};
  co_return co_await impl_->enqueue([](Impl &impl) -> ErrorInfo {
    impl.output.flush();
    if (!impl.output)
      return {Error::write, "Unable to flush asynchronous file sink"};
#ifdef _WIN32
    const auto handle =
        CreateFileW(impl.path.c_str(), GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE)
      return {Error::write, "Unable to open file for durable sync",
              static_cast<int>(GetLastError())};
    const auto synced = FlushFileBuffers(handle);
    const auto system_code = synced ? 0 : static_cast<int>(GetLastError());
    CloseHandle(handle);
    if (!synced)
      return {Error::write, "Unable to durably sync file", system_code};
#else
    const auto descriptor = ::open(impl.path.c_str(), O_WRONLY);
    if (descriptor == -1)
      return {Error::write, "Unable to open file for durable sync", errno};
    const auto synced = ::fsync(descriptor);
    const auto system_code = synced == 0 ? 0 : errno;
    ::close(descriptor);
    if (synced != 0)
      return {Error::write, "Unable to durably sync file", system_code};
#endif
    return {};
  });
}

std::uint64_t AsyncFileSink::written() const noexcept {
  return impl_ ? impl_->written.load(std::memory_order_relaxed) : 0;
}

std::uint64_t AsyncFileSink::position() const noexcept {
  return impl_ ? impl_->position.load(std::memory_order_relaxed) : 0;
}

Task<ErrorInfo> RequestBodyStream::consume(AsyncBodyConsumer consumer) {
  if (!source_)
    co_return ErrorInfo{Error::invalid_argument,
                        "Request body stream is not connected"};
  if (!consumer)
    co_return ErrorInfo{Error::invalid_argument,
                        "Request body consumer is empty"};
  co_return co_await source_->consume(std::move(consumer));
}

Task<ErrorInfo> RequestBodyStream::consume(BodyConsumer consumer) {
  if (!consumer)
    co_return ErrorInfo{Error::invalid_argument,
                        "Request body consumer is empty"};
  co_return co_await consume(
      [consumer = std::move(consumer)](std::string_view data) -> Task<bool> {
        co_return consumer(data);
      });
}

Task<ErrorInfo> RequestBodyStream::discard() {
  co_return co_await consume(
      [](std::string_view) -> Task<bool> { co_return true; });
}

Task<Result<StoredBody>>
RequestBodyStream::save_to_file(std::filesystem::path path) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output)
    co_return ErrorInfo{Error::write, "Unable to open request body file"};
  std::uint64_t size = 0;
  bool write_failed = false;
  auto error = co_await consume(
      [&](std::string_view data) -> Task<bool> {
        output.write(data.data(), static_cast<std::streamsize>(data.size()));
        if (!output) {
          write_failed = true;
          co_return false;
        }
        size += data.size();
        co_return true;
      });
  output.flush();
  if (!output) write_failed = true;
  if (write_failed)
    co_return ErrorInfo{Error::write, "Unable to write request body file"};
  if (error) co_return error;
  co_return StoredBody{std::move(path), size};
}

Task<Result<StoredBody>>
RequestBodyStream::save_to_temp(std::filesystem::path directory) {
  std::error_code filesystem_error;
  if (directory.empty())
    directory = std::filesystem::temp_directory_path(filesystem_error);
  if (filesystem_error)
    co_return ErrorInfo{Error::write,
                        "Unable to locate the temporary directory"};
  std::filesystem::path owner;
  for (unsigned attempt = 0; attempt != 100; ++attempt) {
    owner = directory / ("chhttp-upload-" + detail::random_boundary());
    filesystem_error.clear();
    if (std::filesystem::create_directory(owner, filesystem_error)) break;
    owner.clear();
  }
  if (owner.empty())
    co_return ErrorInfo{Error::write,
                        "Unable to create a temporary upload directory"};
  auto stored = co_await save_to_file(owner / "body.bin");
  if (!stored) {
    std::filesystem::remove_all(owner, filesystem_error);
    co_return stored.error();
  }
  co_return stored;
}

void RequestBodyStream::cancel() noexcept {
  if (source_) source_->cancel();
}

std::optional<std::uint64_t>
RequestBodyStream::content_length() const noexcept {
  return source_ ? source_->content_length() : std::nullopt;
}

std::uint64_t RequestBodyStream::received() const noexcept {
  return source_ ? source_->received() : 0;
}

bool RequestBodyStream::consumed() const noexcept {
  return source_ && source_->consumed();
}

bool RequestBodyStream::complete() const noexcept {
  return source_ && source_->complete();
}

class MultipartParser::Impl {
public:
  enum class State { first_boundary, headers, body, boundary_suffix, complete };

  Impl(std::string content_type, MultipartParserOptions parser_options)
      : options(parser_options) {
    auto parsed = multipart_boundary(content_type);
    if (!parsed) {
      error = parsed.error();
      return;
    }
    boundary = std::move(*parsed);
    first_marker = "--" + boundary;
    delimiter = "\r\n" + first_marker;
  }

  Result<std::vector<MultipartEvent>> feed(std::string_view input) {
    if (error) return error;
    if (state == State::complete) return std::vector<MultipartEvent>{};
    buffer.append(input);
    std::vector<MultipartEvent> events;
    for (;;) {
      const auto previous_size = buffer.size();
      const auto previous_state = state;
      if (state == State::first_boundary) {
        auto position = buffer.find(first_marker);
        while (position != std::string::npos && position != 0 &&
               (position < 2 || buffer.substr(position - 2, 2) != "\r\n"))
          position = buffer.find(first_marker, position + 1);
        if (position == std::string::npos) {
          const auto keep = first_marker.size() + 2;
          if (buffer.size() > options.max_header_size + keep)
            return fail("Multipart preamble exceeds configured limit");
          break;
        }
        const auto suffix = position + first_marker.size();
        if (buffer.size() < suffix + 2) {
          if (position > 0) buffer.erase(0, position);
          break;
        }
        const auto ending = std::string_view(buffer).substr(suffix, 2);
        if (ending != "\r\n" && ending != "--") {
          buffer.erase(0, position + 1);
          continue;
        }
        buffer.erase(0, suffix);
        state = State::boundary_suffix;
      } else if (state == State::boundary_suffix) {
        if (buffer.size() < 2) break;
        if (buffer.starts_with("--")) {
          buffer.erase(0, 2);
          state = State::complete;
        } else if (buffer.starts_with("\r\n")) {
          buffer.erase(0, 2);
          state = State::headers;
        } else {
          return fail("Malformed multipart delimiter suffix");
        }
      } else if (state == State::headers) {
        const auto end = buffer.find("\r\n\r\n");
        if (end == std::string::npos) {
          if (buffer.size() > options.max_header_size)
            return fail("Multipart part headers exceed configured limit");
          break;
        }
        if (end > options.max_header_size)
          return fail("Multipart part headers exceed configured limit");
        auto parsed = parse_part_head(std::string_view(buffer).substr(0, end));
        if (!parsed) return fail(parsed.error().message);
        if (part_count >= options.max_parts)
          return fail("Too many multipart parts");
        current = std::move(*parsed);
        current_size = 0;
        events.push_back({MultipartEventType::part_begin, part_count,
                          current, {}});
        ++part_count;
        buffer.erase(0, end + 4);
        state = State::body;
      } else if (state == State::body) {
        auto position = buffer.find(delimiter);
        if (position != std::string::npos) {
          const auto suffix = position + delimiter.size();
          if (buffer.size() < suffix + 2) {
            if (!emit_data(events, position)) return error;
            buffer.erase(0, position);
            break;
          }
          const auto ending = std::string_view(buffer).substr(suffix, 2);
          if (ending != "\r\n" && ending != "--") {
            if (!emit_data(events, position + 2)) return error;
            buffer.erase(0, position + 2);
            continue;
          }
          if (!emit_data(events, position)) return error;
          events.push_back({MultipartEventType::part_end, part_count - 1,
                            current, {}});
          buffer.erase(0, suffix);
          state = State::boundary_suffix;
        } else {
          const auto keep = delimiter.size() + 1;
          if (buffer.size() <= keep) break;
          const auto count = buffer.size() - keep;
          if (!emit_data(events, count)) return error;
          buffer.erase(0, count);
        }
      } else {
        buffer.clear();
        break;
      }
      if (previous_size == buffer.size() && previous_state == state) break;
    }
    return events;
  }

  Result<std::vector<MultipartEvent>> finish() {
    auto events = feed({});
    if (!events) return events;
    if (state != State::complete)
      return fail("Multipart body has no complete closing delimiter");
    return events;
  }

  Result<std::vector<MultipartEvent>> fail(std::string message) {
    error = ErrorInfo{Error::multipart, std::move(message)};
    return error;
  }

  bool emit_data(std::vector<MultipartEvent> &events, std::size_t count) {
    if (count == 0) return true;
    if (count > options.max_part_size ||
        current_size > options.max_part_size - count) {
      error = {Error::body_too_large,
               "Multipart part exceeds configured limit"};
      return false;
    }
    if (count > options.max_total_size ||
        total_size > options.max_total_size - count) {
      error = {Error::body_too_large,
               "Multipart data exceeds configured limit"};
      return false;
    }
    current_size += count;
    total_size += count;
    events.push_back({MultipartEventType::part_data, part_count - 1, {},
                      buffer.substr(0, count)});
    return true;
  }

  MultipartParserOptions options;
  ErrorInfo error;
  State state{State::first_boundary};
  std::string boundary;
  std::string first_marker;
  std::string delimiter;
  std::string buffer;
  MultipartPart current;
  std::size_t part_count{0};
  std::uint64_t current_size{0};
  std::uint64_t total_size{0};
};

MultipartParser::MultipartParser(std::string content_type,
                                 MultipartParserOptions options)
    : impl_(std::make_unique<Impl>(std::move(content_type), options)) {}
MultipartParser::~MultipartParser() = default;
MultipartParser::MultipartParser(MultipartParser &&) noexcept = default;
MultipartParser &MultipartParser::operator=(MultipartParser &&) noexcept =
    default;

Result<std::vector<MultipartEvent>> MultipartParser::feed(std::string_view data) {
  return impl_->feed(data);
}

Result<std::vector<MultipartEvent>> MultipartParser::finish() {
  return impl_->finish();
}

bool MultipartParser::complete() const noexcept {
  return impl_->state == Impl::State::complete && !impl_->error;
}

Task<ErrorInfo> consume_multipart(RequestBodyStream &body,
                                  std::string content_type,
                                  MultipartCallbacks callbacks,
                                  MultipartParserOptions options) {
  MultipartParser parser(std::move(content_type), options);
  ErrorInfo parser_error;
  auto dispatch = [&](std::vector<MultipartEvent> events) -> Task<bool> {
    for (auto &event : events) {
      bool accepted = true;
      if (event.type == MultipartEventType::part_begin &&
          callbacks.on_part_begin)
        accepted = co_await callbacks.on_part_begin(event.part);
      else if (event.type == MultipartEventType::part_data &&
               callbacks.on_part_data)
        accepted = co_await callbacks.on_part_data(event.data);
      else if (event.type == MultipartEventType::part_end &&
               callbacks.on_part_end)
        accepted = co_await callbacks.on_part_end(event.part);
      if (!accepted) co_return false;
    }
    co_return true;
  };
  auto consumed = co_await body.consume(
      [&](std::string_view data) -> Task<bool> {
        auto events = parser.feed(data);
        if (!events) {
          parser_error = events.error();
          co_return false;
        }
        co_return co_await dispatch(std::move(*events));
      });
  if (parser_error) co_return parser_error;
  if (consumed) co_return consumed;
  auto final_events = parser.finish();
  if (!final_events) co_return final_events.error();
  if (!co_await dispatch(std::move(*final_events)))
    co_return ErrorInfo{Error::cancelled,
                        "Multipart consumer cancelled the transfer"};
  co_return ErrorInfo{};
}

class MultipartWriter::Impl {
public:
  struct Part {
    MultipartPart metadata;
    StreamHandler producer;
    std::optional<std::uint64_t> size;
    std::string data;
  };

  std::string prefix(const Part &part) const {
    return "--" + boundary + "\r\n" + serialize_part_head(part.metadata);
  }

  std::string boundary;
  std::vector<Part> parts;
};

MultipartWriter::MultipartWriter(std::string boundary)
    : impl_(std::make_shared<Impl>()) {
  if (!valid_boundary(boundary))
    boundary = detail::random_boundary();
  impl_->boundary = std::move(boundary);
}

MultipartWriter &MultipartWriter::add_field(std::string name, std::string value,
                                            std::string content_type) {
  Impl::Part part;
  part.metadata.name = std::move(name);
  part.metadata.content_type = std::move(content_type);
  validate_part_metadata(part.metadata);
  part.size = value.size();
  part.data = std::move(value);
  impl_->parts.push_back(std::move(part));
  return *this;
}

MultipartWriter &MultipartWriter::add_file(std::string name,
                                           std::filesystem::path path,
                                           std::string content_type,
                                           std::string filename) {
  return add_file_slice(std::move(name), std::move(path), {},
                        std::move(content_type), std::move(filename));
}

MultipartWriter &MultipartWriter::add_file_slice(std::string name,
                                                 std::filesystem::path path,
                                                 FileBodyOptions options,
                                                 std::string content_type,
                                                 std::string filename) {
  auto size = validate_file_slice(path, options);
  if (!size)
    throw std::invalid_argument(size.error().message);
  if (content_type.empty())
    content_type = mime_type(path);
  if (filename.empty())
    filename = path.filename().string();
  return add_stream(
      std::move(name), std::move(filename), std::move(content_type),
      make_file_producer(std::move(path), options.offset, *size), *size);
}

MultipartWriter &MultipartWriter::add_stream(std::string name,
                                             std::string filename,
                                             std::string content_type,
                                             StreamHandler producer,
                                             std::optional<std::uint64_t> size,
                                             Headers headers) {
  if (!producer)
    throw std::invalid_argument("Multipart producer is empty");
  Impl::Part part;
  part.metadata.name = std::move(name);
  part.metadata.filename = std::move(filename);
  part.metadata.content_type = std::move(content_type);
  part.metadata.headers = std::move(headers);
  validate_part_metadata(part.metadata);
  part.producer = std::move(producer);
  part.size = size;
  impl_->parts.push_back(std::move(part));
  return *this;
}

std::string MultipartWriter::content_type() const {
  return "multipart/form-data; boundary=" + impl_->boundary;
}

std::optional<std::uint64_t> MultipartWriter::content_length() const {
  std::uint64_t total = 0;
  const auto add = [&](std::uint64_t value) {
    if (value > std::numeric_limits<std::uint64_t>::max() - total) return false;
    total += value;
    return true;
  };
  for (const auto &part : impl_->parts) {
    if (!part.size) return std::nullopt;
    if (!add(impl_->prefix(part).size()) || !add(*part.size) || !add(2))
      return std::nullopt;
  }
  if (!add(impl_->boundary.size() + 6)) return std::nullopt;
  return total;
}

Task<void> MultipartWriter::write(StreamWriter &writer) const {
  for (const auto &part : impl_->parts) {
    if (!co_await writer.write(impl_->prefix(part))) co_return;
    if (part.producer) {
      co_await part.producer(writer);
      if (!writer.open()) co_return;
    } else if (!part.data.empty() && !co_await writer.write(part.data)) {
      co_return;
    }
    if (!co_await writer.write("\r\n")) co_return;
  }
  co_await writer.write("--" + impl_->boundary + "--\r\n");
}

void MultipartWriter::apply(Request &request) const {
  request.headers.set("Content-Type", content_type());
  auto self = *this;
  request.set_stream_body(
      [self = std::move(self)](StreamWriter &writer) mutable -> Task<void> {
        co_await self.write(writer);
      },
      content_length());
}

std::string Base64Encoder::update(std::span<const std::byte> data) {
  if (finished_) throw std::logic_error("Base64 encoder is already finished");
  constexpr char alphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string output;
  output.reserve(((pending_size_ + data.size()) / 3) * 4);
  auto emit = [&](const std::array<std::byte, 3> &bytes) {
    const auto value = (std::to_integer<unsigned>(bytes[0]) << 16) |
                       (std::to_integer<unsigned>(bytes[1]) << 8) |
                       std::to_integer<unsigned>(bytes[2]);
    output.push_back(alphabet[(value >> 18) & 63]);
    output.push_back(alphabet[(value >> 12) & 63]);
    output.push_back(alphabet[(value >> 6) & 63]);
    output.push_back(alphabet[value & 63]);
  };
  std::size_t cursor = 0;
  while (pending_size_ < 3 && cursor < data.size())
    pending_[pending_size_++] = data[cursor++];
  if (pending_size_ == 3) {
    emit(pending_);
    pending_size_ = 0;
  }
  while (cursor + 3 <= data.size()) {
    std::array<std::byte, 3> bytes{data[cursor], data[cursor + 1],
                                   data[cursor + 2]};
    emit(bytes);
    cursor += 3;
  }
  while (cursor < data.size()) pending_[pending_size_++] = data[cursor++];
  return output;
}

std::string Base64Encoder::finish() {
  if (finished_) return {};
  finished_ = true;
  constexpr char alphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  if (pending_size_ == 0) return {};
  const auto first = std::to_integer<unsigned>(pending_[0]);
  const auto second = pending_size_ == 2
                          ? std::to_integer<unsigned>(pending_[1])
                          : 0u;
  std::string output;
  output.push_back(alphabet[first >> 2]);
  output.push_back(alphabet[((first & 3) << 4) | (second >> 4)]);
  output.push_back(pending_size_ == 2 ? alphabet[(second & 15) << 2] : '=');
  output.push_back('=');
  pending_size_ = 0;
  return output;
}

void Base64Encoder::reset() noexcept {
  pending_size_ = 0;
  finished_ = false;
}

std::optional<std::uint64_t>
base64_encoded_size(std::uint64_t input_size) noexcept {
  constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
  if (input_size > maximum - 2) return std::nullopt;
  const auto groups = (input_size + 2) / 3;
  if (groups > maximum / 4) return std::nullopt;
  return groups * 4;
}

Task<bool> JsonStreamWriter::raw(std::string_view json) {
  if (base64_string_open_) co_return false;
  co_return co_await writer_.write(json);
}

Task<bool> JsonStreamWriter::string(std::string_view value) {
  if (base64_string_open_) co_return false;
  if (!co_await writer_.write("\"")) co_return false;
  constexpr std::size_t chunk_size = 16 * 1024;
  for (std::size_t offset = 0; offset < value.size(); offset += chunk_size) {
    auto escaped = json_escape(value.substr(offset, chunk_size));
    if (!escaped.empty() && !co_await writer_.write(escaped)) co_return false;
  }
  co_return co_await writer_.write("\"");
}

Task<bool> JsonStreamWriter::begin_base64_string(std::string_view prefix) {
  if (base64_string_open_) co_return false;
  base64_.reset();
  if (!co_await writer_.write("\"" + json_escape(prefix))) co_return false;
  base64_string_open_ = true;
  co_return true;
}

Task<bool> JsonStreamWriter::base64(std::span<const std::byte> data) {
  if (!base64_string_open_) co_return false;
  auto encoded = base64_.update(data);
  co_return encoded.empty() || co_await writer_.write(encoded);
}

Task<bool> JsonStreamWriter::end_base64_string() {
  if (!base64_string_open_) co_return false;
  auto tail = base64_.finish();
  if (!tail.empty() && !co_await writer_.write(tail)) co_return false;
  base64_string_open_ = false;
  co_return co_await writer_.write("\"");
}

bool JsonStreamWriter::open() const noexcept { return writer_.open(); }

} // namespace chhttp
