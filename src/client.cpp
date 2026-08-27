#include "detail.hpp"

#ifdef CHHTTP_HAS_TLS
#include <openssl/err.h>
#include <openssl/pem.h>
#ifdef _WIN32
#include <windows.h>
#include <wincrypt.h>
#endif
#endif

#include <fstream>
#include <mutex>
#include <unordered_map>

namespace chhttp::detail {

#ifdef CHHTTP_HAS_TLS
namespace {

bool load_windows_roots(SSL_CTX *context) {
#ifdef _WIN32
  HCERTSTORE store = CertOpenSystemStoreA(0, "ROOT");
  if (!store) return false;
  PCCERT_CONTEXT certificate_context = nullptr;
  X509_STORE *target = SSL_CTX_get_cert_store(context);
  while ((certificate_context =
              CertEnumCertificatesInStore(store, certificate_context)) != nullptr) {
    const unsigned char *encoded = certificate_context->pbCertEncoded;
    X509 *certificate = d2i_X509(nullptr, &encoded,
                                 certificate_context->cbCertEncoded);
    if (certificate) {
      X509_STORE_add_cert(target, certificate);
      X509_free(certificate);
      ERR_clear_error();
    }
  }
  CertCloseStore(store, 0);
  return true;
#else
  (void)context;
  return false;
#endif
}

std::string read_file(const std::filesystem::path &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) return {};
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

bool use_certificate_pem(SSL_CTX *context, std::string_view pem) {
  BIO *bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
  if (!bio) return false;
  X509 *certificate = PEM_read_bio_X509_AUX(bio, nullptr, nullptr, nullptr);
  bool ok = certificate && SSL_CTX_use_certificate(context, certificate) == 1;
  X509_free(certificate);
  while (ok) {
    X509 *chain = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    if (!chain) {
      ERR_clear_error();
      break;
    }
    if (SSL_CTX_add_extra_chain_cert(context, chain) != 1) {
      X509_free(chain);
      ok = false;
    }
  }
  BIO_free(bio);
  return ok;
}

bool use_private_key_pem(SSL_CTX *context, std::string_view pem,
                         std::string_view password = {}) {
  BIO *bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
  if (!bio) return false;
  std::string pass(password);
  EVP_PKEY *key = PEM_read_bio_PrivateKey(
      bio, nullptr, nullptr, pass.empty() ? nullptr : pass.data());
  const bool ok = key && SSL_CTX_use_PrivateKey(context, key) == 1;
  EVP_PKEY_free(key);
  BIO_free(bio);
  return ok;
}

bool configure_ciphers(SSL_CTX *context, const std::string &ciphers) {
  if (ciphers.empty()) return true;
  return SSL_CTX_set_cipher_list(context, ciphers.c_str()) == 1;
}

} // namespace

SSL_CTX *create_client_tls_context(const TlsClientOptions &options,
                                   ErrorInfo &error) {
  SSL_CTX *context = SSL_CTX_new(TLS_client_method());
  if (!context) {
    error = {Error::tls_configuration, "Unable to create TLS client context", 0,
             static_cast<long>(ERR_get_error())};
    return nullptr;
  }
  SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION);
  SSL_CTX_set_options(context, SSL_OP_NO_COMPRESSION);
  SSL_CTX_set_verify(context, options.verify_peer ? SSL_VERIFY_PEER
                                                   : SSL_VERIFY_NONE,
                     nullptr);
  bool ok = true;
  if (options.verify_peer && options.use_system_certificates) {
    const bool defaults = SSL_CTX_set_default_verify_paths(context) == 1;
    const bool windows = load_windows_roots(context);
    const bool has_explicit_roots =
        !options.ca_file.empty() || !options.ca_directory.empty();
    ok = defaults || windows || has_explicit_roots;
    if (has_explicit_roots) ERR_clear_error();
  }
  if (ok && !options.ca_file.empty())
    ok = SSL_CTX_load_verify_locations(context, options.ca_file.string().c_str(),
                                       nullptr) == 1;
  if (ok && !options.ca_directory.empty())
    ok = SSL_CTX_load_verify_locations(
             context, nullptr, options.ca_directory.string().c_str()) == 1;
  if (ok) ok = configure_ciphers(context, options.ciphers);
  if (ok && !options.certificate_pem.empty())
    ok = use_certificate_pem(context, options.certificate_pem);
  else if (ok && !options.certificate_file.empty())
    ok = SSL_CTX_use_certificate_chain_file(
             context, options.certificate_file.string().c_str()) == 1;
  if (ok && !options.private_key_pem.empty())
    ok = use_private_key_pem(context, options.private_key_pem);
  else if (ok && !options.private_key_file.empty())
    ok = SSL_CTX_use_PrivateKey_file(context,
                                     options.private_key_file.string().c_str(),
                                     SSL_FILETYPE_PEM) == 1;
  if (ok && (!options.private_key_pem.empty() || !options.private_key_file.empty()))
    ok = SSL_CTX_check_private_key(context) == 1;
  if (!ok) {
    error = {Error::tls_configuration, "Unable to configure TLS client context",
             0, static_cast<long>(ERR_get_error())};
    SSL_CTX_free(context);
    return nullptr;
  }
  return context;
}

SSL_CTX *create_server_tls_context(const TlsServerOptions &options,
                                   ErrorInfo &error) {
  SSL_CTX *context = SSL_CTX_new(TLS_server_method());
  if (!context) {
    error = {Error::tls_configuration, "Unable to create TLS server context", 0,
             static_cast<long>(ERR_get_error())};
    return nullptr;
  }
  SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION);
  SSL_CTX_set_options(context, SSL_OP_NO_COMPRESSION);
  bool ok = configure_ciphers(context, options.ciphers);
  if (ok && !options.certificate_pem.empty())
    ok = use_certificate_pem(context, options.certificate_pem);
  else if (ok && !options.certificate_file.empty())
    ok = SSL_CTX_use_certificate_chain_file(
             context, options.certificate_file.string().c_str()) == 1;
  std::string password;
  if (!options.private_key_password_file.empty())
    password = trim(read_file(options.private_key_password_file));
  if (ok && !options.private_key_pem.empty())
    ok = use_private_key_pem(context, options.private_key_pem, password);
  else if (ok && !options.private_key_file.empty()) {
    const auto pem = read_file(options.private_key_file);
    ok = !pem.empty() && use_private_key_pem(context, pem, password);
  }
  if (ok) ok = SSL_CTX_check_private_key(context) == 1;
  if (ok && !options.client_ca_file.empty())
    ok = SSL_CTX_load_verify_locations(
             context, options.client_ca_file.string().c_str(), nullptr) == 1;
  if (ok && options.require_client_certificate)
    SSL_CTX_set_verify(context,
                       SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                       nullptr);
  if (!ok) {
    error = {Error::tls_configuration, "Unable to configure TLS server context",
             0, static_cast<long>(ERR_get_error())};
    SSL_CTX_free(context);
    return nullptr;
  }
  return context;
}
#endif

} // namespace chhttp::detail

namespace chhttp {
namespace {

bool idempotent_method(std::string_view method) {
  return detail::iequals(method, "GET") || detail::iequals(method, "HEAD") ||
         detail::iequals(method, "OPTIONS");
}

bool redirect_status(int status) {
  return status == 301 || status == 302 || status == 303 || status == 307 ||
         status == 308;
}

class RequestControl {
public:
  void bind(const std::shared_ptr<detail::Connection> &connection) {
    bool close = false;
    {
      std::lock_guard lock(mutex_);
      connection_ = connection;
      close = cancelled_.load(std::memory_order_acquire);
    }
    if (close) connection->close();
  }

  void unbind(const std::shared_ptr<detail::Connection> &connection) {
    std::lock_guard lock(mutex_);
    if (connection_.lock() == connection) connection_.reset();
  }

  void cancel() {
    cancelled_.store(true, std::memory_order_release);
    std::shared_ptr<detail::Connection> connection;
    {
      std::lock_guard lock(mutex_);
      connection = connection_.lock();
    }
    if (connection) connection->close();
  }

  [[nodiscard]] bool cancelled() const noexcept {
    return cancelled_.load(std::memory_order_acquire);
  }

private:
  mutable std::mutex mutex_;
  std::weak_ptr<detail::Connection> connection_;
  std::atomic_bool cancelled_{false};
};

std::optional<std::chrono::steady_clock::time_point>
effective_deadline(const RequestOptions &options) {
  auto result = options.deadline;
  if (options.total_timeout) {
    const auto relative = std::chrono::steady_clock::now() +
                          *options.total_timeout;
    if (!result || relative < *result) result = relative;
  }
  return result;
}

Result<std::chrono::milliseconds> limited_timeout(
    std::chrono::milliseconds timeout,
    const std::optional<std::chrono::steady_clock::time_point> &deadline) {
  if (!deadline) return std::max(timeout, 1ms);
  const auto now = std::chrono::steady_clock::now();
  if (*deadline <= now)
    return ErrorInfo{Error::timeout, "Request deadline exceeded"};
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      *deadline - now);
  return std::max(std::min(timeout, remaining), 1ms);
}

} // namespace

class AsyncClient::Impl {
public:
  struct Lease {
    std::shared_ptr<detail::Connection> connection;
    std::string buffer;
    std::string key;
    bool reused{false};
  };

  Impl(std::string url, ClientOptions client_options)
      : runtime(std::make_shared<detail::Runtime>()), base_url(std::move(url)),
        options(std::move(client_options)) {
    base = detail::parse_url(base_url);
  }

  ~Impl() {
    std::vector<std::shared_ptr<detail::Connection>> connections;
    {
      std::lock_guard lock(mutex);
      for (auto &[_, entries] : pool)
        for (auto &entry : entries) connections.push_back(entry.connection);
      for (auto &connection : active)
        if (connection) connections.push_back(connection);
      pool.clear();
      active.clear();
    }
    for (auto &connection : connections) connection->close();
#ifdef CHHTTP_HAS_TLS
    if (tls_context) SSL_CTX_free(tls_context);
#endif
    detail::stop_runtime(std::move(runtime));
  }

  Task<Result<Lease>> acquire(
      const detail::ParsedUrl &url,
      const std::shared_ptr<RequestControl> &control,
      const RequestOptions &request_options) {
    co_await detail::resume_on(runtime);
    const std::string key = url.origin() + "|" + options.proxy.url;
    const auto cancelled = [&] {
      if (request_options.cancellation && *request_options.cancellation)
        control->cancel();
      return control->cancelled();
    };
    for (;;) {
      {
        std::lock_guard lock(mutex);
        auto &entries = pool[key];
        while (!entries.empty()) {
          auto lease = std::move(entries.back());
          entries.pop_back();
          if (lease.connection && lease.connection->open()) {
            lease.reused = true;
            active.push_back(lease.connection);
            control->bind(lease.connection);
            co_return lease;
          }
          auto &count = connection_counts[key];
          if (count != 0) --count;
        }
        auto &count = connection_counts[key];
        if (options.max_connections_per_origin == 0 ||
            count < options.max_connections_per_origin) {
          ++count;
          break;
        }
      }
      if (cancelled())
        co_return ErrorInfo{Error::cancelled, "Request cancelled while queued"};
      if (request_options.deadline &&
          std::chrono::steady_clock::now() >= *request_options.deadline)
        co_return ErrorInfo{Error::timeout, "Request deadline exceeded in connection queue"};
      co_await sleep_for(1ms);
    }

    const auto release_slot = [&] {
      std::lock_guard lock(mutex);
      auto &count = connection_counts[key];
      if (count != 0) --count;
    };
    const auto fail = [&](ErrorInfo error) -> Result<Lease> {
      release_slot();
      return error;
    };

    detail::ParsedUrl endpoint = url;
    std::optional<detail::ParsedUrl> proxy;
    if (!options.proxy.url.empty()) {
      auto parsed_proxy = detail::parse_url(options.proxy.url);
      if (!parsed_proxy) co_return fail(parsed_proxy.error());
      if (parsed_proxy->secure)
        co_return fail(
            ErrorInfo{Error::proxy, "TLS proxies are not supported"});
      proxy = *parsed_proxy;
      endpoint = *parsed_proxy;
    }
    auto connect_timeout = limited_timeout(
        request_options.connect_timeout.value_or(options.connect_timeout),
        request_options.deadline);
    if (!connect_timeout) co_return fail(connect_timeout.error());
    auto connected = co_await detail::Connection::connect(
        runtime, endpoint.host, endpoint.port, *connect_timeout,
        [control](const auto &connection) { control->bind(connection); });
    if (!connected) co_return fail(connected.error());
    auto connection = *connected;
    const auto fail_connection = [&](ErrorInfo error) -> Result<Lease> {
      control->unbind(connection);
      connection->close();
      release_slot();
      return error;
    };
    if (cancelled())
      co_return fail_connection(
          ErrorInfo{Error::cancelled, "Request cancelled"});
    auto socket_option_error = connection->set_no_delay(options.tcp_no_delay);
    if (socket_option_error) {
      co_return fail_connection(std::move(socket_option_error));
    }

    if (proxy && url.secure) {
      Request tunnel;
      tunnel.method = "CONNECT";
      tunnel.target = url.authority();
      tunnel.headers.set("Host", url.authority());
      if (!options.proxy.username.empty())
        tunnel.headers.set("Proxy-Authorization",
                           basic_auth(options.proxy.username,
                                      options.proxy.password));
      auto write_timeout = limited_timeout(
          request_options.write_timeout.value_or(options.write_timeout),
          request_options.deadline);
      if (!write_timeout) co_return fail_connection(write_timeout.error());
      auto write_error = co_await detail::write_request(
          connection, tunnel, tunnel.target, *write_timeout);
      if (write_error) {
        co_return fail_connection(std::move(write_error));
      }
      std::string tunnel_buffer;
      auto response = co_await detail::read_response(
          connection, tunnel_buffer, "CONNECT",
          {.max_body_size = 1024 * 1024,
           .read_timeout = request_options.read_timeout.value_or(
               options.read_timeout),
           .header_timeout = request_options.header_timeout,
           .deadline = request_options.deadline,
           .cancelled = cancelled});
      if (!response || response->status != 200) {
        co_return fail_connection(
            response ? ErrorInfo{Error::proxy,
                                 "HTTPS CONNECT proxy rejected tunnel"}
                     : response.error());
      }
    }

    if (url.secure) {
#ifdef CHHTTP_HAS_TLS
      ErrorInfo context_error;
      if (!tls_context)
        tls_context = detail::create_client_tls_context(options.tls, context_error);
      if (!tls_context) {
        co_return fail_connection(std::move(context_error));
      }
      const std::string server_name = options.tls.server_name.empty()
                                          ? url.host
                                          : options.tls.server_name;
      auto tls_error = connection->enable_tls(tls_context, false, server_name);
      auto handshake_timeout = limited_timeout(
          request_options.connect_timeout.value_or(options.connect_timeout),
          request_options.deadline);
      if (!handshake_timeout)
        co_return fail_connection(handshake_timeout.error());
      if (!tls_error)
        tls_error = co_await connection->handshake(*handshake_timeout);
      if (tls_error) {
        co_return fail_connection(std::move(tls_error));
      }
#else
      co_return fail_connection(
          ErrorInfo{Error::tls_unavailable, "TLS support is disabled"});
#endif
    }
    {
      std::lock_guard lock(mutex);
      active.push_back(connection);
    }
    co_return Lease{connection, {}, key, false};
  }

  void remove_active(const std::shared_ptr<detail::Connection> &connection) {
    std::lock_guard lock(mutex);
    std::erase(active, connection);
  }

  void discard(Lease lease) {
    remove_active(lease.connection);
    lease.connection->close();
    std::lock_guard lock(mutex);
    auto &count = connection_counts[lease.key];
    if (count != 0) --count;
  }

  void release(Lease lease, bool reusable) {
    remove_active(lease.connection);
    if (!reusable || !options.keep_alive || !lease.connection->open()) {
      lease.connection->close();
      std::lock_guard lock(mutex);
      auto &count = connection_counts[lease.key];
      if (count != 0) --count;
      return;
    }
    std::lock_guard lock(mutex);
    auto &entries = pool[lease.key];
    if (entries.size() >= options.connection_pool_size) {
      lease.connection->close();
      auto &count = connection_counts[lease.key];
      if (count != 0) --count;
      return;
    }
    lease.reused = false;
    entries.push_back(std::move(lease));
  }

  Task<ResponseResult> exchange(detail::ParsedUrl url, Request request,
                                RequestOptions request_options,
                                const std::shared_ptr<RequestControl> &control,
                                std::size_t redirects, bool digest_attempted,
                                bool allow_automatic_auth) {
    const auto cancelled = [&] {
      if (request_options.cancellation && *request_options.cancellation)
        control->cancel();
      return control->cancelled();
    };
    if (cancelled())
      co_return ErrorInfo{Error::cancelled, "Request cancelled"};
    if (request_options.deadline &&
        std::chrono::steady_clock::now() >= *request_options.deadline)
      co_return ErrorInfo{Error::timeout, "Request deadline exceeded"};

    for (const auto &[name, value] : options.default_headers)
      if (!request.headers.contains(name)) request.headers.add(name, value);
    request.headers.set("Host", url.authority());
    request.keep_alive = options.keep_alive;
    const bool auto_decompress = request_options.auto_decompress.value_or(
        options.auto_decompress);
#ifdef CHHTTP_HAS_COMPRESSION
    if (auto_decompress && !request.headers.contains("Accept-Encoding"))
      request.headers.set("Accept-Encoding", "gzip, deflate, br, zstd");
#endif
    if (allow_automatic_auth && !request.headers.contains("Authorization")) {
      switch (options.authentication.type) {
      case AuthenticationType::basic:
        request.headers.set("Authorization",
                            basic_auth(options.authentication.username,
                                       options.authentication.password));
        break;
      case AuthenticationType::bearer:
        request.headers.set("Authorization",
                            bearer_auth(options.authentication.token));
        break;
      default: break;
      }
    }
    if (!options.proxy.url.empty() && !url.secure &&
        !options.proxy.username.empty())
      request.headers.set("Proxy-Authorization",
                          basic_auth(options.proxy.username,
                                     options.proxy.password));

    const Request original_request = request;
    for (int attempt = 0; attempt != 2; ++attempt) {
      auto lease_result = co_await acquire(url, control, request_options);
      if (!lease_result) co_return lease_result.error();
      auto lease = std::move(*lease_result);
      if (cancelled()) {
        control->unbind(lease.connection);
        discard(std::move(lease));
        co_return ErrorInfo{Error::cancelled, "Request cancelled"};
      }
      const std::string wire_target =
          !options.proxy.url.empty() && !url.secure
              ? url.origin() + url.target
              : url.target;
      auto write_timeout = limited_timeout(
          request_options.write_timeout.value_or(options.write_timeout),
          request_options.deadline);
      if (!write_timeout) {
        control->unbind(lease.connection);
        discard(std::move(lease));
        co_return write_timeout.error();
      }
      auto write_error = co_await detail::write_request(
          lease.connection, request, wire_target, *write_timeout);
      if (write_error) {
        const bool retry = lease.reused && attempt == 0 &&
                           idempotent_method(request.method) &&
                           !request.body_stream;
        control->unbind(lease.connection);
        discard(std::move(lease));
        if (cancelled())
          co_return ErrorInfo{Error::cancelled, "Request cancelled"};
        if (retry) continue;
        co_return write_error;
      }

      bool response_started = false;
      detail::HttpReadOptions read_options{
          .max_header_size = 64 * 1024,
          .max_body_size = request_options.max_response_body_size.value_or(
              options.max_response_body_size),
          .read_timeout = request_options.read_timeout.value_or(
              options.read_timeout),
          .header_timeout = request_options.header_timeout,
          .first_body_byte_timeout = request_options.first_body_byte_timeout,
          .idle_timeout = request_options.idle_timeout,
          .deadline = request_options.deadline,
          .auto_decompress = auto_decompress,
          .cancelled = cancelled,
          .on_response_head = [&](const ResponseHead &head) {
            response_started = true;
            return !request_options.on_response_head ||
                   request_options.on_response_head(head);
          },
          .on_data = request_options.on_data,
          .on_data_async = request_options.on_data_async,
          .on_progress = request_options.on_progress};
      auto response = co_await detail::read_response(
          lease.connection, lease.buffer, request.method, read_options);
      if (!response) {
        const bool retry = !response_started && lease.reused && attempt == 0 &&
                           idempotent_method(request.method) &&
                           !request.body_stream;
        control->unbind(lease.connection);
        discard(std::move(lease));
        if (cancelled())
          co_return ErrorInfo{Error::cancelled, "Request cancelled"};
        if (retry) continue;
        co_return response.error();
      }
      if (cancelled()) {
        control->unbind(lease.connection);
        discard(std::move(lease));
        co_return ErrorInfo{Error::cancelled, "Request cancelled"};
      }

      const bool reusable = request.keep_alive && response->keep_alive;
      control->unbind(lease.connection);
      release(std::move(lease), reusable);

      if (response->status == 401 && !digest_attempted &&
          allow_automatic_auth &&
          options.authentication.type == AuthenticationType::digest) {
        auto challenge = parse_digest_challenge(
            response->headers.get("WWW-Authenticate"));
        if (challenge) {
          auto authorization = digest_auth(
              request.method, url.target, options.authentication.username,
              options.authentication.password, *challenge);
          if (!authorization) co_return authorization.error();
          request = original_request;
          request.headers.set("Authorization", std::move(*authorization));
          co_return co_await exchange(url, std::move(request),
                                      std::move(request_options), control,
                                      redirects, true, allow_automatic_auth);
        }
      }

      if (options.follow_redirects && redirect_status(response->status) &&
          response->headers.contains("Location")) {
        if (redirects >= options.max_redirects)
          co_return ErrorInfo{Error::redirect_limit,
                              "HTTP redirect limit exceeded"};
        auto resolved = detail::resolve_url(url.origin() + url.target,
                                            response->headers.get("Location"));
        if (!resolved) co_return resolved.error();
        auto next_url = detail::parse_url(*resolved);
        if (!next_url) co_return next_url.error();
        const bool same_origin = next_url->origin() == url.origin();
        if (!same_origin) request.headers.erase("Authorization");
        if (response->status == 303 ||
            ((response->status == 301 || response->status == 302) &&
             detail::iequals(request.method, "POST"))) {
          request.method = "GET";
          request.body.clear();
          request.body_stream = {};
          request.body_stream_length.reset();
          request.headers.erase("Content-Type");
        }
        request.target = next_url->target;
        co_return co_await exchange(*next_url, std::move(request),
                                    std::move(request_options), control,
                                    redirects + 1, false,
                                    same_origin && allow_automatic_auth);
      }
      co_return response;
    }
    co_return ErrorInfo{Error::connect, "Unable to use pooled connection"};
  }

  Task<ResponseResult> request(Request request,
                               RequestOptions request_options) {
    if (!base) co_return base.error();
    if (request_options.on_data && request_options.on_data_async)
      co_return ErrorInfo{Error::invalid_argument,
                          "Configure either on_data or on_data_async"};
    if (request.body_stream && !request.body.empty())
      co_return ErrorInfo{Error::invalid_argument,
                          "Configure either a buffered or streamed request body"};
    if (!request.body_stream && request.body_stream_length)
      co_return ErrorInfo{Error::invalid_argument,
                          "A streamed body length requires a body producer"};
    request_options.deadline = effective_deadline(request_options);
    auto control = std::make_shared<RequestControl>();
    {
      std::lock_guard lock(mutex);
      std::erase_if(controls, [](const auto &item) { return item.expired(); });
      controls.push_back(control);
    }
    std::stop_callback stop_callback(
        request_options.stop_token, [control] { control->cancel(); });
    if (request_options.cancellation && *request_options.cancellation)
      control->cancel();
    auto url = detail::parse_url(request.target, base->origin() + base->target);
    if (!url) co_return url.error();
    request.target = url->target;
    co_return co_await exchange(*url, std::move(request),
                                std::move(request_options), control, 0, false,
                                true);
  }

  void cancel() {
    std::vector<std::shared_ptr<detail::Connection>> connections;
    std::vector<std::shared_ptr<RequestControl>> request_controls;
    {
      std::lock_guard lock(mutex);
      connections = active;
      for (const auto &item : controls)
        if (auto control = item.lock()) request_controls.push_back(control);
    }
    for (auto &control : request_controls) control->cancel();
    for (auto &connection : connections)
      if (connection) connection->close();
  }

  std::shared_ptr<detail::Runtime> runtime;
  std::string base_url;
  ClientOptions options;
  Result<detail::ParsedUrl> base;
  std::mutex mutex;
  std::unordered_map<std::string, std::vector<Lease>> pool;
  std::unordered_map<std::string, std::size_t> connection_counts;
  std::vector<std::shared_ptr<detail::Connection>> active;
  std::vector<std::weak_ptr<RequestControl>> controls;
#ifdef CHHTTP_HAS_TLS
  SSL_CTX *tls_context{nullptr};
#endif
};

AsyncClient::AsyncClient(std::string base_url, ClientOptions options)
    : impl_(std::make_shared<Impl>(std::move(base_url), std::move(options))) {}
AsyncClient::~AsyncClient() {
  if (impl_) impl_->cancel();
}
AsyncClient::AsyncClient(AsyncClient &&) noexcept = default;
AsyncClient &AsyncClient::operator=(AsyncClient &&other) noexcept {
  if (this != &other) {
    if (impl_) impl_->cancel();
    impl_ = std::move(other.impl_);
  }
  return *this;
}

Task<ResponseResult> AsyncClient::request(Request request,
                                          RequestOptions options) {
  if (!impl_) co_return ErrorInfo{Error::internal, "Empty async client"};
  auto impl = impl_;
  co_return co_await impl->request(std::move(request), std::move(options));
}

Task<ResponseResult> AsyncClient::get(std::string target, Headers headers,
                                      RequestOptions options) {
  Request request;
  request.method = "GET";
  request.target = std::move(target);
  request.headers = std::move(headers);
  if (!impl_) co_return ErrorInfo{Error::internal, "Empty async client"};
  auto impl = impl_;
  co_return co_await impl->request(std::move(request), std::move(options));
}

Task<ResponseResult> AsyncClient::head(std::string target, Headers headers,
                                      RequestOptions options) {
  Request request;
  request.method = "HEAD";
  request.target = std::move(target);
  request.headers = std::move(headers);
  if (!impl_) co_return ErrorInfo{Error::internal, "Empty async client"};
  auto impl = impl_;
  co_return co_await impl->request(std::move(request), std::move(options));
}

Task<ResponseResult> AsyncClient::post(std::string target, std::string body,
                                       std::string content_type,
                                       Headers headers,
                                       RequestOptions options) {
  Request request;
  request.method = "POST";
  request.target = std::move(target);
  request.body = std::move(body);
  request.headers = std::move(headers);
  request.headers.set("Content-Type", std::move(content_type));
  if (!impl_) co_return ErrorInfo{Error::internal, "Empty async client"};
  auto impl = impl_;
  co_return co_await impl->request(std::move(request), std::move(options));
}

Task<ResponseResult> AsyncClient::put(std::string target, std::string body,
                                      std::string content_type,
                                      Headers headers,
                                      RequestOptions options) {
  Request request;
  request.method = "PUT";
  request.target = std::move(target);
  request.body = std::move(body);
  request.headers = std::move(headers);
  request.headers.set("Content-Type", std::move(content_type));
  if (!impl_) co_return ErrorInfo{Error::internal, "Empty async client"};
  auto impl = impl_;
  co_return co_await impl->request(std::move(request), std::move(options));
}

Task<ResponseResult> AsyncClient::patch(std::string target, std::string body,
                                        std::string content_type,
                                        Headers headers,
                                        RequestOptions options) {
  Request request;
  request.method = "PATCH";
  request.target = std::move(target);
  request.body = std::move(body);
  request.headers = std::move(headers);
  request.headers.set("Content-Type", std::move(content_type));
  if (!impl_) co_return ErrorInfo{Error::internal, "Empty async client"};
  auto impl = impl_;
  co_return co_await impl->request(std::move(request), std::move(options));
}

Task<ResponseResult> AsyncClient::del(std::string target, Headers headers,
                                      RequestOptions options) {
  Request request;
  request.method = "DELETE";
  request.target = std::move(target);
  request.headers = std::move(headers);
  if (!impl_) co_return ErrorInfo{Error::internal, "Empty async client"};
  auto impl = impl_;
  co_return co_await impl->request(std::move(request), std::move(options));
}

void AsyncClient::cancel() {
  if (impl_) impl_->cancel();
}

class Client::Impl {
public:
  Impl(std::string base_url, ClientOptions options)
      : client(std::move(base_url), std::move(options)) {}
  AsyncClient client;
};

Client::Client(std::string base_url, ClientOptions options)
    : impl_(std::make_unique<Impl>(std::move(base_url), std::move(options))) {}
Client::~Client() = default;
Client::Client(Client &&) noexcept = default;
Client &Client::operator=(Client &&) noexcept = default;

ResponseResult Client::request(Request request, RequestOptions options) {
  return impl_->client.request(std::move(request), std::move(options)).get();
}
ResponseResult Client::get(std::string target, Headers headers,
                           RequestOptions options) {
  return impl_->client.get(std::move(target), std::move(headers),
                           std::move(options)).get();
}
ResponseResult Client::head(std::string target, Headers headers,
                            RequestOptions options) {
  return impl_->client
      .head(std::move(target), std::move(headers), std::move(options))
      .get();
}
ResponseResult Client::post(std::string target, std::string body,
                            std::string content_type, Headers headers,
                            RequestOptions options) {
  return impl_->client
      .post(std::move(target), std::move(body), std::move(content_type),
            std::move(headers), std::move(options))
      .get();
}
ResponseResult Client::put(std::string target, std::string body,
                           std::string content_type, Headers headers,
                           RequestOptions options) {
  return impl_->client
      .put(std::move(target), std::move(body), std::move(content_type),
           std::move(headers), std::move(options))
      .get();
}
ResponseResult Client::patch(std::string target, std::string body,
                             std::string content_type, Headers headers,
                             RequestOptions options) {
  return impl_->client
      .patch(std::move(target), std::move(body), std::move(content_type),
             std::move(headers), std::move(options))
      .get();
}
ResponseResult Client::del(std::string target, Headers headers,
                           RequestOptions options) {
  return impl_->client.del(std::move(target), std::move(headers),
                           std::move(options)).get();
}
void Client::cancel() { impl_->client.cancel(); }

Task<Result<std::shared_ptr<WebSocket>>>
AsyncWebSocketClient::connect(std::string url, Headers headers,
                              ClientOptions options) {
  auto runtime = std::make_shared<detail::Runtime>();
  auto result = co_await detail::websocket_client_connect(
      runtime, std::move(url), std::move(headers), std::move(options));
  if (!result) detail::stop_runtime(std::move(runtime));
  // A successful connection retains the runtime in its WebSocket channel.
  co_return result;
}

} // namespace chhttp
