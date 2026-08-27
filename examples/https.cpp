#include <chhttp/chhttp.hpp>

#include <iostream>

int main(int argc, char **argv) {
#ifndef CHHTTP_HAS_TLS
  (void)argc;
  (void)argv;
  std::cerr << "This build of chhttp has TLS disabled.\n";
  return 1;
#else
  if (argc < 3) {
    std::cerr << "usage: chhttp_https <server-cert.pem> <server-key.pem> "
                 "[trusted-ca.pem]\n";
    return 1;
  }

  // The server loads a PEM chain and matching private key during start().
  chhttp::ServerOptions server_options;
  server_options.tls = chhttp::TlsServerOptions{
      .certificate_file = argv[1], .private_key_file = argv[2]};
  chhttp::Server server(std::move(server_options));
  server.get("/secure", [](const chhttp::Request &,
                            chhttp::Response &response) {
    response.set_content("encrypted response");
  });
  if (!server.start("127.0.0.1", 0)) {
    std::cerr << "TLS server configuration failed\n";
    return 2;
  }

  chhttp::ClientOptions client_options;
  std::string host;
  if (argc >= 4) {
    // Verification is the production setting: trust the supplied CA and check localhost.
    client_options.tls.use_system_certificates = false;
    client_options.tls.ca_file = argv[3];
    host = "localhost";
  } else {
    // Disabling verification is acceptable only for this loopback demonstration.
    client_options.tls.verify_peer = false;
    host = "127.0.0.1";
    std::cerr << "warning: peer verification disabled for local example\n";
  }
  // SNI and hostname/IP verification are derived from this URL unless overridden.
  chhttp::Client client("https://" + host + ":" +
                            std::to_string(server.port()),
                        std::move(client_options));
  auto response = client.get("/secure");
  // Stop after the request so the example also demonstrates orderly TLS teardown.
  server.stop();
  if (!response) {
    std::cerr << response.error().message << '\n';
    return 3;
  }
  std::cout << response->body << '\n';
#endif
}
