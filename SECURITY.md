# Security policy

Please report vulnerabilities privately to the project maintainers before
publishing details.

Production deployments should keep TLS peer verification enabled, configure
explicit request/response size limits, avoid doing unbounded CPU or filesystem
work on I/O threads, validate application-level authorization before streaming
data, and place private-key material outside the source tree.

The library rejects malformed URL escapes and multipart boundaries, caps Beast
HTTP parsers, bounds decompression output, strips `Authorization` across
redirect origins, and canonicalizes static-file paths to prevent traversal.

