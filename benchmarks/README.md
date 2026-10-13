# Performance benchmarks

Build in Release mode with `CHHTTP_BUILD_BENCHMARKS=ON`. Keep compiler,
dependencies, TLS/compression options, LTO setting and optimization flags equal
between the two builds. Run benchmarks without concurrent builds or tests.

```sh
cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release \
  -DCHHTTP_BUILD_BENCHMARKS=ON -DCHHTTP_BUILD_EXAMPLES=OFF
cmake --build build-bench --parallel
build-bench/chhttp_benchmarks --help
build-bench/chhttp_benchmarks 3 encoding
python3 benchmarks/compare.py /path/to/baseline/chhttp_benchmarks \
  build-bench/chhttp_benchmarks --rounds 5 --scale 3 --output results.json
```

On Windows, use `python` and executable paths ending in `.exe`; for
multi-configuration generators, build with `--config Release` and use the
executables in `Release/`. Python is needed only for the comparison script.
Provide dependencies through existing CMake presets or `CMAKE_PREFIX_PATH`.

To compare against a revision predating a benchmark, copy the current
`benchmark.cpp` into that revision's `benchmarks/` directory before building it.
Both executables must exercise the same workloads. The runner warms each case,
alternates baseline/candidate execution order, retains every measurement and raw
output in JSON, and reports per-case medians. `--cases` limits the selection.
Failures and missing measurements stop the comparison.

| Selector | Workload |
|---|---|
| `parse` | HTTP request headers, including query decoding |
| `chunked` | Buffered 8-byte HTTP chunks with trailers |
| `encoding` | 64 KiB binary streaming Base64 encode and Base64 decode |
| `sse` | Batches of 256-byte SSE events |
| `websocket` | Buffered masked 256-byte binary WebSocket frames |
| `streaming` | 32-byte chunked response writes over loopback TCP |
| `keep-alive`, `1-conn`, `8-conn`, `64-conn` | TCP request round trips |
| `static`, `echo`, `routing` | Files, buffered bodies, route-table lookup |
| `memory-idle`, `memory-stream` | Live connection/stream memory (run separately) |

The buffered parser cases exclude socket waits. Network cases include both
client and server CPU in one process; wall time also includes kernel scheduling.
These are distinct measurements, and parser speedups do not imply equal
end-to-end HTTP speedups. The new workloads check decoded data, event counts,
trailers or frame contents as part of each timed run.

## Implementation changes (2026-10-13)

- TCP writes up to 16 KiB use libuv's portable `uv_try_write` on the owning loop. A completed
  write avoids a pending-write allocation, timer and completion callback. Partial
  writes retain their original storage and queue only the remaining bytes with
  the existing `uv_write` timeout and close handling. After 64 immediate writes,
  the next write uses the asynchronous path to let the loop service timers and
  other work. Larger blocks go directly to `uv_write`: probing large blocks first
  regressed buffered body transfers under socket pressure. The behavior follows
  the [libuv stream API](https://docs.libuv.org/en/v1.x/stream.html#c.uv_try_write).
- Buffered HTTP chunk-size lines are parsed from a borrowed view. Incomplete
  lines retain the incremental reader; limits, extensions and trailer validation
  apply to both paths. Views are fully consumed before buffer storage is released.
- Outbound HTTP chunks up to 64 KiB return the connection's owned write task
  directly, avoiding a wrapper coroutine and two deadline clock reads per small
  producer write. Frames reserve their actual hexadecimal length and framing
  bytes, allowing tiny frames to retain the string's inline storage. Larger
  producer writes still split into bounded blocks with one aggregate deadline;
  the existing transport retains backpressure, timeout, cancellation and loop
  fairness. Empty writes remain no-ops.
- Fully buffered request and response heads skip the read-helper coroutine too,
  with the same size-limit check shared by the fast and incremental paths.
- Head parsing extracts framing, Host, Connection and Expect controls during the
  field-validation pass, using field-name lengths to select comparisons. This
  removes repeated header-vector scans while retaining case-insensitive names,
  duplicate-field semantics, wire order and framing-conflict checks. Complete
  request-head parsing runs in a plain function so its temporary owning fields
  do not enlarge the incremental reader's coroutine frame.
- Buffered fixed-length bodies reserve up to 64 KiB when their first payload
  bytes arrive (or enough for that actual block if larger), capped by the
  declared length. This reduces repeated growth from short head-adjacent body
  fragments without allocating the advertised body size up front. Limits and
  cancellation are checked before allocation; callback-based streaming retains
  its existing delivery path.
- Buffered WebSocket frames skip read-helper coroutine allocations when the
  requested bytes are already available. Outbound small frames reserve their
  actual header and payload size together, including masking when required.
- Base64 encoders share a complete-triple loop that writes into a checked,
  pre-sized string. The decoder uses a 256-entry byte lookup table and a complete
  quartet path, retaining padding/invalid-input checks. No CPU-specific intrinsics
  or compiler architecture flags are used.
- Session completion updates the shutdown wait predicate under the same mutex
  used by `stop()`. This closes a lost-wakeup window that could make an already
  drained server wait for its full shutdown grace period. A repeated shutdown
  regression test races completion against the wait.

Public interfaces, handler execution policy, TLS/compression options and libuv's
Windows/Linux/macOS transport abstraction are preserved. The SSE scanner
experiment was reverted because it showed no clear benefit.

Installation checks also found that the installed CMake package required the
vcpkg-specific Brotli config even when the source build had used system libraries.
Source and installed builds now share Brotli discovery and stable imported target
names, including the common library for both encoder and decoder when available.

## Validation

All checks below ran on macOS arm64 with Apple Clang 17, C++20 and Homebrew
dependencies. Windows and Linux were not executed in this environment.

| Configuration | Functional | Stress | Property | Total |
|---|---:|---:|---:|---:|
| Release | 211 | 35 | 10 | 256 passed |
| ASan + UBSan | 211 | 35 | 10 | 256 passed |
| Release + LTO | 211 | 35 | 10 | 256 passed |
| TLS and compression both disabled | 197 | 33 | 9 | 239 passed |

The twelve new regression tests cover socket backpressure with exact byte ordering,
concurrent writes, timeout/close behavior, loop fairness, Base64 known vectors
and invalid octets, buffered header/chunk-line boundaries, shutdown wakeups,
mixed-case/duplicate controls in both field orders, response Host scope, and
fragmented binary bodies around 64 KiB and 512 KiB with progress, limits,
cancellation, streaming consumers and pipelined messages. Chunk-write tests also
cover hexadecimal size boundaries, empty/final frames, pending-frame ownership,
64 KiB splitting, concurrent writes, timeout and close handling. The fairness
test exercises both direct transport and framed writes, and TLS integrity tests
exercise both single-frame and segmented uploads. Existing
tests also cover fragmented HTTP/WebSocket/SSE traffic, TLS, compression,
cancellation and randomized binary inputs. Configured examples build in the LTO
and optional-features-disabled configurations.

An independent `tests/package` consumer was configured, linked and run against
the installed package using system Brotli libraries with pkg-config disabled.
The shared discovery module fixes the previously failing package configuration.

## Measurement environment

All executables use the same unchanged harness, Release (`-O3 -DNDEBUG`),
TLS/compression enabled and LTO disabled. The host is macOS 26.1, Apple M4 Max
(16 logical CPUs, arm64), Apple Clang 17. Other unrelated builds and benchmarks
were observed on this shared workstation; small differences in network tests
must be interpreted cautiously. No builds or tests for this task ran concurrently
with its benchmarks.

Three baselines distinguish retained gains from the remaining CPU regression:

- **Original:** clean revision `8c6d8cc`, rebuilt with this harness.
- **Delivered:** the saved build after the request-head and buffered-echo fixes,
  immediately before this small-chunk change (SHA-256 prefix `6048e3cd3f06`).
- **Pre-regression:** the earlier optimized build against which the remaining
  2.4% streaming user-CPU increase was reported (`1baed65a6bef`).

The runner warms each executable, alternates their order and reports medians.
Full suites use five rounds at scale 5; the targeted streaming comparisons use
six rounds at scale 10, balancing which executable runs first. CPU figures below
are **user CPU time per operation**, not total user-plus-kernel CPU. Throughput
includes scheduling and kernel waits. Absolute rates from separate comparisons
should not be compared directly.

## Small-chunk CPU regression verification

Each targeted run sends 1,310,720 checked 32-byte chunks through loopback TCP.
Both client and server CPU are included. The remaining regression is eliminated
against its actual pre-regression reference, as well as the delivered build.

| Reference build | Reference user µs/chunk | Candidate user µs/chunk | User CPU | Throughput |
|---|---:|---:|---:|---:|
| Delivered | 0.7205 | 0.6050 | -16.0% | +5.8% |
| Pre-regression | 0.7235 | 0.6195 | -14.4% | +0.6% |

The complete-suite comparison below independently measured -17.7% user CPU and
+5.6% throughput against the delivered build. The implementation removes one
wrapper coroutine and two deadline clock reads from each single-frame write;
it does not batch producer writes or delay their delivery. Transport ownership,
backpressure, fairness and the large-write aggregate deadline are preserved.

## Complete suite versus the original revision

| Case | Baseline ops/s | Candidate ops/s | Throughput | User CPU |
|---|---:|---:|---:|---:|
| parse request head (9 fields) | 1116388 | 1140166 | +2.1% | -1.9% |
| HTTP parse 8-byte chunks | 19130273 | 44802723 | +134.2% | -57.7% |
| stream Base64 encode 64 KiB | 2785 | 48056 | +1625.5% | -94.1% |
| Base64 decode 64 KiB | 3544 | 33119 | +834.5% | -89.3% |
| SSE parse 256-byte events | 7746765 | 7798838 | +0.7% | -0.8% |
| WebSocket parse masked 256-byte frames | 3121531 | 4422233 | +41.7% | -29.4% |
| stream 32-byte chunks over TCP | 353581 | 618170 | +74.8% | -24.1% |
| keep-alive requests (1 connection) | 27409 | 26835 | -2.1% | +1.4% |
| requests over 8 connections | 72704 | 71307 | -1.9% | -1.9% |
| requests over 64 connections | 96306 | 95463 | -0.9% | -1.8% |
| static file 1024 KiB | 2730 | 2756 | +1.0% | -2.0% |
| buffered echo 512 KiB | 5274 | 5630 | +6.8% | -7.6% |
| routing across 193 routes | 23770 | 25872 | +8.8% | -8.2% |

The request-head and buffered-echo fixes remain ahead of the original revision,
as do the large Base64, chunk-parser and WebSocket gains. Small network cases
include negative results as well as gains; the complete data is retained.

## Complete suite versus the delivered build

| Case | Baseline ops/s | Candidate ops/s | Throughput | User CPU |
|---|---:|---:|---:|---:|
| parse request head (9 fields) | 1152977 | 1165626 | +1.1% | -1.0% |
| HTTP parse 8-byte chunks | 45878163 | 45385795 | -1.1% | +0.0% |
| stream Base64 encode 64 KiB | 48423 | 48293 | -0.3% | +0.2% |
| Base64 decode 64 KiB | 33569 | 33830 | +0.8% | -0.7% |
| SSE parse 256-byte events | 7865907 | 7867771 | +0.0% | +0.0% |
| WebSocket parse masked 256-byte frames | 4585854 | 4544206 | -0.9% | +0.9% |
| stream 32-byte chunks over TCP | 589748 | 622766 | +5.6% | -17.7% |
| keep-alive requests (1 connection) | 28770 | 28290 | -1.7% | +1.6% |
| requests over 8 connections | 84951 | 84629 | -0.4% | -0.6% |
| requests over 64 connections | 96875 | 96388 | -0.5% | +1.1% |
| static file 1024 KiB | 2836 | 2876 | +1.4% | +0.0% |
| buffered echo 512 KiB | 5729 | 5751 | +0.4% | -1.3% |
| routing across 193 routes | 28056 | 28337 | +1.0% | -0.0% |

Other than the intended streaming gain, throughput changes ranged from -1.7%
to +1.4% in this comparison. Keep-alive requests measured -1.7% throughput and
+1.6% user CPU; these measurements are retained rather than claiming every metric
improved. The earlier major parser/encoding gains remain intact.

## Raw results and reproduction

Current raw measurements are saved locally under `build-bench/`:

- `performance-stream-cpu-original.json`: full suite versus the original revision.
- `performance-stream-cpu-delivered.json`: full suite versus the delivered build.
- `performance-stream-cpu-focused.json`: six-round streaming comparison versus
  the delivered build.
- `performance-stream-cpu-pre-regression.json`: six-round comparison versus the
  build preceding the 2.4% regression.
- `performance-stream-cpu-manifest.json`: executable paths, SHA-256 hashes,
  compiler/build configuration and comparison settings.
- `performance-stream-cpu-memory.json`: the memory snapshot below.

Earlier raw JSON files remain available; the preceding report is retained as
`build-bench/performance-before-stream-cpu.md`. Snapshot executables are needed
to reproduce comparisons with uncommitted intermediate builds. The manifest
records the saved paths on this machine. Substitute equivalent paths below:

```sh
python3 benchmarks/compare.py /path/to/original/chhttp_benchmarks \
  build-bench/chhttp_benchmarks --rounds 5 --scale 5 --output original.json
python3 benchmarks/compare.py /path/to/delivered/chhttp_benchmarks \
  build-bench/chhttp_benchmarks --rounds 5 --scale 5 --output delivered.json
python3 benchmarks/compare.py /path/to/pre-regression/chhttp_benchmarks \
  build-bench/chhttp_benchmarks --rounds 6 --scale 10 \
  --cases streaming --output pre-regression.json
```

A separate single-run memory comparison found unchanged idle live heap
(7.15 KiB per connection, including both ends in-process). During 16 concurrent
4 MiB uploads, live heap growth was 3.11 MiB in the delivered build versus
3.54 MiB in the candidate; RSS growth was 3.61 MiB versus 3.50 MiB. These
in-flight snapshots do not hold buffered byte counts constant and are not
evidence of a memory reduction.
