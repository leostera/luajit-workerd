# LuaJIT host-capability parity audit

**Status: incomplete.** This audit extends the prior matrix audit at `98760d713` with the shared `RequestScope` implementation (`7c7a49353`), opt-in inbound request-body streaming (`cbb9da4b2`), and backpressured outbound pass-through and synchronous Lua upload producers on `leostera/luajit`. The source-of-truth checklist remains [feature-matrix.md](feature-matrix.md); this report summarizes verified coverage and does not upgrade partial or blocked rows into parity claims.

## Coverage review

The registered JSG inventory in `src/workerd/api/rtti.c++` contains 45 groups. A fresh source-to-matrix check found 45 matching rows, with no missing, extra, or duplicate families. The matrix separately covers worker configuration/bindings, service linking, `WorkerInterface` lifecycle, operational behavior, security posture, and runtime limits, using `workerd.capnp`, `Server::makeWorker()`, and the I/O interfaces as sources.

Verified Lua capabilities are deliberately narrow:

- The experimental HTTP worker keeps the `{ fetch = function(request) ... end }` module contract. Inbound bodies remain buffered by default; `luaRequestBodyStreaming = true` opts into request-local chunk reads (64 KiB per read, 1 MiB total) without changing the default `request.body` string. String-valued `workerd.fetch()` request bodies and handler response bodies remain buffered to 1 MiB; streamed inbound bodies can be passed through with backpressure, and Lua producer functions can supply synchronous binary-safe chunks up to 64 KiB each, with a 1 MiB upload limit. `workerd.sleep()` and host operations yield through per-request Lua coroutines.
- `workerd.fetch()` uses only the linked `globalOutbound` route. Outbound response bodies are request-scoped streams read in bounded chunks with `workerd.readBody()`; `readAll()` and explicit/automatic cancellation are covered. Requests interleave at host yield points. Lua `LuaIoContext` and JavaScript `IoContext::IncomingRequest` both compose the runtime-neutral `RequestScope`, which applies per-invocation subrequest admission and observer wrapping. Lua tests verify rejection before route dispatch and successful observer wrapping. The standalone server injects an unrestricted policy and the base no-op observer, matching standalone workerd's current policy/observer behavior; this does not establish Cloudflare production provider wiring or production quotas/metrics.
- `workerd.parseUrl(url[, base])` is a small Lua-native structured parser backed by workerd's KJ URL parser; it supports absolute remote URLs and relative resolution, returns plain component/query tables, and reports invalid input as `nil, error`. It is not a mutable WHATWG URL class.
- `workerd.randomBytes(size)` and `workerd.randomUUID()` delegate to the request's injected KJ entropy source, shared with workerd's standard request context. Byte output is capped at 64 KiB; UUIDs use workerd's version-4 generator. This does not imply support for WebCrypto, key management, or other cryptographic operations.
- Text, binary-data, and JSON configuration bindings are installed before module evaluation under `workerd.bindings`. JSON `null` uses `workerd.null`; values share the mutable service-lifetime Lua state.
- Runtime failures retain Lua source location and message; the server logs uncaught handler errors and returns a generic 500. Outbound fetch propagates internal and user trace parents through a `lua.fetch` child span.
- Configured unsupported bindings, Durable Object storage, scheduled/alarm interface calls, WebSocket upgrades, and CONNECT are rejected or fail explicitly as documented.

Lua remains experimental and trusted-code-only. Standard Lua libraries include OS and native facilities; there is no Lua CPU/memory quota, sandbox, or V8 inspector. The audit makes no claim that Lua is safe for untrusted tenant code.

## Material gaps

1. **Yieldable outbound body producers:** `workerd.fetch()` can pass through `request.bodyStream` and pull synchronous Lua producer functions with backpressure. Producer functions cannot yield for asynchronous I/O; string uploads and handler response bodies remain buffered up to 1 MiB.
2. **Provider-specific policy and metrics:** The shared `RequestScope` invokes an injected subrequest policy before dispatch and wraps clients with the invocation observer; tests prove both hooks. Standalone workerd supplies an unrestricted policy and base no-op observer, consistent with standalone JS behavior. The public source does not establish that Cloudflare production providers inject equivalent policy/metrics for Lua, so production parity must not be claimed without that verification and user acceptance of any exclusion.
3. **Events and actors:** Scheduled handlers need runtime-neutral lifecycle, retry, cancellation, trace/metrics, and `waitUntil` ownership. Alarms and Durable Objects need actor identity, persistent state, per-actor concurrency, eviction, and retry semantics. Those models are absent; adding callbacks over service-global Lua state would not reproduce actor lifecycle behavior.
4. **Other operational APIs:** There is no Lua structured logging or public request metrics/tracing API, and no CPU/memory enforcement. These are documented as gaps rather than inferred from error logging, trace-parent propagation, observer hooks, or coroutine support.
5. **Other cryptographic operations:** Lua exposes bounded secure random bytes and UUID generation only; digest, signature, encryption, key import/usage, and `CryptoKey` binding behavior remain unsupported.

The `http-network` task's contract is met for the selected experimental HTTP surface: outbound response streaming/backpressure, opt-in inbound request streaming, backpressured pass-through and synchronous producer uploads, disconnect cancellation, error handling, configured routing, injected subrequest admission/observer hooks, and interleaving are covered by tests and documented. Yieldable upload producers remain an explicit exclusion, and no Cloudflare production provider wiring is claimed. Do not mark the overall parity goal complete until remaining material exclusions are verified or accepted by the user.

## Verification evidence

The latest focused runtime/server and I/O regression tests passed uncached:

```text
bazel test --nocache_test_results //src/workerd/rt/luajit:lua-runtime-test@ //src/workerd/server:server-test@ //src/workerd/io:io-context-test@ //src/workerd/io:worker-entrypoint-test@
```

The relevant server binary also built successfully:

```text
bazel build //src/workerd/server:workerd
```

Prettier, clang-format, Buildifier, and `git diff --check` passed for the modified file set. The 45-family inventory check reported no missing, extra, or duplicate rows.

Runtime/server tests cover value binding conversion and validation, absolute and relative URL parsing and invalid-input behavior, bounded random-byte output and invalid-size handling, version-4 UUID formatting, server entropy-source delegation, cancellation and subsequent request reuse, source-aware Lua errors, trace-parent propagation, logged handler failures/500 responses, response streaming while upstream remains open, opt-in inbound body chunking and backpressure, binary chunk preservation, partial-read errors, body-size enforcement, EOF, request-handle isolation, demand-driven binary producer chunks, per-chunk and total producer upload limits, producer error mapping, chunked pass-through while input remains open, synchronous producer uploads, upload cancellation on client disconnect, configured outbound routing, injected subrequest rejection before dispatch, observer wrapping, unsupported event/config paths, and request interleaving. They do not cover yieldable upload producers or verify Cloudflare production policy/observer wiring.
