# LuaJIT host-capability parity audit

**Status: incomplete.** This audit extends the prior matrix audit at `98760d713` with the shared `RequestScope` implementation (`7c7a49353`) and opt-in inbound request-body streaming (`cbb9da4b2`) on `leostera/luajit`. The source-of-truth checklist remains [feature-matrix.md](feature-matrix.md); this report summarizes verified coverage and does not upgrade partial or blocked rows into parity claims.

## Coverage review

The registered JSG inventory in `src/workerd/api/rtti.c++` contains 45 groups. A fresh source-to-matrix check found 45 matching rows, with no missing, extra, or duplicate families. The matrix separately covers worker configuration/bindings, service linking, `WorkerInterface` lifecycle, operational behavior, security posture, and runtime limits, using `workerd.capnp`, `Server::makeWorker()`, and the I/O interfaces as sources.

Verified Lua capabilities are deliberately narrow:

- The experimental HTTP worker keeps the `{ fetch = function(request) ... end }` module contract. Inbound bodies remain buffered by default; `luaRequestBodyStreaming = true` opts into request-local chunk reads (64 KiB per read, 1 MiB total) without changing the default `request.body` string. String-valued `workerd.fetch()` request bodies and handler response bodies remain buffered to 1 MiB; in streaming mode, the current inbound body handle can be passed through to `workerd.fetch()` with backpressure and the same total read limit. `workerd.sleep()` and host operations yield through per-request Lua coroutines.
- `workerd.fetch()` uses only the linked `globalOutbound` route. Outbound response bodies are request-scoped streams read in bounded chunks with `workerd.readBody()`; `readAll()` and explicit/automatic cancellation are covered. Requests interleave at host yield points. Lua `LuaIoContext` and JavaScript `IoContext::IncomingRequest` both compose the runtime-neutral `RequestScope`, which applies per-invocation subrequest admission and observer wrapping. Lua tests verify rejection before route dispatch and successful observer wrapping. The standalone server injects an unrestricted policy and the base no-op observer, matching standalone workerd's current policy/observer behavior; this does not establish Cloudflare production provider wiring or production quotas/metrics.
- Text, binary-data, and JSON configuration bindings are installed before module evaluation under `workerd.bindings`. JSON `null` uses `workerd.null`; values share the mutable service-lifetime Lua state.
- Runtime failures retain Lua source location and message; the server logs uncaught handler errors and returns a generic 500. Outbound fetch propagates internal and user trace parents through a `lua.fetch` child span.
- Configured unsupported bindings, Durable Object storage, scheduled/alarm interface calls, WebSocket upgrades, and CONNECT are rejected or fail explicitly as documented.

Lua remains experimental and trusted-code-only. Standard Lua libraries include OS and native facilities; there is no Lua CPU/memory quota, sandbox, or V8 inspector. The audit makes no claim that Lua is safe for untrusted tenant code.

## Material gaps

1. **Lua-produced outbound body streams:** `workerd.fetch()` can pass through the current inbound `request.bodyStream` with backpressure, but Lua cannot produce arbitrary body chunks. String uploads and handler response bodies remain buffered up to 1 MiB.
2. **Provider-specific policy and metrics:** The shared `RequestScope` invokes an injected subrequest policy before dispatch and wraps clients with the invocation observer; tests prove both hooks. Standalone workerd supplies an unrestricted policy and base no-op observer, consistent with standalone JS behavior. The public source does not establish that Cloudflare production providers inject equivalent policy/metrics for Lua, so production parity must not be claimed without that verification and user acceptance of any exclusion.
3. **Events and actors:** Scheduled handlers need runtime-neutral lifecycle, retry, cancellation, trace/metrics, and `waitUntil` ownership. Alarms and Durable Objects need actor identity, persistent state, per-actor concurrency, eviction, and retry semantics. Those models are absent; adding callbacks over service-global Lua state would not reproduce actor lifecycle behavior.
4. **Other operational APIs:** There is no Lua structured logging or public request metrics/tracing API, and no CPU/memory enforcement. These are documented as gaps rather than inferred from error logging, trace-parent propagation, observer hooks, or coroutine support.

The `http-network` task's contract is met for the selected experimental HTTP surface: outbound response streaming/backpressure, opt-in inbound request streaming and backpressured outbound pass-through, disconnect cancellation, error handling, configured routing, injected subrequest admission/observer hooks, and interleaving are covered by tests and documented. Lua-generated outbound body sources remain an explicit exclusion, and no Cloudflare production provider wiring is claimed. Do not mark the overall parity goal complete until remaining material exclusions are verified or accepted by the user.

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

Runtime/server tests cover value binding conversion and validation, cancellation and subsequent request reuse, source-aware Lua errors, trace-parent propagation, logged handler failures/500 responses, response streaming while upstream remains open, opt-in inbound body chunking and backpressure, binary chunk preservation, partial-read errors, body-size enforcement, EOF, request-handle isolation, chunked outbound pass-through while input remains open, upload cancellation on client disconnect, configured outbound routing, injected subrequest rejection before dispatch, observer wrapping, unsupported event/config paths, and request interleaving. They do not cover Lua-generated outbound body sources or verify Cloudflare production policy/observer wiring.
