# LuaJIT host-capability parity audit

**Status: incomplete.** This audit was performed on `leostera/luajit` at `98760d713`; the final report/link and Buildifier normalization change no executable behavior. The source-of-truth checklist remains [feature-matrix.md](feature-matrix.md); this report summarizes its audit and does not upgrade partial or blocked rows into parity claims.

## Coverage review

The registered JSG inventory in `src/workerd/api/rtti.c++` contains 45 groups. A source-to-matrix check found 45 matching rows, with no missing, extra, duplicate, or unclassified families. The matrix separately covers worker configuration/bindings, service linking, `WorkerInterface` lifecycle, operational behavior, security posture, and runtime limits, using `workerd.capnp`, `Server::makeWorker()`, and the I/O interfaces as sources.

Verified Lua capabilities are deliberately narrow:

- The experimental HTTP worker keeps the `{ fetch = function(request) ... end }` module contract. Its inbound and handler response bodies remain buffered (1 MiB limit); `workerd.sleep()` and host operations yield through per-request Lua coroutines.
- `workerd.fetch()` uses only the linked `globalOutbound` route. Outbound response bodies are request-scoped streams read in bounded chunks with `workerd.readBody()`; `readAll()` and explicit/automatic cancellation are covered. Requests interleave at host yield points.
- Text, binary-data, and JSON configuration bindings are installed before module evaluation under `workerd.bindings`. JSON `null` uses `workerd.null`; values share the mutable service-lifetime Lua state.
- Runtime failures retain Lua source location and message; the server logs uncaught handler errors and returns a generic 500. Outbound fetch propagates internal and user trace parents through a `lua.fetch` child span.
- Configured unsupported bindings, Durable Object storage, scheduled/alarm interface calls, WebSocket upgrades, and CONNECT are rejected or fail explicitly as documented.

Lua remains experimental and trusted-code-only. Standard Lua libraries include OS and native facilities; there is no Lua CPU/memory quota, sandbox, or V8 inspector. The audit makes no claim that Lua is safe for untrusted tenant code.

## Material gaps

1. **Outbound accounting/observation:** `LuaWorkerInterface::fetch()` starts the linked subrequest channel directly. `IoContext::getSubrequest()` is where workerd applies `LimitEnforcer` and `RequestObserver`/metrics wrappers. Lua's server service has neither `IoContext` nor a `Worker::Isolate`, and fabricating a V8 worker is outside the runtime boundary. A runtime-neutral request-scope integration remains necessary before outbound-fetch parity, service bindings, or storage adapters can be treated as complete.
2. **Streaming request bodies:** Inbound `request.body` and `workerd.fetch()` request bodies are strings buffered up to 1 MiB. A backpressure-aware producer/consumer API has not been defined that preserves the existing string-body behavior. Outbound response streaming does not establish request-body streaming parity.
3. **Events and actors:** Scheduled handlers need runtime-neutral lifecycle, retry, cancellation, trace/metrics, and `waitUntil` ownership. Alarms and Durable Objects need actor identity, persistent state, per-actor concurrency, eviction, and retry semantics. Those models are absent; adding callbacks over service-global Lua state would not reproduce actor lifecycle behavior.
4. **Other operational APIs:** There is no Lua structured logging or request metrics observer, no Lua-visible tracing API, and no CPU/memory enforcement. These are documented as gaps rather than inferred from error logging, trace-parent propagation, or coroutine support.

Consequently, the `http-network` task remains open. The response-streaming, cancellation/error, route, and interleaving paths are tested, but the task's subrequest-accounting requirement is not satisfied. Do not mark the overall parity goal complete until this gap is resolved or the user accepts the material exclusion.

## Verification evidence

The latest focused run passed:

```text
bazel test //src/workerd/rt/luajit:lua-runtime-test@ //src/workerd/server:server-test@
```

The relevant server binary also built successfully:

```text
bazel build //src/workerd/server:workerd
```

Prettier, clang-format, and Buildifier checks passed for the modified file set, and `git diff --check` was clean.

Runtime/server tests cover value binding conversion and validation, cancellation and subsequent request reuse, source-aware Lua errors, trace-parent propagation, logged handler failures/500 responses, response streaming while upstream remains open, client disconnect cancellation, configured outbound routing, unsupported event/config paths, and request interleaving. These tests do not cover standard subrequest accounting or streaming request bodies.
