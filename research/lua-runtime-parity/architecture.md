# LuaJIT host-capability integration: architecture and implementation

- **Status:** Architecture and implementation note for experimental Lua host capabilities; this document does not define a stable API.
- **Source snapshot:** workerd `631d8bee2` (`leostera/luajit`), inspected 2026-10-13.
- **Related checklist:** [feature-matrix.md](feature-matrix.md).

## Decision

Extend the Lua service through a **small, explicit KJ host-capability interface**, with Lua-specific adapters at the boundary. Reuse workerd's linked service channels and per-request accounting instead of opening network/storage primitives directly. Keep the existing `fetch(request)` script contract and shared-state/cooperative-coroutine contract. Do not make a dummy V8 `Worker`, add `env`/`ctx` arguments, or expose `IoContext`/JSG/V8 objects to Lua.

This is the least-coupled route toward host capability parity. It is not a plan to make all existing JSG APIs callable from Lua.

## Important current architecture fact

`Server::LuaWorkerService::startRequest()` currently returns `LuaWorkerInterface` directly. It does not go through `Server::WorkerService::startRequest()` and `newWorkerEntrypoint()`. The latter path creates `IoContext::IncomingRequest` and `IoContext`; the Lua path does not. `IoContext` is intentionally coupled to `Worker`, `Worker::Actor`, `Worker::Lock`, and a `LimitEnforcer`, and its common call sites use `jsg::Lock`.

Therefore a Lua host API must **not** call `IoContext::current()` or assume `IoContext` already exists. Constructing a fake `Worker` merely to gain access to `IoContext` would couple the Lua runtime to V8/JSG internals and would not establish correct Worker/Isolate/GC semantics. The Lua path uses a request-scoped `LuaIoContext` and the linked `globalOutbound` service with parent metadata; it also propagates KJ promise cancellation. A shared runtime-neutral `RequestScope` now handles subrequest admission and observer wrapping for both Lua and JavaScript invocation contexts. The standalone Lua server injects an unrestricted policy and the base no-op observer, matching standalone workerd behavior. Inbound request bodies support opt-in chunked reads; the default `request.body` remains buffered, and `workerd.fetch()` supports pass-through streams and synchronous Lua chunk producers with backpressure. Producer callbacks cannot yield for asynchronous I/O.

Relevant seams in source:

- Lua service: `src/workerd/server/server.c++`, `Server::LuaWorkerService` and `Server::makeWorker()`.
- JS service: `src/workerd/server/server.c++`, `Server::WorkerService`; it owns linked I/O channels, implements `IoChannelFactory`/`LimitEnforcer`/`TimerChannel`, and creates the standard worker entrypoint.
- Standard request scope: `src/workerd/io/worker-entrypoint.c++`, `WorkerEntrypoint::init()`; this constructs `IoContext` and `IoContext::IncomingRequest`.
- Context and host channels: `src/workerd/io/io-context.h` and `src/workerd/io/io-channels.h`.

## Proposed boundaries

### 1. Service-lifetime `LuaWorkerService`

Owns the Lua module/runtime and immutable config-level capability descriptions. It links configured service targets during the server's existing service-link phase. It must not own per-request HTTP clients, active Lua requests, or request-scoped spans.

The current constructor receives the source, simple bindings, timer, configured `globalOutbound`, and server channel context. It resolves the target through the server's service-linking phase and retains the linked route for request-scoped `LuaIoContext` instances. Do not make the Lua service a subclass of the V8 `WorkerService` just to inherit its private state.

### 2. Request-lifetime `LuaIoContext`

`LuaWorkerService::startRequest()` creates a `LuaWorkerInterface` for each request. That interface owns a `LuaIoContext`, then passes it privately to `LuaRuntime::fetch()` as `LuaHostOperations`; it is not an extra Lua handler argument. The context owns or borrows, for that request's lifetime:

- the linked outbound route selected by worker configuration;
- request/subrequest metadata needed for trace propagation;
- the shared `RequestScope` admission and observer hooks;
- Lua-specific trace metadata and host fetch operations.

`workerd.sleep()` is implemented by the coroutine bridge and the runtime timer; response stream handles are owned by the coroutine invocation. KJ cancellation tears down the in-flight promise and request-local streams. Keep this narrower than `IoContext`; do not transplant its `Worker::Lock` and isolate lifecycle into Lua. `IoChannelFactory` and `RequestObserver` remain host-side types and are not exposed to Lua code.

The current `LuaHostOperations` interface in `src/workerd/rt/luajit/lua-runtime.h` contains the Lua-native HTTP request/response types, an async `fetch()` operation, synchronous random-byte/UUID operations, and optional serialized CF request metadata. `LuaIoContext` delegates randomness to the same injected `kj::EntropySource` used by the standard request context. It does not expose `sleep()` or `onAbort()` as virtual methods: timer suspension is handled by the runtime bridge, and cancellation is expressed through KJ promise lifetime.

### 3. Lua runtime and coroutine bridge

`LuaRuntime` owns one service-lifetime Lua state, as today. A fetch invocation owns one Lua coroutine and a registry reference until success, error, or cancellation. All Lua API calls and coroutine resumes happen on the owning event-loop thread; no concurrent or nested `lua_resume()` calls are permitted.

Lua host operations use a closed, typed protocol:

1. A Lua wrapper such as `workerd.sleep(...)`, `workerd.randomBytes(...)`, `workerd.randomUUID()`, or `workerd.fetch(...)` validates its Lua arguments and yields a descriptor.
2. The C++ bridge parses the descriptor with raw Lua table access into a typed operation. Reject unknown operations and malformed fields; do not invoke user metamethods during parsing.
3. Pure operations such as random-byte generation are handled synchronously by the runtime bridge. Request I/O operations are started by the request's `LuaHostOperations` as KJ promises.
4. On async completion, the bridge resumes only that request's coroutine with a result or a typed error. The operation and coroutine are destroyed exactly once on completion or cancellation.

A promise continuation must not retain a borrowed `lua_State*`, response object, host stream, or request context after its owner is gone. Use explicit request ownership/attachments and cancellation cleanup; verify this with cancellation and teardown tests. Coroutines are cooperative: this bridge yields only at registered host calls and does not interrupt CPU-bound Lua.

### 4. Lua-visible API

Preserve:

```lua
return {
  fetch = function(request)
    -- request.method, request.url, request.headers, request.body
  end,
}
```

Keep host operations in the existing `workerd` namespace (for example, `workerd.sleep`, `workerd.randomBytes`, `workerd.randomUUID`, and a Lua-designed async `workerd.fetch`). Simple text, binary-data, and JSON bindings are exposed through `workerd.bindings`; JSON `null` is represented by `workerd.null`. This preserves the `fetch(request)` signature without adding JavaScript-shaped `env` or `ctx` parameters. Binding values are installed when the service's Lua state starts and therefore share Lua's service-global mutability/lifetime. Each future capability must make its config and request-scoped availability explicit.

Do not mirror JavaScript `Request`, `Response`, `Promise`, `ReadableStream`, or DOM object semantics by name unless a written Lua contract and tests establish the needed equivalence. Prefer simple Lua values and small resource wrappers with explicit close/cancel behavior. `workerd.parseUrl(url[, base])` is a synchronous, narrow adapter over `kj::Url`: it converts parsed URL components into plain Lua tables and deliberately does not expose the KJ type or promise WHATWG URL semantics.

## Outbound HTTP and accounting

A Lua `workerd.fetch()` must use a configured `ServiceDesignator` channel, not `kj::Network` or raw sockets directly. The adapter links `globalOutbound`, propagates request metadata, accepts buffered string uploads, streams the current inbound body, or pulls synchronous Lua producer chunks with backpressure. It exposes the outbound response body as a request-scoped stream. `workerd.readBody()` reads at most 64 KiB per host operation and yields while waiting for data; `workerd.readAll()` is a convenience that can accumulate an unbounded Lua string. `workerd.cancelBody()` closes an unread stream. Stream handles carry request-local identifiers and are rejected if reused by another request. Server coverage verifies that Lua can return a received chunk before the upstream response body is complete.

The Lua adapter uses `RequestScope` (`src/workerd/io/request-scope.{h,c++}`) rather than trying to reuse `IoContext::getSubrequest()` directly. `RequestScope` delegates admission to a runtime-neutral `SubrequestLimitPolicy` and client wrapping to the invocation's `RequestObserver`. `LimitEnforcer` implements that policy for JavaScript requests; `IoContext::IncomingRequest` owns the JavaScript scope, while `LuaIoContext` owns a Lua scope composed with Lua's configured outbound route and trace metadata. The Lua test injects a rejecting policy and recording observer to verify admission occurs before routing, and that successful clients reach the observer wrapper. The standalone server currently supplies an unrestricted policy and base no-op observer, as do standalone workerd's limit/observer implementations; this does not claim Cloudflare production enforcement or metrics. V8 external-memory adjustments remain on the JS-specific path. Inbound request bodies stream only in the explicit `luaRequestBodyStreaming` mode and remain limited to 1 MiB; outbound fetch can pass through this stream or pull bounded synchronous Lua producer chunks, while string bodies stay buffered. Keep this capability experimental, do not claim unverified production parity, and do not instantiate a fake V8 worker.

## Request scope: reuse IoContext behavior without constructing IoContext

The Lua service should reuse the applicable subrequest behavior of `IoContext`, but it cannot currently instantiate an `IoContext` for itself. `IoContext` is constructed by `WorkerEntrypoint::init()` with a `Worker`, `ThreadContext`, and `LimitEnforcer`; its constructor consults the worker isolate, and its broader lifecycle manages JS locks, GC/I/O ownership, actor gates, and waitUntil work. A fake `Worker`/V8 isolate or an implicit `IoContext::current()` lookup would couple Lua to machinery it does not own.

The implementation uses a composite runtime context with JS- and Lua-specific lifetimes. The reusable unit is a narrower, invocation-scoped **request scope**, not the whole existing `IoContext`. That class is `final`, constructed around `Worker`/`LimitEnforcer`, and combines request I/O with JS locks, actor state, inspector behavior, GC/I/O ownership, and waitUntil. On this branch, a source search found references to `IoContext` across 173 workerd files and more than 450 `current()`/`tryCurrent()` call sites. Turning that entire class into an interface or renaming it wholesale would be a broad migration with many JS-only consumers.

A phased composite shape avoids that cutover, and its subrequest-policy/observer slice is implemented:

- Keep the existing `IoContext` as the JS/actor-facing context, and move shared subrequest admission and observer wrapping behind `RequestScope` plus the `SubrequestLimitPolicy` interface.
- `IoContext::IncomingRequest` owns one scope and delegates the existing JS admission/wrapping operations through it without changing JS call sites.
- Give each Lua invocation a `LuaIoContext` that composes the same scope with Lua coroutine/response-stream lifetime, the linked outbound route, and Lua trace metadata, without a `Worker`, isolate, actor API, or JS lock.
- Scope the shared component to one `IncomingRequest`/event even when an actor's outer `IoContext` persists across multiple events; do not leak subrequest counts, observers, or trace state between invocations.
- Keep JSG/V8 memory accounting and actor-specific behavior in the JS/actor side; do not imply those capabilities exist for Lua.

`IoContext_IncomingRequest` is the JS-side composition seam: it owns per-invocation metrics, the channel factory, worker tracer, and invocation spans while pointing back to the longer-lived `IoContext`. It now owns a `RequestScope` backed by the current `LimitEnforcer` and observer. Keep `drain()`, scheduled/alarm handling, actor gates, and waitUntil semantics on the JS/actor side. A `LuaIoContext` composes one such request scope per Lua HTTP invocation with the configured route and Lua coroutine/stream lifetime.

`RequestScope` deliberately exposes only the shared subrequest operations:

- Admit a subrequest using the invocation's policy before dispatch.
- Wrap the resulting client with the invocation's `RequestObserver`.
- Avoid exposing JSG, V8, JS locks, or `IoContext` to `LuaRuntime`/LJGlue. V8 external-memory adjustment remains on the JS-specific path.

Trace construction, route selection, pending-work cancellation, and resource lifetime remain with each runtime adapter because their ownership differs. `LuaIoContext` applies admission before calling its own already-linked outbound route, then wraps that client's `WorkerInterface`; it does not borrow its caller's scope or route. It is created for one `LuaWorkerInterface` request, so service-dispatched Lua requests get their own policy/observer lifetime. Future capability adapters must use the same scope and preserve their service's configured target.

`LuaWorkerService::startRequest()` creates a fresh Lua interface and scope for every dispatched request, using the Lua service's already-linked route and that invocation's metadata. The `WorkerInterface` request promise bounds the scope lifetime; cancellation propagates through the Lua coroutine and response streams. Runtime tests inject a rejecting policy and recording observer, while server tests verify configured routing and disconnect cancellation. Exact-once teardown accounting and any Cloudflare production provider's policy/observer injection remain unverified. Do not use ambient thread-local context.

## Configuration and capability ownership

- Parse text, data, and JSON bindings into typed descriptors during config setup; reject duplicates and unsupported types before serving. Treat service/storage, secret-derived, and cryptographic bindings separately because they require request accounting, redaction, or key-usage designs.
- Resolve/link target services once at the service layer; create per-request call handles with fresh metadata at dispatch.
- No Lua global should hold a request-scoped service handle. Any handle returned to Lua must either be request-bound and invalidated at completion, or be a safe config-level immutable descriptor that can only create a current-request operation.
- Keep worker code/config and runtime state service-scoped; keep body streams, network clients, timers, trace parents, and cancellation request-scoped.
- Errors from configuration are config errors; errors from Lua compilation/module evaluation are startup errors; host operation failures resume as Lua errors and flow through the normal request failure/response path.

## Implementation order and gates

1. Keep the linked `ServiceDesignator` and request-scoped KJ adapter, without changing JS `WorkerService` behavior.
2. Implement and test `RequestScope` admission and observer wrapping. Request cancellation remains owned by each runtime's invocation lifetime; keep cancellation regressions alongside the scope tests.
3. Keep opt-in inbound streaming additive to the default string contract; synchronous outbound producer callbacks are implemented, while yieldable producer sources remain a separate design.
4. Add individual binding adapters on the same capability contract. Storage, actors, RPC, queues, and lifecycle events remain separate designs when their semantics require more than a request channel.

Do not extend into dynamic untrusted code loading, process isolation, or resource-quota claims under this design. Those require an independent security/resource-limit design. The `RequestScope` seam and local Lua server wiring are implemented and tested; this note does not verify any Cloudflare production provider's policy/observer wiring or request quotas.

## Required architectural tests

- A Lua request can issue an allowed host fetch through the configured channel, and the response status/headers/body are mapped correctly. (Routing and response mapping are tested; body reads are incremental.)
- An outbound call is admitted before dispatch, reaches the invocation observer wrapper, propagates trace metadata, and cannot route outside its configured capability. (The runtime test injects a rejecting policy and recording observer; the standalone server uses unrestricted/no-op implementations.)
- Client disconnect/request cancellation cancels the pending outbound operation and releases coroutine/stream/request references. (Runtime cancellation, request-local stream-handle validation, explicit stream cancellation, and end-to-end server disconnect during both pending fetch and response-body read are tested; exact-once release is not instrumented.)
- Two requests interleave only at host yield points; a resumed coroutine receives only its own result; shared global Lua state remains serialized. (Timer interleaving and host-result isolation are tested.)
- Service shutdown cancels active work before closing `lua_State`. (Not yet covered by a dedicated teardown test.)
- Invalid binding type/missing target fails configuration before any request is dispatched.

## Open implementation question

The shared subrequest seam is implemented as `RequestScope` plus `SubrequestLimitPolicy`; JavaScript's `IoContext::IncomingRequest` and Lua's `LuaIoContext` compose that component with their own invocation lifetimes. The standalone server supplies an unrestricted limit policy and the base no-op observer, matching current standalone workerd behavior. Do not infer Cloudflare production enforcement or metrics from this adapter: any provider-specific exclusions remain unverified and must be accepted before the overall goal can be completed. Inbound body streaming is available only through the opt-in configuration and remains limited to 1 MiB; `workerd.fetch()` can pass that stream through or pull bounded synchronous Lua producer chunks. Producer callbacks cannot suspend for asynchronous I/O. Do not instantiate a fake V8 worker or reach for `IoContext::current()`.
