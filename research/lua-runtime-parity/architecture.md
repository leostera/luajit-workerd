# LuaJIT host-capability integration: architecture proposal

- **Status:** Design proposal with an experimental HTTP prototype; this document does not define a stable API.
- **Source snapshot:** workerd `8548f49da652625d8d7d1db84cbd868f85e0dc4f` (`leostera/luajit`), inspected 2026-10-13.
- **Related checklist:** [feature-matrix.md](feature-matrix.md).

## Decision

Extend the Lua service through a **small, explicit KJ host-capability interface**, with Lua-specific adapters at the boundary. Reuse workerd's linked service channels and per-request accounting instead of opening network/storage primitives directly. Keep the existing `fetch(request)` script contract and shared-state/cooperative-coroutine contract. Do not make a dummy V8 `Worker`, add `env`/`ctx` arguments, or expose `IoContext`/JSG/V8 objects to Lua.

This is the least-coupled route toward host capability parity. It is not a plan to make all existing JSG APIs callable from Lua.

## Important current architecture fact

`Server::LuaWorkerService::startRequest()` currently returns `LuaWorkerInterface` directly. It does not go through `Server::WorkerService::startRequest()` and `newWorkerEntrypoint()`. The latter path creates `IoContext::IncomingRequest` and `IoContext`; the Lua path does not. `IoContext` is intentionally coupled to `Worker`, `Worker::Actor`, `Worker::Lock`, and a `LimitEnforcer`, and its common call sites use `jsg::Lock`.

Therefore a Lua host API must **not** call `IoContext::current()` or assume `IoContext` already exists. Constructing a fake `Worker` merely to gain access to `IoContext` would couple the Lua runtime to V8/JSG internals and would not establish correct Worker/Isolate/GC semantics. The current prototype adds a request-scoped KJ adapter and uses the linked `globalOutbound` service with parent metadata; it also propagates KJ promise cancellation. It still bypasses `IoContext`'s normal subrequest observer/accounting wrappers and buffers HTTP bodies, so it is not parity-complete.

Relevant seams in source:

- Lua service: `src/workerd/server/server.c++`, `Server::LuaWorkerService` and `Server::makeWorker()`.
- JS service: `src/workerd/server/server.c++`, `Server::WorkerService`; it owns linked I/O channels, implements `IoChannelFactory`/`LimitEnforcer`/`TimerChannel`, and creates the standard worker entrypoint.
- Standard request scope: `src/workerd/io/worker-entrypoint.c++`, `WorkerEntrypoint::init()`; this constructs `IoContext` and `IoContext::IncomingRequest`.
- Context and host channels: `src/workerd/io/io-context.h` and `src/workerd/io/io-channels.h`.

## Proposed boundaries

### 1. Service-lifetime `LuaWorkerService`

Owns the Lua module/runtime and immutable config-level capability descriptions. It links configured service targets during the server's existing service-link phase. It must not own per-request HTTP clients, active Lua requests, or request-scoped spans.

The current constructor only receives service name, source, and timer. Capability work will need a shared server-side channel/linking seam. Prefer factoring the reusable _channel construction and linking_ out of `WorkerService` over duplicating its target resolution and creating an independent path. Do not make the Lua service a subclass of the V8 `WorkerService` just to inherit its private state.

### 2. Request-lifetime `LuaRequestContext`

Created for each `WorkerInterface::request()` invocation and passed privately from the C++ interface into `LuaRuntime`; it is not an extra Lua handler argument. It owns or borrows, for exactly that request's lifetime:

- the linked host capability handles selected by worker configuration;
- request/subrequest metadata needed for trace propagation;
- host methods for outbound calls and supported asynchronous operations;
- the request's abort/cancellation signal and operation cleanup;
- the metrics/accounting hook used to count and observe outbound work.

Keep this narrower than `IoContext`. As the implementation evolves, factor any truly shared, language-neutral request context out of `IoContext`; do not transplant its `Worker::Lock` and isolate lifecycle into Lua. `IoChannelFactory` and `RequestObserver` provide useful host-side reference contracts, but Lua-facing code should consume a smaller adapter so it cannot accidentally depend on JavaScript behavior.

Suggested initial host-side shape (illustrative, not a committed ABI):

```cpp
struct LuaHttpRequest;
struct LuaHttpResponse;

class LuaHostOperations {
 public:
  virtual kj::Promise<LuaHttpResponse> fetch(LuaHttpRequest request) = 0;
  virtual kj::Promise<void> sleep(kj::Duration delay) = 0;
  virtual kj::Promise<void> onAbort() = 0;
};
```

Keep these types in the runtime/host layer and use KJ ownership. Outbound response bodies use KJ stream handles held in the current request's Lua coroutine frame; Lua sees a request-local stream ID and reads bounded chunks through host operations. The handle is destroyed with the request and never escapes its event loop.

### 3. Lua runtime and coroutine bridge

`LuaRuntime` owns one service-lifetime Lua state, as today. A fetch invocation owns one Lua coroutine and a registry reference until success, error, or cancellation. All Lua API calls and coroutine resumes happen on the owning event-loop thread; no concurrent or nested `lua_resume()` calls are permitted.

Host async operations use a closed, typed protocol:

1. A Lua wrapper such as `workerd.sleep(...)` or `workerd.fetch(...)` validates its Lua arguments and yields a descriptor.
2. The C++ bridge parses the descriptor with raw Lua table access into a typed operation. Reject unknown operations and malformed fields; do not invoke user metamethods during parsing.
3. The request's `LuaHostOperations` starts the KJ promise.
4. On completion, the bridge resumes only that request's coroutine with a result or a typed error. The operation and coroutine are destroyed exactly once on completion or cancellation.

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

Keep host operations in the existing `workerd` namespace (for example, `workerd.sleep`, and a Lua-designed async `workerd.fetch`). Simple text, binary-data, and JSON bindings are exposed through `workerd.bindings`; JSON `null` is represented by `workerd.null`. This preserves the `fetch(request)` signature without adding JavaScript-shaped `env` or `ctx` parameters. Binding values are installed when the service's Lua state starts and therefore share Lua's service-global mutability/lifetime. Each future capability must make its config and request-scoped availability explicit.

Do not mirror JavaScript `Request`, `Response`, `Promise`, `ReadableStream`, or DOM object semantics by name unless a written Lua contract and tests establish the needed equivalence. Prefer simple Lua values and small resource wrappers with explicit close/cancel behavior.

## Outbound HTTP and accounting

A Lua `workerd.fetch()` must use a configured `ServiceDesignator` channel, not `kj::Network` or raw sockets directly. The prototype links `globalOutbound`, propagates request metadata, buffers outbound request bodies (up to 1 MiB), and exposes the outbound response body as a request-scoped stream. `workerd.readBody()` reads at most 64 KiB per host operation and yields while waiting for data; `workerd.readAll()` is a convenience that can accumulate an unbounded Lua string. `workerd.cancelBody()` closes an unread stream. Stream handles carry request-local identifiers and are rejected if reused by another request. Server coverage verifies that Lua can return a received chunk before the upstream response body is complete.

The adapter does not yet reuse `IoContext::getSubrequest()` or provide an equivalent request observer/accounting adapter. Source inspection shows `IoContext` applies `LimitEnforcer::newSubrequest()`, wraps clients with `RequestObserver::wrapSubrequestClient()`, and attaches V8 external-memory adjustments when a JS lock is held. `IoChannelFactory::SubrequestMetadata` carries trace and compatibility data but no request-scope capability; `LuaWorkerService::startRequest()` receives only this metadata and the service's linked outbound route. The `LimitEnforcer` contract itself depends on `jsg::Lock` and `IoContext`, so exposing it directly to Lua would leak JSG types. `LuaWorkerService` has no `IoContext` or `Worker::Isolate`, and constructing a fake one would violate the runtime boundary. A runtime-neutral request-scope API must be factored and passed explicitly through service dispatch before Lua can preserve applicable limits and observer hooks. Inbound/outbound request bodies also remain buffered. Keep this capability experimental and document those gaps; do not claim JavaScript-fetch parity. Do not instantiate a fake V8 worker to get there.

## Request scope: reuse IoContext behavior without constructing IoContext

The Lua service should reuse the applicable subrequest behavior of `IoContext`, but it cannot currently instantiate an `IoContext` for itself. `IoContext` is constructed by `WorkerEntrypoint::init()` with a `Worker`, `ThreadContext`, and `LimitEnforcer`; its constructor consults the worker isolate, and its broader lifecycle manages JS locks, GC/I/O ownership, actor gates, and waitUntil work. A fake `Worker`/V8 isolate or an implicit `IoContext::current()` lookup would couple Lua to machinery it does not own.

The useful direction is a composite runtime context with JS- and Lua-specific lifetimes, but the reusable unit should be a narrower, invocation-scoped **request scope**, not the whole existing `IoContext`. The current class is `final`, constructed around `Worker`/`LimitEnforcer`, and combines request I/O with JS locks, actor state, inspector behavior, GC/I/O ownership, and waitUntil. On this branch, a source search found references to `IoContext` across 173 workerd files and more than 450 `current()`/`tryCurrent()` call sites. Turning that entire class into an interface or renaming it wholesale would be a broad migration with many JS-only consumers.

A phased composite shape avoids that cutover:

- Keep the existing `IoContext` as the JS/actor-facing context initially, but move the runtime-neutral invocation services behind a small interface/component (the name `RequestScope` below is provisional).
- Make the JS context delegate the shared subrequest, trace/observer, and I/O-lifetime behavior to that component without changing existing JS call sites.
- Give each Lua invocation a `LuaIoContext` that composes the same request-scope component with Lua coroutine/stream lifetime management, without a `Worker`, isolate, actor API, or JS lock.
- Scope the shared component to one `IncomingRequest`/event even when an actor's outer `IoContext` persists across multiple events; do not leak subrequest counts, observers, or trace state between invocations.
- Keep JSG/V8 memory accounting and actor-specific behavior in the JS/actor side; do not imply those capabilities exist for Lua.

The existing `IoContext_IncomingRequest` is a useful extraction seam: it already owns per-invocation metrics, the channel factory, worker tracer, and invocation spans while pointing back to the longer-lived `IoContext`. Extract only the runtime-neutral request services from that object. Keep `drain()`, scheduled/alarm handling, actor gates, and waitUntil semantics on the JS/actor side. A `LuaIoContext` should compose one such request scope per HTTP invocation with its Lua coroutine/stream lifetime.

A `RequestScope` should expose only the shared host operations:

- Admit a subrequest using the applicable limit policy before dispatch.
- Establish and propagate the child trace context.
- Wrap the resulting client with the current request's `RequestObserver`/metrics hooks.
- Tie pending subrequest work to request cancellation and lifetime.
- Avoid exposing JSG, V8, JS locks, or `IoContext` to `LuaRuntime`/LJGlue. V8 external-memory adjustment remains on the JS-specific path.

A request-scope operation should accept a factory for the **already-linked Lua outbound route**, plus the operation/tracing metadata and accounting classification. The scope wraps that client; it must not substitute the caller's global outbound route for the Lua service's configured `globalOutbound`. The scope belongs to the Lua invocation itself: when Lua is called through a service binding, it must not borrow the caller's `IoContext`, which would charge the subrequest to the wrong worker/request. Instead, factor the reusable request-scope behavior from the JS `IoContext` path and construct a fresh scope for each Lua invocation, both for direct HTTP entry and service-dispatched calls.

Pass the scope explicitly as a request-lifetime capability through service dispatch (or an equivalent typed request object). Its lifetime must be bounded by the Lua invocation so cancellation tears down streams and clients before the scope disappears. The exact ownership path still needs validation against `WorkerInterface` teardown and the current request metadata plumbing; this is a proposed seam, not implemented code. Tests should cover both a direct Lua request and a Lua service-binding request, verify the configured route is retained, observe the Lua invocation's own subrequest-limit and metrics behavior, and prove disconnect cancellation releases the operation. Do not use ambient thread-local context.

## Configuration and capability ownership

- Parse text, data, and JSON bindings into typed descriptors during config setup; reject duplicates and unsupported types before serving. Treat service/storage, secret-derived, and cryptographic bindings separately because they require request accounting, redaction, or key-usage designs.
- Resolve/link target services once at the service layer; create per-request call handles with fresh metadata at dispatch.
- No Lua global should hold a request-scoped service handle. Any handle returned to Lua must either be request-bound and invalidated at completion, or be a safe config-level immutable descriptor that can only create a current-request operation.
- Keep worker code/config and runtime state service-scoped; keep body streams, network clients, timers, trace parents, and cancellation request-scoped.
- Errors from configuration are config errors; errors from Lua compilation/module evaluation are startup errors; host operation failures resume as Lua errors and flow through the normal request failure/response path.

## Implementation order and gates

1. Keep the linked `ServiceDesignator` and request-scoped KJ adapter, without changing JS `WorkerService` behavior.
2. Complete the runtime-neutral `RequestScope` for subrequest observation/accounting and cancellation, or document a verified alternative. Keep `workerd.sleep` as the regression test.
3. Extend the currently streamed outbound response-body path to a justified Lua-native request-body streaming contract if preserving the `fetch(request)` surface allows it.
4. Add individual binding adapters on the same capability contract. Storage, actors, RPC, queues, and lifecycle events remain separate designs when their semantics require more than a request channel.

Do not extend into dynamic untrusted code loading, process isolation, or resource-quota claims under this design. Those require an independent security/resource-limit design. Before adopting this direction in production-facing code, verify channel linking and `RequestObserver` ownership against the actual service wiring and test request teardown; this note records the architecture seam discovered in source, not completed implementation evidence.

## Required architectural tests

- A Lua request can issue an allowed host fetch through the configured channel, and the response status/headers/body are mapped correctly. (Routing and response mapping are tested; body reads are incremental.)
- An outbound call increments the correct accounting/observer path, propagates trace metadata, and cannot route outside its configured capability. (Configured routing and metadata propagation are implemented; observer/accounting remains unresolved.)
- Client disconnect/request cancellation cancels the pending outbound operation and releases coroutine/stream/request references. (Runtime cancellation, request-local stream-handle validation, explicit stream cancellation, and end-to-end server disconnect during both pending fetch and response-body read are tested; exact-once release is not instrumented.)
- Two requests interleave only at host yield points; a resumed coroutine receives only its own result; shared global Lua state remains serialized. (Timer interleaving and host-result isolation are tested.)
- Service shutdown cancels active work before closing `lua_State`.
- Invalid binding type/missing target fails configuration before any request is dispatched.

## Open implementation question

The unresolved engineering seam is how to give the current non-`WorkerService` Lua service an explicit, runtime-neutral request scope that adapts the standard subrequest limit and observer/accounting hooks without routing through V8's `WorkerEntrypoint`. The experimental prototype avoids raw network access and uses a request-scoped adapter, but it does not close that accounting gap. Resolve this in the server architecture before treating outbound networking as parity-complete; do not instantiate a fake V8 worker or reach for `IoContext::current()`.
