# LuaJIT host-capability integration: architecture proposal

- **Status:** Proposed design for incremental implementation; this document does not define a stable API.
- **Source snapshot:** workerd `8548f49da652625d8d7d1db84cbd868f85e0dc4f` (`leostera/luajit`), inspected 2026-10-13.
- **Related checklist:** [feature-matrix.md](feature-matrix.md).

## Decision

Extend the Lua service through a **small, explicit KJ host-capability interface**, with Lua-specific adapters at the boundary. Reuse workerd's linked service channels and per-request accounting instead of opening network/storage primitives directly. Keep the existing `fetch(request)` script contract and shared-state/cooperative-coroutine contract. Do not make a dummy V8 `Worker`, add `env`/`ctx` arguments, or expose `IoContext`/JSG/V8 objects to Lua.

This is the least-coupled route toward host capability parity. It is not a plan to make all existing JSG APIs callable from Lua.

## Important current architecture fact

`Server::LuaWorkerService::startRequest()` currently returns `LuaWorkerInterface` directly. It does not go through `Server::WorkerService::startRequest()` and `newWorkerEntrypoint()`. The latter path creates `IoContext::IncomingRequest` and `IoContext`; the Lua path does not. `IoContext` is intentionally coupled to `Worker`, `Worker::Actor`, `Worker::Lock`, and a `LimitEnforcer`, and its common call sites use `jsg::Lock`.

Therefore a Lua host API must **not** call `IoContext::current()` or assume `IoContext` already exists. Constructing a fake `Worker` merely to gain access to `IoContext` would couple the Lua runtime to V8/JSG internals and would not establish correct Worker/Isolate/GC semantics. Conversely, dispatching directly to a global service without a request-scoped host adapter would bypass request metadata, cancellation, subrequest accounting, and bindings.

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

Keep these types in the runtime/host layer and use KJ ownership. A response-streaming implementation may replace buffered bodies with explicit KJ stream handles, but those handles must never escape their request context or event loop.

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

Keep host operations in the existing `workerd` namespace (for example, `workerd.sleep`, and a Lua-designed async `workerd.fetch`). Do not pass JavaScript-shaped `env` or `ctx` parameters. When named bindings are introduced, add a dedicated capability namespace/table such as `workerd.bindings`; keep it read-only where possible and make every capability's availability explicit in config.

Do not mirror JavaScript `Request`, `Response`, `Promise`, `ReadableStream`, or DOM object semantics by name unless a written Lua contract and tests establish the needed equivalence. Prefer simple Lua values and small resource wrappers with explicit close/cancel behavior.

## Outbound HTTP and accounting

A Lua `workerd.fetch()` must use a configured `ServiceDesignator` channel, not `kj::Network` or raw sockets directly. The service-side adapter must preserve the selected route and host policy, propagate trace/request metadata, account for subrequests, and cancel the network operation when the incoming request is canceled. The `globalOutbound = "internet"` default currently passes config validation but does not create a Lua fetch capability.

Before implementing this API, factor or reuse the `WorkerService` linked-channel setup so the Lua service gets the correct target clients without instantiating a V8 worker. Confirm where a `RequestObserver`/subrequest counter can be supplied to the Lua entrypoint; the direct Lua service path currently has no `IoContext::IncomingRequest` that could supply it.

Initially buffered `workerd.fetch` bodies could be a deliberately temporary subset, bounded by documented limits. Since broad host capability parity includes large HTTP bodies and current request/response limits are 1 MiB, the planned HTTP milestone should design request and response streaming/backpressure rather than silently presenting a fully buffered adapter as general Fetch parity.

## Configuration and capability ownership

- Parse the worker's bindings into typed, immutable capability descriptors during config setup; reject unsupported types before serving.
- Resolve/link target services once at the service layer; create per-request call handles with fresh metadata at dispatch.
- No Lua global should hold a request-scoped service handle. Any handle returned to Lua must either be request-bound and invalidated at completion, or be a safe config-level immutable descriptor that can only create a current-request operation.
- Keep worker code/config and runtime state service-scoped; keep body streams, network clients, timers, trace parents, and cancellation request-scoped.
- Errors from configuration are config errors; errors from Lua compilation/module evaluation are startup errors; host operation failures resume as Lua errors and flow through the normal request failure/response path.

## Implementation order and gates

1. Extract/reuse service channel linking and provide a request-scoped KJ-only host adapter for Lua without changing JS `WorkerService` behavior.
2. Prove timer and a harmless test subrequest through this adapter, including trace/accounting and cancellation. Keep `workerd.sleep` as the regression test.
3. Define Lua-native HTTP request/response and streaming semantics; implement global outbound fetch/service binding through the adapter.
4. Add individual binding adapters on the same capability contract. Storage, actors, RPC, queues, and lifecycle events remain separate designs when their semantics require more than a request channel.

Do not extend into dynamic untrusted code loading, process isolation, or resource-quota claims under this design. Those require an independent security/resource-limit design. Before adopting this direction in production-facing code, verify channel linking and `RequestObserver` ownership against the actual service wiring and test request teardown; this note records the architecture seam discovered in source, not completed implementation evidence.

## Required architectural tests

- A Lua request can issue an allowed host fetch through the configured channel, and the response status/headers/body are mapped correctly.
- An outbound call increments the correct accounting/observer path, propagates trace metadata, and cannot route outside its configured capability.
- Client disconnect/request cancellation cancels the pending outbound operation and releases coroutine/stream/request references exactly once.
- Two requests interleave only at host yield points; a resumed coroutine receives only its own result; shared global Lua state remains serialized.
- Service shutdown cancels active work before closing `lua_State`.
- Invalid binding type/missing target fails configuration before any request is dispatched.

## Open implementation question

The biggest unresolved engineering seam is how to give the current non-`WorkerService` Lua service linked outbound channels **and** a request observer/cancellation scope without routing through V8's `WorkerEntrypoint`. Resolve this in the server architecture before implementing the first outbound API; do not use direct global service lookup as a shortcut.
