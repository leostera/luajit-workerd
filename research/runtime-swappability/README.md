# Workerd runtime-swappability research

## Question and conclusion

What would it take for workerd to run workloads on a runtime other than V8, and where could a runtime-neutral boundary live?

**Conclusion:** Replacing V8 as the implementation of workerd's existing JavaScript/Workers API is not an incremental backend swap. JSG is a V8 binding/runtime layer, not a neutral API facade: the worker lifecycle, JavaScript object model, event loop integration, module evaluation, promises, bindings, and public API implementations use JSG/V8 directly. Replacing that substrate while preserving current Workers behavior would amount to replacing JSG and rewriting or porting a very large portion of the API/runtime integration. This is a reasonable no-go for the “make existing JS Workers run unchanged on another VM” interpretation.

That does **not** rule out adding a second, language-specific runtime alongside V8. Workerd already has useful host-side primitives—KJ async I/O, service routing, request observers/tracing, and `WorkerInterface`—that could support a constrained non-JavaScript workload type. That would be an additive service/runtime implementation, not a V8 replacement. It needs a new explicit service construction/lifecycle boundary: the current `Server::WorkerService` and `makeWorkerImpl()` path construct a `Worker`, `Worker::Isolate`, `WorkerdApi`, JSG context, and JS entrypoints before yielding a `WorkerInterface`. Merely implementing `WorkerInterface` does not bypass those construction assumptions.

A sensible first product slice would be HTTP-only, with a documented non-JS request/response contract and an explicit list of host capabilities. It should not promise JavaScript, Web API, Node.js, Durable Object, Workflow, or RPC compatibility by default. Add those event types only when there is a deliberate language-level mapping and test contract. Preserve the V8 path unchanged.

## Current architecture and evidence

### Runtime initialization and lifecycle

- [`src/workerd/jsg/setup.h:57`](../../src/workerd/jsg/setup.h#L57) defines `V8System`, required for the JSG API and process-wide V8 initialization. [`setup.h:697`](../../src/workerd/jsg/setup.h#L697) defines JSG's typed isolate; isolate construction uses V8-specific create params and scopes.
- [`src/workerd/io/worker.h:246`](../../src/workerd/io/worker.h#L246) models `Worker::Script`, [`worker.h:333`](../../src/workerd/io/worker.h#L333) models `Worker::Isolate`, and [`worker.h:707`](../../src/workerd/io/worker.h#L707) models the V8-bound worker lock. The `Isolate` owns the `Api`, but lock acquisition and run scopes are JSG/V8-oriented.
- [`src/workerd/io/worker.c++:1115`](../../src/workerd/io/worker.c++#L1115) initializes the worker isolate, configures V8 memory pressure, GC and promise callbacks, and cross-request promise behavior. [`worker.c++:1381`](../../src/workerd/io/worker.c++#L1381) compiles scripts under V8 context/handle scopes; [`worker.c++:1925`](../../src/workerd/io/worker.c++#L1925) creates contexts and executes startup modules/scripts.
- [`src/workerd/server/cli-main.c++:295`](../../src/workerd/server/cli-main.c++#L295) initializes the V8 platform and `V8System` for serving. V8 is therefore a process-level server dependency as well as a per-worker execution choice.

### The apparent API seam is V8-specific

[`src/workerd/io/worker.h:592`](../../src/workerd/io/worker.h#L592) describes `Worker::Api` as an abstract interface that can customize supported APIs. But its methods accept/return `jsg::Lock`, `jsg::JsContext`, JSG values, and `v8::Local` handles; for example `newContext()` at [`worker.h:633`](../../src/workerd/io/worker.h#L633), module/export operations at lines 636–650, and execution-context wrapping at line 679. [`src/workerd/server/workerd-api.h:38`](../../src/workerd/server/workerd-api.h#L38) implements this seam as `WorkerdApi`, and [`workerd-api.c++:85`](../../src/workerd/server/workerd-api.c++#L85) registers a large set of API types with `JSG_DECLARE_ISOLATE_TYPE`.

The type/API substrate is extensive: a lexical count at this checkout found 385 files with `v8::` or `jsg::` references across `src/workerd/{api,io,server,jsg}` and 11,318 such references in C++ headers/sources under `{api,io,server}`. These counts are indicators of breadth, not a precise rewrite estimate. Many exposed APIs make JavaScript object identity, promises, prototypes, typed arrays, and V8/JSG conversion part of their behavior; swapping an interpreter does not preserve those contracts automatically.

### Host-side boundary and its limits

- [`src/workerd/io/worker-interface.h:36`](../../src/workerd/io/worker-interface.h#L36) defines `WorkerInterface` on top of `kj::HttpService`, with HTTP/connect, prewarm, scheduled/alarm, and custom-event operations. It is runtime-neutral at the type level, although it is broader than a plain HTTP handler.
- [`src/workerd/server/server.c++:3504`](../../src/workerd/server/server.c++#L3504) defines `Server::WorkerService`; its request path eventually calls `newWorkerEntrypoint()` at [`server.c++:4136`](../../src/workerd/server/server.c++#L4136). This operates on existing `Worker`/JS entrypoints.
- [`server.c++:5976`](../../src/workerd/server/server.c++#L5976) implements `makeWorkerImpl()`: it unconditionally creates `WorkerdApi`, `Worker::Isolate`, `Script`, and `Worker` before validating JS-exported handlers and building the service. An alternate runtime needs an alternative at or above this factory/service layer, not just an `Api` subclass.
- The KJ/server service graph, HTTP streaming, service bindings, channel plumbing, and much observability can be reused. Actor storage/events and JS binding conversion are not automatically runtime-neutral. An initial backend should say “unsupported” explicitly for unimplemented event types rather than silently emulate JS semantics.

### Candidate: libbeam

Inspected the public `leostera/libbeam` repository at HEAD `4e2062028e1a2c7f2fea8fb35f79e42ac7400e82` (README and the linked isolate/additive-construction design documents).

Its README explicitly says the public C++ Engine/Isolate API and isolate proof are not implemented; an experimental native startup entry can return to the host but cannot yet shut down safely; it is not a security boundary for untrusted workloads. Its proposed engine/isolate ownership model is substantial BEAM runtime work, not an embeddable drop-in already available to workerd. BEAM execution also does not implement JavaScript semantics or the Workers API. Mapping a BEAM program to workerd would require a deliberate entrypoint convention, event/request model, host-capability policy, cancellation and lifecycle integration.

Therefore libbeam is an interesting future backend candidate for Erlang/BEAM workloads, but it is not presently a viable implementation for an executable workerd integration POC or a replacement for V8. Revisit it after the libbeam Engine/Isolate lifecycle and its supported profile have executable evidence. Do not claim the BEAM isolation/security properties from the design documents as implemented behavior.

### Candidate for a small embedding experiment: LuaJIT

LuaJIT is used here only to check that a tiny non-JavaScript runtime can be owned and invoked behind a narrow host request contract. The experiment in [`lua-poc.cc`](lua-poc.cc) defines `RuntimeBackend`, plain request/response values, and a `HostService` that dispatches to an embedded Lua state. It runs a handler twice in one state and once in another to exercise per-state mutable state. It does not include workerd/JSG headers, alter the server, or claim that Lua is a production backend.

Run it with:

```sh
research/runtime-swappability/run-lua-poc.sh
```

It requires a local LuaJIT development installation exposed through `pkg-config` as `luajit`. The runner compiled and executed successfully in this session on macOS 26.6.2 arm64 with LuaJIT 2.1.1788856981:

```text
$ research/runtime-swappability/run-lua-poc.sh
Lua runtime seam prototype passed
exit status: 0
```

The linker also printed a non-fatal warning that `/opt/homebrew/opt/mariadb-connector-c/lib/mariadb` does not exist in this environment; the executable linked and exited successfully. The verified source and runner SHA-256 hashes were `14a6707b8779dad8073918c20c8296bd9ed2f6b44c498bd84e52a8e9c9d4f755` (`lua-poc.cc`) and `3140788faca6e2394547a896b69633de747daf9dab274c6b5cb439da778ead2c` (`run-lua-poc.sh`).

The prototype calls `luaL_openlibs()`, does not sandbox scripts, has no execution limits/cancellation, and implements only a toy HTTP-shaped call. It is not safe to run untrusted Lua and is not wired into `Server::Service` or `WorkerInterface`. Its result is narrowly that a host-owned runtime object and language-neutral request/response envelope are straightforward; product lifecycle, permissions, errors, streaming, and routing remain to be designed.

## Current verification

The runtime unit test covers request/response conversion, independent runtime states, malformed modules and results, Lua errors, and unsupported scheduled/alarm methods. The server tests exercise config selection, an end-to-end HTTP request through `Server::Service`, unsupported configuration and HTTP features, and the existing V8 service-worker path.

Verified on macOS arm64 with Bazel 9.2.0:

```sh
bazel build --registry=https://raw.githubusercontent.com/bazelbuild/bazel-central-registry/main //src/workerd/server:workerd
bazel test --registry=https://raw.githubusercontent.com/bazelbuild/bazel-central-registry/main //src/workerd/server:lua-runtime-test@
bazel test --registry=https://raw.githubusercontent.com/bazelbuild/bazel-central-registry/main //src/workerd/server:server-test@
```

The workerd binary built and both test targets passed. The raw GitHub BCR mirror was used because this checkout's JVM trust store did not validate `https://bcr.bazel.build/`; the default registry failed before dependency analysis. The declared dependency remains pinned through `bazel_dep(name = "luajit", version = "2.1.0-beta3")`.

## Implementation mapping: LuaJIT alongside V8

The confirmed implementation scope is an additive, experimental LuaJIT worker that handles stateless HTTP, not replacing the existing JavaScript worker path.

- The `Worker` union in [`workerd.capnp`](../../src/workerd/server/workerd.capnp#L293) now includes `luaScript @21` alongside `modules`, `serviceWorkerScript`, and `inherit`. This makes runtime selection intrinsic to each worker without changing the outer `Service` union.
- [`Server::makeService()`](../../src/workerd/server/server.c++#L6446) still routes the `worker` arm to `Server::makeWorker()`. `makeWorker()` branches on `luaScript` before `makeWorkerImpl()` and its V8/JSG construction path; the V8 branch remains unchanged. Lua workers get a sibling `Server::LuaWorkerService`, and the actor/workflow config passes skip Lua workers so unsupported actor settings are rejected by the Lua config validator instead of entering V8-specific construction.
- [`Server::Service`](../../src/workerd/server/server.c++#L279) remains the listener-facing lifecycle boundary: `startRequest()` returns a `WorkerInterface` for each request, while the service owns the persistent Lua state. [`LuaWorkerInterface`](../../src/workerd/server/lua-worker-interface.h) implements HTTP requests, while CONNECT and WebSockets return 501 and scheduled/alarm/custom events are unavailable. Transfer/tokenization is rejected rather than implying RPC support.
- `LuaRuntime` in [`lua-runtime.c++`](../../src/workerd/server/lua-runtime.c++) creates and closes one LuaJIT state per service, evaluates a module returning a synchronous `fetch(request)` function, and converts HTTP method, URL, headers, and bounded request/response bodies. Lua globals persist between requests for that service. The standard libraries are open, execution is synchronous, and no sandbox, memory limit, CPU limit, or preemption is provided; source must be trusted.

### Pinned Bazel dependency evidence

`MODULE.bazel` now declares `bazel_dep(name = "luajit", version = "2.1.0-beta3")`. The BCR module exposes `@luajit//:luajit`; BCR metadata pins upstream LuaJIT commit `8271c643c21d1b2f344e339f559f2de6f3663191` with SHA-256 integrity `fzseweErHjzjGtzTPXPdfFBWsd8Cx3eeKfIjna7WvJM=`. The package builds LuaJIT's generated VM through Bazel rules and has a macOS arm64 configuration.

Verified on Bazel 9.2.0 / macOS arm64 with:

```sh
bazel build --registry=https://raw.githubusercontent.com/bazelbuild/bazel-central-registry/main @luajit//:luajit
```

Result: `Build completed successfully, 101 total actions`; output was `bazel-bin/external/luajit+/libluajit.a`. The default `https://bcr.bazel.build` registry failed TLS validation in this local Java trust-store configuration before loading the graph; using GitHub's raw BCR mirror succeeded. Bazel also warned that optional Go version discovery could not reach `go.dev` due the same TLS issue, but this did not affect the LuaJIT target. The default registry issue is environmental, not a LuaJIT build failure.

## Scope and branch note

Research artifacts and implementation changes are being kept in the current `leostera/swappable-runtime` branch. No commits, pushes, or publication are being made. LuaJIT remains an experimental additive backend for synchronous HTTP handlers; it does not replace V8 or implement JavaScript, Web APIs, Node.js, Durable Objects, scheduled/alarm/RPC/connect/WebSocket events, or a security boundary. Existing V8 workers continue through their original construction path.
