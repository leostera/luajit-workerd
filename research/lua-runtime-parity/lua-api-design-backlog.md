# Lua host-capability API design backlog

- **Status:** Exploratory; proposals are not accepted API contracts.
- **Source snapshot:** workerd `9f79d373b08a52f7865cfdfeaedf68f15cd4efec`, audited 2026-10-14.
- **Purpose:** Turn the 20 `BL` JSG-family labels in [feature-matrix.md](feature-matrix.md) into explicit Lua-facing design work, rather than suggesting those capabilities are intrinsically impossible.
- **Scope:** Candidate idioms, dependencies, and questions only. No handler/config/trust contract is changed by this note.

## Interpretation of `BL`

`BL` currently means “the capability is not implemented and a design, dependency, or policy prerequisite remains.” It does not mean “cannot be implemented.” The matrix keeps its `AS`/`NA`/`BL` family classification for continuity; this note adds a second axis:

1. **Lua API design:** decide the Lua value shape and method/event contract.
2. **Host integration:** connect the proposed interface to workerd services, channels, timers, storage, or `WorkerInterface` paths.
3. **Lifecycle and policy:** define ownership, cancellation, retries, resource/accounting rules, egress, serialization, and shutdown.
4. **Decision/verification gate:** obtain explicit approval where the handler contract or trusted-code boundary would change, and verify production wiring where standalone tests cannot.

A family may involve several axes. Some have obvious initial Lua shapes; none below is a final API decision.

## Shared Lua design principles

- Keep the existing module-returned `fetch(request)` entrypoint and do not add positional `env` or `ctx` arguments.
- Put configured capabilities under `workerd.bindings.NAME`; do not expose ambient network, storage, or filesystem access that bypasses workerd policy.
- Model async host I/O as ordinary Lua calls that suspend the current request coroutine and resume with a value or a catchable Lua error. Do not expose JavaScript promises.
- Use binary-safe Lua strings for byte sequences, Lua tables for structured values, `workerd.null` for explicit null, and opaque/request-scoped handles where streams or connections need lifetime control.
- Make close/cancel, EOF, retry, and ownership semantics visible; do not let request-bound handles escape into shared service state.
- Preserve request-scope admission, tracing, and accounting via host adapters. A Lua wrapper is not evidence that the standalone server or Cloudflare production path enforces policy.
- Keep this runtime trusted-code-only. A binding API does not make Lua a sandbox; dynamic code loading, raw networking, and filesystem access need separate threat-model review.

## Family-by-family design questions

| JSG family | Candidate Lua-facing shape (illustrative only) | Design work and dependencies |
| --- | --- | --- |
| `durable-objects` | A configured actor binding plus a Lua-defined actor factory in a separate actor-export section; methods would be ordinary yielding Lua methods on a per-object instance. | Define actor identity, construction, per-object VM/state ownership, method dispatch, serialization, concurrency gates, eviction, migrations, and how actor exports coexist with the existing fetch export. Large architecture decision. |
| `durable-objects-state` | An actor-instance `state` capability, with coroutine-yielding storage methods and explicit transaction/SQL handles. | Depends on the actor lifecycle. Specify transactions, consistency, persistence, alarms, restart behavior, cancellation, and per-operation accounting before API methods. |
| `analytics-engine` | `workerd.bindings.ANALYTICS:writeDataPoint({ indexes = ..., blobs = ..., doubles = ... })`. | Decide table field types, null/omission rules, batch limits, error behavior, write accounting, and whether the service's HTTP-backed adapter is reusable. |
| `cache` | `workerd.bindings.CACHE:match(request)`, `:put(request, response)`, and `:delete(request)` with response-body handles. | Define cache namespace/config, request-key semantics, cache statuses, stream ownership, cancellation, body limits, and accounting. Do not silently equate `globalOutbound` with Cache API. |
| `html-rewriter` | A transform that consumes an HTTP response and a Lua handler table, returning a new response; selector callbacks would be explicit Lua functions. | Design a runtime-neutral callback protocol that cannot yield through unsafe C frames, while preserving backpressure, cancellation, parse errors, and handler lifetime. Compare event callbacks with a bounded pull API. |
| `hyperdrive` | A typed binding exposing a connection or database client, not a raw host/secret descriptor. | Decide whether Lua receives a standard driver, query interface, or socket capability; specify secret exposure, pooling/connection lifetime, cancellation, transaction behavior, and policy. |
| `memory-cache` | `workerd.bindings.MEMORY_CACHE:get(key)`, `:put(key, value[, options])`, and `:delete(key)` using byte strings and an opaque namespace descriptor. | Define process/service scope, cross-worker sharing, value/key types, eviction, limits, visibility, concurrency, and accounting. Reuse the configured memory-cache implementation rather than introducing a second cache. |
| `queue` | Producer binding methods such as `queue:send(value)` and `queue:sendBatch(values)`; consumer delivery would be a separate event handler contract. | Define message serialization, batch bounds, retry/ack ownership, delivery lifetime, cancellation, and whether queue consumers are in scope. Producer support does not imply consumer lifecycle support. |
| `r2` | `workerd.bindings.BUCKET:get/put/delete/list`, returning object metadata plus request-scoped body readers/writers. | Define metadata tables, conditional writes, range/multipart operations, streaming body ownership, cancellation, size limits, and R2-specific accounting. Reuse service bindings without exposing HTTP endpoints as the public API. |
| `worker-rpc` | A configured service object with a method-call API such as `service:call(method, args)`, yielding a restricted Lua value result. | Define serialization domain, capability transfer (likely none initially), error/rejection mapping, method lookup, resource limits, recursion, and request accounting. Keep distinct from HTTP `fetch()`. |
| `scheduled` | A possible additional `scheduled(event)` callback in the returned module table; `fetch(request)` remains unchanged. | This adds a worker event contract. Define event payload, completion/retry result, cancellation, trace context, `waitUntil` equivalent or explicit absence, shutdown, and concurrent access to service-global Lua state before approval/implementation. |
| `websocket` | A request-scoped duplex handle with `receive()`, `send(bytes)`, `close(code, reason)`, and explicit cancellation. | Define upgrade acceptance, message/binary representation, fragmentation, queues/backpressure, ping/pong, disconnect, cancellation, and whether callbacks or pull-based reads fit Lua coroutines. Integrate the `WorkerInterface` upgrade path. |
| `sql` | No independent top-level SQL binding proposed; an actor's `state.storage.sql`-like capability is the closest Lua-shaped analogue. | Depends on Durable Objects. Specify query/value types, transaction lifetime, SQLite concurrency, result iteration, cancellation, and schema/migration semantics only after the actor contract exists. |
| `sockets` | A configured socket capability whose `connect()` returns an opaque stream/datagram handle; never expose arbitrary LuaJIT OS sockets as the workerd API. | Define TCP/UDP egress policy, address allowlists, DNS, deadlines, read/write backpressure, datagram boundaries, cancellation, accounting, and observability. Raw sockets would bypass host controls. |
| `container` | A configured container capability with explicit start/exec/stop operations and request-scoped I/O handles. | Depends on actor/container ownership and container-engine policy. Define process lifetime, reuse, isolation, egress, resource budgets, stream cancellation, and shutdown before exposing this service. |
| `webfs` | A root-scoped binding with `open/read/write/stat` methods returning opaque file handles; paths are relative to the configured root. | Define permissions, symlink traversal, path normalization, file lifetime, async I/O, quotas, and whether this duplicates existing OS library access. Preserve managed-root semantics rather than presenting it as unrestricted filesystem access. |
| `sync-kv` | No positive API proposal. Keep the existing coroutine-yielding KV API as the default. | Decide whether a concrete local/cached use case justifies synchronous access. Otherwise retain as an intentional non-goal: blocking remote I/O can stall the event loop and undermines the Lua async contract. |
| `worker-loader` | A configured loader object might expose `load({ name, script, bindings })`, returning a worker handle with `fetch(request)`. | Dynamic source is code loading, not an ordinary binding. Define code provenance/trust, isolation, binding allowlists, cache/reuse, unloading, limits, compatibility, and cancellation. Do not implement without explicit trust-boundary approval. |
| `performance` | `workerd.performance.now()` (and perhaps `timeOrigin`) backed by workerd's request `TimerChannel`. | Obtain a request-scoped TimerChannel through a runtime-neutral interface; preserve Spectre mitigation and timeout clamping. Decide monotonicity/precision and test that no ambient `kj::Timer` bypass is possible. |
| `tracing` | A scoped callback form such as `workerd.tracing.withSpan(name, attributes, function(span) ... end)` plus limited span methods. | Define span lifetime across coroutine yields, parent propagation, tags/events, error status, context isolation, and interaction with tracing/metrics policy. Avoid leaking KJ span types or permitting request-crossing handles. |

## Suggested design sequencing

1. **Choose use cases before designing every surface equally.** The family list is exhaustive inventory, not evidence of demand. Rank storage, event, and networking APIs using representative trusted Lua programs.
2. **Define a common async/resource contract.** Decide how yielding methods, catchable errors, cancellation, and opaque request-scoped handles compose. Reuse the existing descriptor/coroutine bridge rather than each binding inventing its own protocol.
3. **Design capability families with stable host services first.** For each candidate, identify the existing workerd channel/service and its accounting hooks before specifying Lua names or return values.
4. **Treat lifecycle-heavy families as separate proposals.** Actors, scheduled/queue consumers, WebSockets, and containers need contracts for ownership, retries, shutdown, and concurrency—not only method names.
5. **Separate design from authorization and evidence.** Handler-response streaming still requires explicit user approval; dynamic loading or trust changes require security review; production policy/metrics need provider verification. A design sketch does not close any of those gates.

## Open decisions

- Which trusted Lua workload should be the first design target, and which of the 20 families does it actually need?
- Should event handlers be additional named fields in the returned table, a separate config-selected entrypoint, or remain out of scope? No event contract is selected here.
- Should Durable Objects be a Lua feature at all, given their per-object lifecycle and persistence costs?
- Which data and error shapes should be common across bindings, and which are service-specific?
- Which capabilities require a user/policy/security decision before design can become an implementation proposal?

This backlog intentionally does not select APIs or authorize handler/trust-boundary changes. Convert a row into a reviewable proposal only after its use case, host dependency, lifecycle semantics, and required approvals are known.
