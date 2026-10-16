# Experimental LuaJIT workers

LuaJIT workers are an experimental, HTTP-only runtime option selected for an individual worker with the `luaScript` member of the `Workerd.Worker` config union. V8 workers continue to use the existing JavaScript runtime path.

LuaJIT workers require workerd's `--experimental` option. A minimal config is:

```capnp
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [(name = "lua", worker = (
    luaScript = embed "worker.lua",
    bindings = [
      (name = "GREETING", text = "Hello from Lua"),
      (name = "SETTINGS", json = "true")
    ]
  ))],
  sockets = [(name = "http", address = "127.0.0.1:8080", service = "lua")]
);
```

Start the server with `workerd serve config.capnp --experimental`.

## Lua module contract

`worker.lua` is evaluated once when the worker service is constructed. It must return a table with a `fetch(request, env)` function. The second argument contains the worker's configured bindings; existing handlers that declare only `request` continue to work. The function may suspend at supported host operations and then continue with the result:

```lua
return {
  fetch = function(request, env)
    return {
      status = 200,
      headers = {
        { name = "content-type", value = "text/plain" },
      },
      body = env.GREETING .. ": " .. request.url ..
        " (enabled=" .. tostring(env.SETTINGS) .. ")",
    }
  end,
}
```

A handler can call `workerd.sleep(milliseconds)` to suspend for a timer delay from 0 through 4,294,967,295 milliseconds. `workerd.randomBytes(size)` returns a cryptographically random, binary-safe Lua string for an integer size from 0 through 65,536 bytes; other sizes raise a Lua error. `workerd.randomUUID()` returns a random version-4 UUID. Both use the request's configured workerd entropy source. `workerd.digest(algorithm, data)` computes MD5, SHA-1, SHA-256, SHA-384, or SHA-512 over a binary-safe Lua string up to 1 MiB and returns the digest as a binary-safe Lua string. `workerd.hmac(algorithm, key, data)` computes a MAC with a binary-safe raw key from 1 through 65,536 bytes and data up to 1 MiB; it returns a binary-safe Lua string. `workerd.verifyHmac(algorithm, key, data, mac)` returns whether the supplied MAC is valid, using a constant-time comparison when MAC lengths match. These raw-key functions accept case-insensitive algorithm names and raise Lua errors for unsupported algorithms, invalid argument types, or oversized inputs. You can also pass a configured HMAC key to `workerd.hmac(key, data)` or `workerd.verifyHmac(key, data, mac)`; these opaque bindings allow only their configured `sign` and `verify` usages. Configured keys are non-extractable, contain 1–65,536 bytes, and support HMAC with SHA-1, SHA-256, SHA-384, or SHA-512. MD5 and SHA-1 are available for compatibility and should not be used where collision resistance is required. Configured HMAC keys do not expose their bytes or WebCrypto `CryptoKey` properties to Lua, and arbitrary CryptoKey algorithms/formats, other signatures, encryption, and streaming digests/MACs remain unsupported. A handler can also call `workerd.fetch(request)` to send an HTTP subrequest through the worker's configured `globalOutbound` service:

```lua
local response = workerd.fetch({
  method = "POST", -- defaults to GET
  url = "https://example.com/echo",
  headers = {
    { name = "content-type", value = "text/plain" },
  },
  body = "hello",
})
```

A `workerd.fetch()` request body may be a Lua string, the current `request.bodyStream`, or a producer function. Strings remain the default and are limited to 1 MiB. Passing `request.bodyStream` requires `luaRequestBodyStreaming`; it streams the current inbound request from its current position, with the same 1 MiB total read limit. A producer function runs in a request-scoped child coroutine and must return a binary-safe Lua string (at most 64 KiB) or `nil` at EOF; the total upload limit is 1 MiB. The next chunk is requested only after workerd accepts the previous chunk, preserving backpressure. While producing a chunk, the callback can use yielding workerd host operations such as `workerd.sleep()` and `workerd.fetch()`. For streamed bodies, caller-supplied `Content-Length` and `Transfer-Encoding` headers are replaced with workerd's streaming framing. GET/HEAD requests cannot use a body stream or producer. For example, a generator can produce a sequence of chunks without concatenating them:

```lua
local chunks = { "first", "second" }
local index = 0
local function nextChunk()
  index = index + 1
  return chunks[index]
end

local response = workerd.fetch({
  method = "POST",
  url = "https://example.com/upload",
  body = nextChunk,
})
```

The result has `status`, `headers` (an array of `{ name, value }` tables), and `body`, a response-body stream handle. `workerd.readBody(body[, maxBytes])` reads at most the requested number of bytes (default 16 KiB; valid range 1–64 KiB) and returns `nil` at EOF. `workerd.readAll(body)` reads to EOF and concatenates the chunks into one Lua string. `workerd.cancelBody(body)` closes an unread response stream and returns `true`; body handles are request-scoped, cannot be used by another request, and are automatically closed when the request completes. Body reads suspend the request coroutine while waiting for data; cancellation releases the request-scoped stream. A host-operation error is raised as a Lua error and can be caught with `pcall()`. GET and HEAD requests cannot include a body.

The inbound request table contains:

- `method`: the HTTP method string.
- `url`: the request URL as passed to the workerd HTTP service (normally an absolute URL for a configured HTTP listener).
- `headers`: an array of `{ name, value }` string tables. Repeated headers are represented by repeated array entries.
- `cf`: when the host supplies Cloudflare request metadata, a per-request Lua table decoded lazily from that JSON object on first access; absent when no metadata is supplied, and malformed JSON raises a Lua error only when accessed. JSON `null` members use `workerd.null`. This is a local value snapshot, not an Access identity binding or the JavaScript `Request.cf` compatibility implementation.
- `body`: by default, the fully buffered request body as a Lua string.

Set `luaRequestBodyStreaming = true` on the worker to opt into incremental inbound reads. In that mode, `request.body` is `nil` and `request.bodyStream` is a request-scoped handle. `workerd.readRequestBody(bodyStream[, maxBytes])` reads at most 64 KiB per call (default 16 KiB), returns `nil` at EOF, and suspends the request coroutine while waiting for input. A handler can read at most 1 MiB in total; attempting to read beyond that limit raises a Lua error. Handles cannot be reused by another request and are released when the request completes or is cancelled. This option is rejected unless `luaScript` is configured; it does not change the default string-valued `request.body` contract.

`workerd.parseUrl(url[, base])` returns a detached Lua table snapshot for an absolute remote URL, or resolves a relative URL against an absolute remote base. The returned table can be mutated without changing any host URL state. The result contains a canonical `href`, `scheme`, `host` (including port), decoded `path` components, `hasTrailingSlash`, a duplicate-preserving `query` array of `{ name, value? }` tables, and any `fragment`, `username`, or `password`. A query entry without `=` has no `value` field; an explicit empty value is `""`. Invalid arguments and URLs return `nil, error`. This deliberately uses workerd's KJ URI parser and is not a WHATWG `URL` implementation: mutating the returned Lua table does not reparse or normalize its fields, and there is no `URLSearchParams`; non-remote forms such as `data:` URLs are rejected.

Configured `text`, `data`, `json`, restricted HMAC `cryptoKey`, HTTP service, KV namespace, D1, Workers AI, and `fromEnvironment` bindings are available through the `env` argument. Text bindings become Lua strings, data bindings become binary-safe Lua strings, and JSON bindings become Lua tables/values. JSON `null` is represented by the `workerd.null` sentinel so it remains distinct from a missing table field. HTTP service bindings are exposed as `env.NAME.fetch(request)` and dispatch through their configured worker channel, independently of `globalOutbound`; they support fetch requests only, not typed Worker RPC. `fromEnvironment` bindings are captured when the Lua service is constructed: a set variable becomes a Lua string (including the empty string), while an unset variable becomes `workerd.null`. The same service-global table remains available as `workerd.bindings` for existing scripts. The table and its values are mutable Lua state shared across requests, just like other globals.

A D1 database binding configured with the `cloudflare-internal:d1-api` wrapper exposes a limited prepared-statement API. The wrapper must contain exactly one inner service binding named `fetcher`; that service is used for D1 HTTP API requests:

```capnp
(name = "DB", wrapped = (
  moduleName = "cloudflare-internal:d1-api",
  innerBindings = [(name = "fetcher", service = "d1-service")]
))
```

Lua supports `env.DB:prepare(sql):bind(...):all()`, `:first([columnName])`, and `:run()`. `all()` returns a D1 result table whose `results` field is an array of row objects; `first()` returns the first row or the selected column value, or `nil` when no row exists; `run()` returns the D1 result and metadata. Bound values may be valid UTF-8 strings, finite numbers, booleans, `workerd.null`, or `nil` (the latter two become SQL `NULL`), with at most 100 values. SQL text is limited to 64 KiB, and encoded query and response bodies are each limited to 1 MiB. Unsupported values and malformed D1 responses raise Lua errors. This adapter does not implement `batch()`, `exec()`, `dump()`, sessions/bookmarks, or the D1 JS-RPC binding contract. Other `wrapped` binding modules and wrapper shapes are rejected for Lua workers.

A Workers AI binding configured with the `cloudflare-internal:ai-api` wrapper exposes the limited `env.AI:run(model, inputs[, options])` interface. The wrapper must contain exactly one inner service binding named `fetcher`:

```capnp
(name = "AI", wrapped = (
  moduleName = "cloudflare-internal:ai-api",
  innerBindings = [(name = "fetcher", service = "ai-service")]
))
```

`inputs` and `options` are Lua tables serialized as JSON objects. Values may be nested tables, valid UTF-8 strings, finite numbers, booleans, and `workerd.null`; arrays must use contiguous integer keys starting at 1, and JSON objects must use string keys. Cycles, unsupported Lua values, mixed/sparse tables, and nesting beyond 64 levels are rejected. The `options` object is forwarded to the AI binding, including `gateway` selection for `run()`; separate AI Gateway methods and wrapper controls such as `prefix`, `extraHeaders`, `returnRawResponse`, WebSockets, streaming inputs, model search, AutoRAG, websearch, and toMarkdown are not implemented. A successful response with content type exactly `application/json` becomes Lua values; other successful response bodies become binary-safe Lua strings. Non-2xx responses raise Lua errors. Model IDs are limited to 1 KiB, and request/response JSON bodies are each limited to 1 MiB. This adapter dispatches through the configured `fetcher` service; standalone workerd does not provide Cloudflare's authenticated Workers AI service wiring. The `samples/lua-d1-ai/` example uses `cf dev` as a local bridge to remote account bindings, while `config-mock.capnp` remains available for standalone tests without live services.

The screenshot-style D1 + AI handler can then be written as:

```lua
local row = env.DB:prepare("SELECT answer FROM prompts WHERE id = ?")
  :bind("welcome"):first()
local result = env.AI:run("@cf/meta/llama-3.1-8b-instruct-fast", { prompt = row.answer })
return { status = 200, body = result.response }
```

A KV namespace configured with the `kvNamespace` binding variant exposes `env.NAME.get(key[, options])`, `.getWithMetadata(key[, options])`, `.put(key, value[, options])`, `.delete(key)`, and `.list([options])`. These calls suspend the current request coroutine during host I/O. `get()` returns the value as a binary-safe Lua string, or `nil` when the KV service responds with 404 or 410; `put()` and `delete()` return `nil` after a successful response. `get()` and `getWithMetadata()` accept only a `cacheTtl` option, an integer from 60 through 2,147,483,647 seconds; values remain Lua binary strings, and JavaScript `type` conversion is not provided. `put()` accepts `expiration` (an integer from 0 through 2,147,483,647, with timestamp policy enforced by the KV service) and `expirationTtl` (an integer from 60 through 2,147,483,647 seconds). Unlike the JavaScript KV adapter, Lua does not currently decode response `Content-Encoding`; it returns the bytes exposed by the linked KV channel. `getWithMetadata()` returns a table with `value`, `metadata`, and `cacheStatus`; metadata JSON is decoded into Lua values, and absent value/metadata/cache status are represented by `workerd.null`. For missing keys, `value` and `metadata` are `workerd.null` while any returned cache status is preserved. `list()` returns a Lua table with `keys`, `list_complete`, an optional `cursor`, and `cacheStatus`; each key has a `name` and may have `expiration` and `metadata`, whose JSON string is decoded into Lua values. Missing cache status is `workerd.null`. Non-2xx errors fail the host operation. Keys must be non-empty strings other than `.` and `..`, and at most 512 bytes; values are binary-safe Lua strings up to 1 MiB. KV response values and list responses are capped at 1 MiB. `list()` accepts an optional table with `limit` (integer 0–1000, where 0 uses the service default), `prefix` (at most 512 bytes), and `cursor` (at most 4 KiB). Other GET/PUT options, PUT metadata, KV `type` conversion, and bulk operations are rejected or not implemented. KV operations retain separate GET/GET_WITH/PUT/DELETE/LIST quota accounting and in-house subrequest admission, with request-local `lua.kv.*` child spans; they do not use the external-subrequest observer path. Standalone Lua services currently supply unrestricted KV operation limits, so this does not establish Cloudflare production quota enforcement.

A Lua HMAC key binding uses the workerd `cryptoKey` config variant with `raw`, `hex`, or `base64` key data, a JSON algorithm of `HMAC` with a SHA-1/SHA-2 hash, and `sign` and/or `verify` usages. For example:

```capnp
(name = "SIGNING_KEY", cryptoKey = (
  raw = "secret bytes",
  algorithm = (json = `{"name":"HMAC","hash":"SHA-256"}
  ),
  extractable = false,
  usages = [sign, verify]
))
```

Only non-extractable keys are accepted. PKCS#8, SPKI, JWK, other algorithms/hashes, and usages other than `sign`/`verify` are rejected during service construction. Lua receives an opaque key userdata and can use it only with the key-based HMAC overloads; the raw key bytes are not converted to Lua strings or exposed through key properties. The server-owned key buffer is cleansed when the key is destroyed.

The handler response must be a table with an integer `status` from 200 through 599 and a string `body`. It may include `headers`, an array of `{ name, value }` string tables. Response header names and values are validated by workerd's HTTP header implementation. Handler exceptions, invalid module results, and invalid response fields fail the request; they are not converted into successful responses. The HTTP reason phrase is `LuaJIT` for every response; clients should use the numeric status code.

One LuaJIT state is owned by each configured Lua worker service. The module is evaluated once and Lua globals persist across that service's HTTP requests, but state is neither shared with another worker service nor persisted across process restarts. Each request runs in its own Lua coroutine. `workerd.sleep(milliseconds)`, `workerd.fetch(request)`, `workerd.readBody()`, and opt-in `workerd.readRequestBody()` suspend that request without blocking other requests; handlers can share and interleave access to Lua globals across such suspension points. Ordinary Lua code remains synchronous and runs on the workerd event-loop thread. Inbound bodies remain limited to 1 MiB; they are buffered by default and streamed in bounded chunks only when `luaRequestBodyStreaming` is enabled. String-valued `workerd.fetch()` request bodies and handler response bodies remain buffered and limited to 1 MiB each; streamed inbound and producer uploads are bounded to 1 MiB total. Producer callbacks run in request-scoped child coroutines and may yield through existing workerd host operations. Outbound response bodies are read incrementally, with each `workerd.readBody()` call limited to 64 KiB; `workerd.readAll()` can accumulate an unbounded Lua string.

## Runtime architecture and extension boundary

The server selects a worker runtime from the `Workerd.Worker` config union. `Server::makeWorker()` keeps runtime construction explicit: JavaScript workers follow the `Worker`/`Worker::Isolate` path, while Lua workers use a service-lifetime `LuaRuntime` and a per-request `LuaWorkerInterface`. Both are exposed to the service graph through `WorkerInterface`, whose contract includes HTTP and other event types; each implementation must explicitly support or reject those event types.

The Lua engine boundary is `LuaRuntime` plus its `LuaHostOperations` interface in `src/workerd/rt/luajit/lua-runtime.{h,c++}`. LuaJIT state, coroutines, Lua value conversion, and binding installation stay on this side. Its `luajit-runtime` Bazel target depends on LuaJIT, KJ, Cap'n Proto JSON, SIMD UTF-8, and crypto libraries, not workerd I/O. `LuaIoContext` implements the host operations using workerd channels, tracing metadata, request-body state, and `WorkerInvocation`; it owns the per-request cancellation domain used by routed host operations. `LuaWorkerInterface` adapts KJ requests and responses. Their declarations are in `lua-worker-interface.h`, and the workerd-facing implementations are isolated in `lua-worker-interface.c++` and the `luajit-host-adapter` target, which depends on the runtime and workerd I/O. Values crossing between the engine and host are explicit KJ/domain values; Lua tables and LuaJIT state do not escape into workerd's service graph.

The JavaScript path has different lifecycle semantics. `Worker::Api`, JSG/V8 locks and contexts, and `IoContext` are JavaScript-specific. `IoContext` owns JavaScript promise/re-entry, isolate task, and actor lifecycle behavior; it is not a language-neutral backend. `IoContext::IncomingRequest` and `LuaIoContext` each own a `WorkerInvocation`, which composes neutral subrequest/KV admission and a narrow observer hook. JavaScript selects `ExecutionContextCancellationDomain`, leaving cancellation with its existing JSG/I/O async machinery rather than adding another wrapper around subrequest clients; this preserves actor request handoffs. Lua selects `RequestScopedCancellationDomain`, whose canceler is owned by `WorkerInvocation`, so routed host clients are canceled when that request scope ends. The scope borrows its policies and observer. This shares host-operation ownership without flattening the runtimes' distinct delivery, draining, abort, or actor semantics. `WorkerInvocationObserver` and `CountSubrequest` live in a small header independent of JSG; `RequestObserver` implements that hook on the JavaScript/workerd side. `cli-main` also initializes V8 for the server process, so adding another worker runtime does not make the current process independent of V8.

A future runtime can follow this extension path without adopting JSG or claiming JavaScript API parity:

1. Add a distinct worker-config union arm and parse it at the server boundary into domain types. Reject unsupported or malformed configuration before constructing the service; use a tagged type for binding alternatives so a payload cannot be paired with an unrelated discriminator.
2. Add an explicit service-construction branch that owns one runtime instance for the required service lifetime and resolves configured service channels in the server's link phase.
3. Implement `WorkerInterface` for the request/event contract the runtime actually supports. Explicitly reject unsupported event types and worker configuration rather than silently emulating JavaScript behavior.
4. Define a plain-value host-operation interface and a per-request context with explicit ownership, cancellation, tracing, and resource-lifetime rules. Use `WorkerInvocation` for shared subrequest/KV admission, the neutral observer hook, and cancellation of routed host operations; explicitly select the cancellation domain that matches the runtime's ownership semantics. Keep engine objects and runtime-specific state on their adapter side.
5. Add focused tests for config parsing, runtime-state lifetime, request cancellation, body/resource ownership, host-operation routing, accounting/observer behavior, and every supported event path. Document limits and security assumptions; do not treat the runtime as a sandbox unless it actually provides one.

## Supported scope and limitations

The Lua backend does not implement JavaScript, Web APIs, Node.js APIs, or the JSG interface. `workerd.fetch()` is an experimental HTTP adapter over the configured `globalOutbound` route. Lua and JavaScript invocation contexts share `WorkerInvocation` for subrequest/KV admission and observer wrapping. Lua's request-scoped cancellation domain also wraps routed host clients; JavaScript retains cancellation in its existing promise/re-entry machinery, preserving its actor-aware context lifetime. `LuaIoContext` composes the scope with Lua's route and trace metadata, while `IoContext::IncomingRequest` composes it with JavaScript request attribution. The standalone Lua server currently supplies an unrestricted policy and `NoopWorkerInvocationObserver`, matching workerd's standalone limit/observer behavior; embedders can inject invocation-specific policies and observers. This does not add Lua CPU/memory limits or metrics that workerd's current server does not provide. Optional incoming CF-Blob JSON is exposed on `request.cf` as a per-request Lua table; this does not add Access identity bindings or JavaScript-specific CF compatibility behavior. Outbound response bodies support incremental reads and backpressure through `workerd.readBody()`; inbound request bodies can opt into bounded reads with `luaRequestBodyStreaming` and be streamed through `workerd.fetch()`. Lua producer functions can provide bounded upload chunks from a request-scoped child coroutine and may yield through existing host operations; handler response streaming remains unsupported. When the inbound request is cancelled, pending outbound operations and response-body reads are cancelled with it.

Lua exceptions retain their `worker.lua` source location and message in the KJ failure; the server logs an uncaught exception and returns a generic HTTP 500. Lua does not have a request-scoped `console`/structured logging API, a Worker metrics observer, or a Lua inspector. Outbound fetch propagates internal and user trace parents through a `lua.fetch` child span. Subrequests pass through the shared request observer hook, but the standalone server supplies `NoopWorkerInvocationObserver`; no Lua-visible metrics API or Cloudflare production metrics wiring is established.

Supported bindings currently include text, data, JSON, restricted non-extractable HMAC CryptoKey bindings, HTTP service bindings, KV namespaces (get/getWithMetadata/put/delete/list), the limited D1 prepared-statement adapter, a JSON-oriented Workers AI `run()` adapter, and `fromEnvironment`. HTTP service bindings expose only `fetch(request)`; typed service-binding RPC is not implemented. CryptoKey bindings accept raw, hex, or base64 key data for HMAC-SHA-1/SHA-2 signing and verification only; they are opaque and usage-restricted, not a general WebCrypto key API. Other service/storage integrations remain unavailable: Analytics Engine, Cache API, Hyperdrive, process-local memory cache, Queues, R2, and SQL/actor storage. Other unimplemented configuration or lifecycle surfaces include parameter/inherited bindings and wrapped bindings other than the supported D1 and Workers AI shapes, other crypto key types and algorithms, module fallback and Worker Loader, containers, Access identity bindings, Workflows, scheduled events, alarms, Durable Objects, and tail workers. The runtime also has no Lua WebSocket/CONNECT handler, host-mediated TCP/UDP sockets, HTMLRewriter, managed webfs, or Spectre-mitigated performance clock. Lua errors, HTTP trace-parent propagation, and `request.cf` do not provide Lua-visible logging, metrics, tracing APIs, resource quotas, or Cloudflare production policy wiring. Unsupported worker configuration is rejected during service construction; unsupported request/event methods fail explicitly.

Scheduled handlers are deferred until Lua requests have a runtime-neutral event scope for lifecycle completion, retry outcomes, cancellation, tracing, metrics, and `waitUntil` work. Alarms depend on per-object identity, persistent state, serialized concurrency, and eviction/retry lifecycle, none of which exists for Lua workers; implementing an alarm callback over service-global Lua state would not preserve Durable Object semantics.

LuaJIT is not a sandbox. The runtime opens Lua's standard libraries, including operating-system, filesystem, package-loading, and native-library facilities. Lua source must be trusted. There is no instruction, CPU-time, or memory limit; synchronous Lua code can block the request event loop, even though `workerd.sleep()` can yield while waiting for a timer. Do not use this backend for tenant-controlled or otherwise untrusted code.
