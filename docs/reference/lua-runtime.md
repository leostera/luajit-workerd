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

`worker.lua` is evaluated once when the worker service is constructed. It must return a table with a `fetch(request)` function. The function may suspend at supported host operations and then continue with the result:

```lua
return {
  fetch = function(request)
    return {
      status = 200,
      headers = {
        { name = "content-type", value = "text/plain" },
      },
      body = workerd.bindings.GREETING .. ": " .. request.url ..
        " (enabled=" .. tostring(workerd.bindings.SETTINGS) .. ")",
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

A `workerd.fetch()` request body may be a Lua string, the current `request.bodyStream`, or a producer function. Strings remain the default and are limited to 1 MiB. Passing `request.bodyStream` requires `luaRequestBodyStreaming`; it streams the current inbound request from its current position, with the same 1 MiB total read limit. A producer function is called synchronously on demand and must return a binary-safe Lua string (at most 64 KiB) or `nil` at EOF; the total upload limit is 1 MiB. The next chunk is requested only after workerd accepts the previous chunk, preserving backpressure. Producer functions cannot yield or perform asynchronous host operations. For streamed bodies, caller-supplied `Content-Length` and `Transfer-Encoding` headers are replaced with workerd's streaming framing. GET/HEAD requests cannot use a body stream or producer. For example, a generator can produce a sequence of chunks without concatenating them:

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

Configured `text`, `data`, `json`, restricted HMAC `cryptoKey`, HTTP service, KV namespace, and `fromEnvironment` bindings are available through the service-global `workerd.bindings` table; no `env` or `ctx` argument is added. Text bindings become Lua strings, data bindings become binary-safe Lua strings, and JSON bindings become Lua tables/values. JSON `null` is represented by the `workerd.null` sentinel so it remains distinct from a missing table field. HTTP service bindings are exposed as `workerd.bindings.NAME.fetch(request)` and dispatch through their configured worker channel, independently of `globalOutbound`; they support fetch requests only, not typed Worker RPC. `fromEnvironment` bindings are captured when the Lua service is constructed: a set variable becomes a Lua string (including the empty string), while an unset variable becomes `workerd.null`. These values are available only to the trusted Lua source and are not passed as an `env` argument. The binding table and its values are mutable Lua state shared across requests, just like other globals.

A KV namespace configured with the `kvNamespace` binding variant exposes `workerd.bindings.NAME.get(key)`, `.put(key, value)`, `.delete(key)`, and `.list([options])`. These calls suspend the current request coroutine during host I/O. `get()` returns the value as a binary-safe Lua string, or `nil` when the KV service responds with 404 or 410; `put()` and `delete()` return `nil` after a successful response. `list()` returns a Lua table with `keys`, `list_complete`, an optional `cursor`, and `cacheStatus`; each key has a `name` and may have `expiration` and `metadata`, whose JSON string is decoded into Lua values. Missing cache status is `workerd.null`. Non-2xx errors fail the host operation. Keys must be non-empty strings other than `.` and `..`, and at most 512 bytes; values are binary-safe Lua strings up to 1 MiB. KV response values and list responses are capped at 1 MiB. `list()` accepts an optional table with `limit` (integer 0–1000, where 0 uses the service default), `prefix` (at most 512 bytes), and `cursor` (at most 4 KiB). Other list options, GET options, `getWithMetadata()`, PUT metadata/expiration controls, and bulk operations are not implemented. KV operations retain the dedicated GET/PUT/DELETE/LIST quota dimension and in-house subrequest admission, with request-local `lua.kv.*` child spans; they do not use the external-subrequest observer path. Standalone Lua services currently supply unrestricted KV operation limits, so this does not establish Cloudflare production quota enforcement.

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

One LuaJIT state is owned by each configured Lua worker service. The module is evaluated once and Lua globals persist across that service's HTTP requests, but state is neither shared with another worker service nor persisted across process restarts. Each request runs in its own Lua coroutine. `workerd.sleep(milliseconds)`, `workerd.fetch(request)`, `workerd.readBody()`, and opt-in `workerd.readRequestBody()` suspend that request without blocking other requests; handlers can share and interleave access to Lua globals across such suspension points. Ordinary Lua code remains synchronous and runs on the workerd event-loop thread. Inbound bodies remain limited to 1 MiB; they are buffered by default and streamed in bounded chunks only when `luaRequestBodyStreaming` is enabled. String-valued `workerd.fetch()` request bodies and handler response bodies remain buffered and limited to 1 MiB each; streamed inbound and producer uploads are bounded to 1 MiB total. Outbound response bodies are read incrementally, with each `workerd.readBody()` call limited to 64 KiB; `workerd.readAll()` can accumulate an unbounded Lua string.

## Supported scope and limitations

The Lua backend does not implement JavaScript, Web APIs, Node.js APIs, or the JSG interface. `workerd.fetch()` is an experimental HTTP adapter over the configured `globalOutbound` route. Lua and JavaScript invocation contexts share the runtime-neutral `RequestScope` for subrequest admission and observer wrapping; `LuaIoContext` composes it with Lua's route and trace metadata, while `IoContext::IncomingRequest` composes it on the JavaScript path. The standalone Lua server currently supplies an unrestricted policy and the base no-op `RequestObserver`, matching workerd's standalone limit/observer behavior; embedders can inject invocation-specific policies and observers. This does not add Lua CPU/memory limits or metrics that workerd's current server does not provide. Optional incoming CF-Blob JSON is exposed on `request.cf` as a per-request Lua table; this does not add Access identity bindings or JavaScript-specific CF compatibility behavior. Outbound response bodies support incremental reads and backpressure through `workerd.readBody()`; inbound request bodies can opt into bounded reads with `luaRequestBodyStreaming` and be streamed through `workerd.fetch()`. Lua producer functions can provide bounded synchronous upload chunks; handler response streaming remains unsupported. KJ promise cancellation is covered at the runtime boundary and by server tests that disconnect the inbound client while an outbound fetch or response-body read is pending.

Lua exceptions retain their `worker.lua` source location and message in the KJ failure; the server logs an uncaught exception and returns a generic HTTP 500. Lua does not have a request-scoped `console`/structured logging API, a Worker metrics observer, or a Lua inspector. Outbound fetch propagates internal and user trace parents through a `lua.fetch` child span. Subrequests pass through the shared request observer hook, but the standalone server supplies the base no-op observer; no Lua-visible metrics API or Cloudflare production metrics wiring is established.

Supported bindings currently include text, data, JSON, restricted non-extractable HMAC CryptoKey bindings, HTTP service bindings, KV namespaces (get/put/delete/list), and `fromEnvironment`. HTTP service bindings expose only `fetch(request)`; typed service-binding RPC is not implemented. CryptoKey bindings accept raw, hex, or base64 key data for HMAC-SHA-1/SHA-2 signing and verification only; they are opaque and usage-restricted, not a general WebCrypto key API. Other service/storage integrations remain unavailable: Analytics Engine, Cache API, Hyperdrive, process-local memory cache, Queues, R2, and SQL/actor storage. Other unimplemented configuration or lifecycle surfaces include parameter/inherited/wrapped bindings, other crypto key types and algorithms, module fallback and Worker Loader, containers, Access identity bindings, Workflows, scheduled events, alarms, Durable Objects, and tail workers. The runtime also has no Lua WebSocket/CONNECT handler, host-mediated TCP/UDP sockets, HTMLRewriter, managed webfs, or Spectre-mitigated performance clock. These are tracked as blocked in the parity matrix; Lua errors, HTTP trace-parent propagation, `request.cf`, and `RequestScope` hooks do not imply the missing logging, metrics, tracing API, resource quotas, or Cloudflare production policy wiring. Unsupported worker configuration is rejected during service construction; unsupported request/event methods fail explicitly.

Scheduled handlers are deferred until Lua requests have a runtime-neutral event scope for lifecycle completion, retry outcomes, cancellation, tracing, metrics, and `waitUntil` work. Alarms depend on per-object identity, persistent state, serialized concurrency, and eviction/retry lifecycle, none of which exists for Lua workers; implementing an alarm callback over service-global Lua state would not preserve Durable Object semantics.

LuaJIT is not a sandbox. The runtime opens Lua's standard libraries, including operating-system, filesystem, package-loading, and native-library facilities. Lua source must be trusted. There is no instruction, CPU-time, or memory limit; synchronous Lua code can block the request event loop, even though `workerd.sleep()` can yield while waiting for a timer. Do not use this backend for tenant-controlled or otherwise untrusted code.

## Tests

The runtime unit tests cover text/data/JSON binding conversion and persistence, malformed JSON, per-request CF metadata decoding, isolation, and lazy-parse errors, source-aware Lua errors, outbound trace-parent propagation, explicit rejection of scheduled and alarm events, HTTP and KV data conversion, host-operation request/response mapping, KV missing values, list metadata/cache-status conversion and malformed-result rejection, and operation accounting, bounded cryptographic random bytes including zero and 64 KiB boundary cases, version-4 UUID generation, bounded MD5/SHA-1/SHA-2 digest and HMAC vectors including binary input, case-insensitive algorithm names, digest/input/key size boundaries, and MAC verification success/failure, absolute and relative URL parsing, invalid URL handling, chunked response reads, request-local body-handle validation, streamed inbound body reads with async chunk boundaries, binary chunks, partial-read errors, the 1 MiB stream limit, cross-request handle rejection, demand-driven binary producer chunks, oversized-chunk rejection, cancellation of host operations, response streams, and pending inbound body reads, coroutine suspension/resumption, independent states, and invalid modules and handler results. Server tests cover text/JSON, HMAC CryptoKey raw/hex/base64 formats and algorithm parsing, rejection of extractable/malformed key configuration, and environment-derived bindings; HTTP service-binding dispatch independent of `globalOutbound`, KV namespace routing, GET/PUT/DELETE including a 404 missing-key response, and list query/result mapping, duplicate and unsupported binding rejection, optional incoming `request.cf` metadata, configured entropy-source delegation for random bytes, Durable Object storage rejection, logged Lua handler failures and HTTP 500 responses, custom `globalOutbound` routing, incremental response reads, opt-in inbound request-body streaming and rejection of the option on non-Lua workers, client disconnect cancellation during outbound operations and inbound body reads, and an inbound request suspended through `workerd.sleep()`. They also verify injected per-invocation subrequest admission and observer wrapping on global outbound and service-binding routes, a separate injected KV-operation limit policy, streamed outbound pass-through while the inbound body is open, Lua-produced chunk uploads, and cancellation after client disconnect. The standalone server uses unrestricted/no-op subrequest and KV-operation implementations; producer callbacks cannot yield for asynchronous I/O.
