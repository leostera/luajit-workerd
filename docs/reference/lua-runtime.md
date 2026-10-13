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

A handler can call `workerd.sleep(milliseconds)` to suspend for a timer delay from 0 through 4,294,967,295 milliseconds. It can also call `workerd.fetch(request)` to send an HTTP subrequest through the worker's configured `globalOutbound` service:

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

The request body is a Lua string. The result has `status`, `headers` (an array of `{ name, value }` tables), and `body`, a response-body stream handle. `workerd.readBody(body[, maxBytes])` reads at most the requested number of bytes (default 16 KiB; valid range 1–64 KiB) and returns `nil` at EOF. `workerd.readAll(body)` reads to EOF and concatenates the chunks into one Lua string. `workerd.cancelBody(body)` closes an unread response stream and returns `true`; body handles are request-scoped, cannot be used by another request, and are automatically closed when the request completes. Body reads suspend the request coroutine while waiting for data; cancellation releases the request-scoped stream. A host-operation error is raised as a Lua error and can be caught with `pcall()`. GET and HEAD requests cannot include a body.

The inbound request table contains:

- `method`: the HTTP method string.
- `url`: the request URL as passed to the workerd HTTP service (normally an absolute URL for a configured HTTP listener).
- `headers`: an array of `{ name, value }` string tables. Repeated headers are represented by repeated array entries.
- `body`: the fully buffered request body as a Lua string.

Configured `text`, `data`, and `json` bindings are available through the service-global `workerd.bindings` table; no `env` or `ctx` argument is added. Text bindings become Lua strings, data bindings become binary-safe Lua strings, and JSON bindings become Lua tables/values. JSON `null` is represented by the `workerd.null` sentinel so it remains distinct from a missing table field. The binding table and its values are mutable Lua state shared across requests, just like other globals.

The handler response must be a table with an integer `status` from 200 through 599 and a string `body`. It may include `headers`, an array of `{ name, value }` string tables. Response header names and values are validated by workerd's HTTP header implementation. Handler exceptions, invalid module results, and invalid response fields fail the request; they are not converted into successful responses. The HTTP reason phrase is `LuaJIT` for every response; clients should use the numeric status code.

One LuaJIT state is owned by each configured Lua worker service. The module is evaluated once and Lua globals persist across that service's HTTP requests, but state is neither shared with another worker service nor persisted across process restarts. Each request runs in its own Lua coroutine. `workerd.sleep(milliseconds)`, `workerd.fetch(request)`, and `workerd.readBody()` suspend that request without blocking other requests; handlers can share and interleave access to Lua globals across such suspension points. Ordinary Lua code remains synchronous and runs on the workerd event-loop thread. Inbound request bodies, outbound request bodies, and handler response bodies are buffered and limited to 1 MiB each. `workerd.fetch()` response bodies are read incrementally, with each `workerd.readBody()` call limited to 64 KiB; `workerd.readAll()` can accumulate an unbounded Lua string. Each header list is limited to 128 entries. Streaming request bodies are not implemented: adding backpressure-aware inbound and outbound body sources requires a Lua producer/consumer contract that preserves the existing string-valued `request.body` and `workerd.fetch()` body behavior.

## Supported scope and limitations

The Lua backend does not implement JavaScript, Web APIs, Node.js APIs, or the JSG interface. `workerd.fetch()` is an experimental HTTP adapter over the configured `globalOutbound` route. Response bodies support incremental reads and backpressure through `workerd.readBody()`, but inbound and outbound request bodies remain buffered. The adapter does not use the normal `IoContext` subrequest accounting/observer path, so it is not equivalent to JavaScript `fetch()`; a runtime-neutral host-context integration is still required. KJ promise cancellation is covered at the runtime boundary and by server tests that disconnect the inbound client while an outbound fetch or response-body read is pending.

Service and storage bindings, parameter/inherited bindings, environment-derived values, crypto keys, Wasm bindings, compatibility dates or flags, Durable Objects, Workflows, Cache API bindings, module fallback, tail workers, containers, Access bindings, RPC, scheduled events, alarms, HTTP CONNECT, and WebSockets are not implemented. Only text, data, and JSON values are currently supported as bindings. Unsupported worker configuration is rejected during service construction; unsupported request/event methods fail explicitly.

Scheduled handlers are deferred until Lua requests have a runtime-neutral event scope for lifecycle completion, retry outcomes, cancellation, tracing, metrics, and `waitUntil` work. Alarms depend on actor identity and persistent storage, neither of which exists for Lua workers; implementing an alarm callback over service-global Lua state would not preserve Durable Object lifecycle semantics.

LuaJIT is not a sandbox. The runtime opens Lua's standard libraries, including operating-system, filesystem, package-loading, and native-library facilities. Lua source must be trusted. There is no instruction, CPU-time, or memory limit; synchronous Lua code can block the request event loop, even though `workerd.sleep()` can yield while waiting for a timer. Do not use this backend for tenant-controlled or otherwise untrusted code.

## Tests

The runtime unit tests cover text/data/JSON binding conversion and persistence, malformed JSON, explicit rejection of scheduled and alarm events, HTTP data conversion, host-operation request/response mapping, chunked response-body reads, request-local body-handle validation, cancellation of host operations and response streams, coroutine suspension/resumption, independent states, and invalid modules and handler results. Server tests cover text/JSON config bindings, duplicate and unsupported binding rejection, Durable Object storage rejection, custom `globalOutbound` routing, incremental response reads while the upstream body is still open, client disconnects during pending fetches and body reads, and an inbound request suspended through `workerd.sleep()`. They do not establish `IoContext` subrequest accounting/observer parity or streaming request-body support.
