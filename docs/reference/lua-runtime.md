# Experimental LuaJIT workers

LuaJIT workers are an experimental, HTTP-only runtime option selected for an individual worker with the `luaScript` member of the `Workerd.Worker` config union. V8 workers continue to use the existing JavaScript runtime path.

LuaJIT workers require workerd's `--experimental` option. A minimal config is:

```capnp
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [(name = "lua", worker = (
    luaScript = embed "worker.lua"
  ))],
  sockets = [(name = "http", address = "127.0.0.1:8080", service = "lua")]
);
```

Start the server with `workerd serve config.capnp --experimental`.

## Lua module contract

`worker.lua` is evaluated once when the worker service is constructed. It must return a table with a synchronous `fetch(request)` function:

```lua
return {
  fetch = function(request)
    return {
      status = 200,
      headers = {
        { name = "content-type", value = "text/plain" },
      },
      body = "Hello from Lua: " .. request.url,
    }
  end,
}
```

The request table contains:

- `method`: the HTTP method string.
- `url`: the request URL as passed to the workerd HTTP service (normally an absolute URL for a configured HTTP listener).
- `headers`: an array of `{ name, value }` string tables. Repeated headers are represented by repeated array entries.
- `body`: the request body as a Lua string.

The response must be a table with an integer `status` from 200 through 599 and a string `body`. It may include `headers`, an array of `{ name, value }` string tables. Response header names and values are validated by workerd's HTTP header implementation. Handler exceptions, invalid module results, and invalid response fields fail the request; they are not converted into successful responses. The HTTP reason phrase is `LuaJIT` for every response; clients should use the numeric status code.

One LuaJIT state is owned by each configured Lua worker service. The module is evaluated once and Lua globals persist across that service's HTTP requests, but state is neither shared with another worker service nor persisted across process restarts. Handlers run synchronously on the workerd request path. Request and response bodies are limited to 1 MiB each, and each header list is limited to 128 entries.

## Supported scope and limitations

The Lua backend does not implement JavaScript, Web APIs, Node.js APIs, or the JSG interface. It does not support worker bindings, custom outbound services, compatibility dates or flags, Durable Objects, Workflows, Cache API outbound bindings, module fallback, tail workers, containers, Access bindings, RPC, scheduled events, alarms, HTTP CONNECT, or WebSockets. Unsupported worker configuration is rejected during service construction; unsupported request/event methods fail explicitly.

LuaJIT is not a sandbox. The runtime opens Lua's standard libraries, including operating-system, filesystem, package-loading, and native-library facilities. Lua source must be trusted. There is no instruction, CPU-time, or memory limit; synchronous Lua code can block the request event loop. Do not use this backend for tenant-controlled or otherwise untrusted code.

## Tests

The runtime unit test covers HTTP data conversion, independent states, invalid modules and handler results, and unsupported scheduled/alarm events. The server test exercises config selection and an HTTP request through workerd's service path, and includes the unchanged V8 service-worker path as a regression check:

On macOS arm64 with Bazel 9.2.0, the workerd binary and both focused test targets built successfully; both test targets passed. This checkout's JVM trust store could not validate `https://bcr.bazel.build/`, so the commands below used the raw GitHub mirror of the Bazel Central Registry:

```sh
bazel build --registry=https://raw.githubusercontent.com/bazelbuild/bazel-central-registry/main //src/workerd/server:workerd
bazel test --registry=https://raw.githubusercontent.com/bazelbuild/bazel-central-registry/main //src/workerd/server:lua-runtime-test@
bazel test --registry=https://raw.githubusercontent.com/bazelbuild/bazel-central-registry/main //src/workerd/server:server-test@
```
