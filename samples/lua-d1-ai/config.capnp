using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "lua", worker = (
      luaScript = embed "worker.lua",
      bindings = [
        (name = "DB", wrapped = (
          moduleName = "cloudflare-internal:d1-api",
          innerBindings = [(name = "fetcher", service = "wrangler-bridge")]
        )),
        (name = "AI", wrapped = (
          moduleName = "cloudflare-internal:ai-api",
          innerBindings = [(name = "fetcher", service = "wrangler-bridge")]
        ))
      ]
    )),
    (name = "wrangler-bridge", external = (
      address = "127.0.0.1:18788",
      http = (style = host)
    ))
  ],
  sockets = [(name = "http", address = "127.0.0.1:8080", service = "lua")]
);
