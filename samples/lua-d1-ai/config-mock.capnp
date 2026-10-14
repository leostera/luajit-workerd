using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "lua", worker = (
      luaScript = embed "worker.lua",
      bindings = [
        (name = "DB", wrapped = (
          moduleName = "cloudflare-internal:d1-api",
          innerBindings = [(name = "fetcher", service = "d1-mock")]
        )),
        (name = "AI", wrapped = (
          moduleName = "cloudflare-internal:ai-api",
          innerBindings = [(name = "fetcher", service = "ai-mock")]
        ))
      ]
    )),
    (name = "d1-mock", worker = (luaScript = embed "d1-mock.lua")),
    (name = "ai-mock", worker = (luaScript = embed "ai-mock.lua"))
  ],
  sockets = [(name = "http", address = "127.0.0.1:8080", service = "lua")]
);
