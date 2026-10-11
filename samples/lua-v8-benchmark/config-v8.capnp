using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [(name = "v8", worker = .v8Worker)],
  sockets = [
    (name = "v8-http", address = "127.0.0.1:18081", http = (), service = "v8"),
  ],
);

const v8Worker :Workerd.Worker = (
  compatibilityDate = "2025-01-01",
  modules = [(name = "worker.js", esModule = embed "worker.js")],
);
