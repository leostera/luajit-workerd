# LuaJIT and V8 HTTP benchmark

This sample compares the current synchronous LuaJIT `fetch(request)` handler contract with a JavaScript `fetch(request)` Worker through the same workerd binary, HTTP server, and loopback client. It does not change the Lua API or provide `env`/`ctx`.

Both workers expose matching endpoints:

- `GET /hello` returns the same short text response. This mostly measures request dispatch and response overhead.
- `GET /cpu?iters=N` runs the same deterministic numeric loop and returns identical compact JSON. This adds predictable handler work so the comparison is not just an empty-response measurement.

Parity here means the same request cases and observable status, content type, and body—not instruction-for-instruction identity. The handlers use each language's normal URL/query and response-formatting operations, so the comparison includes those runtime-specific costs.

The Python 3.9+ standard-library driver verifies the response status, content type, and body, warms both endpoints, then measures one backend at a time using multiple concurrent keep-alive clients. It alternates backend order across rounds and reports per endpoint: requests per second and p50/p95 client-observed request latency. Workerd process startup-to-ready is reported separately because both workers share one process. Driver settings are configurable; defaults are 8 clients, 2 seconds of warmup, 10 seconds per measurement, and 3 alternating rounds.

This is a local end-to-end HTTP benchmark, not a raw interpreter microbenchmark. It includes workerd dispatch and the Python client; results are specific to the host, build, settings, and these two workloads. It compares status, content type, and body; it does not compare reason phrases (the Lua runtime currently uses a fixed `LuaJIT` reason phrase). See [`docs/reference/lua-runtime.md`](../../docs/reference/lua-runtime.md) for the runtime contract and limitations. The Python client can itself become the throughput bottleneck, so compare repeated runs and do not treat these results as a general runtime ranking.

## Run the examples

Build the optimized workerd server binary, then start both services (the Lua worker requires `--experimental`):

```sh
bazel build --config=benchmark //src/workerd/server:workerd
bazel run --config=benchmark //src/workerd/server:workerd -- serve samples/lua-v8-benchmark/config.capnp --experimental
```

If the default Bazel Central Registry fails certificate validation in your environment, add the registry mirror flag shown in [Recorded run](#recorded-run) to Bazel commands.

The Lua service listens on port 18080 and V8 on 18081. For example:

```sh
curl 'http://127.0.0.1:18080/hello'
curl 'http://127.0.0.1:18081/cpu?iters=10000'
```

To run the JavaScript example alone on the ordinary V8 worker path, use its V8-only config without `--experimental`:

```sh
bazel run --config=benchmark //src/workerd/server:workerd -- serve samples/lua-v8-benchmark/config-v8.capnp
```

`--experimental` is needed for the combined config because it contains a Lua worker; the JavaScript service itself uses the regular ES-module Worker config.

## Run the comparison

The driver starts and stops the same server config itself:

```sh
python3 samples/lua-v8-benchmark/benchmark.py \
  --workerd bazel-bin/src/workerd/server/workerd
```

Use `--help` to change ports, concurrency, warmup, measurement duration, number of alternating rounds, or loop iterations. Close other CPU-heavy workloads and record the host/OS, Bazel build mode, and driver settings when sharing measurements. The Lua worker remains experimental and trusted-source-only; this benchmark does not measure security or establish JavaScript/Web API compatibility.

## Recorded run

Two complete default-settings runs of the final handlers and driver on 2026-10-11 produced the following ranges. Each run reports throughput as the median across its three rounds and latency percentiles over requests pooled across those rounds. The table shows the minimum and maximum across the two runs. Readiness time covers the shared process and both listeners, not an individual runtime.

| Endpoint | Runtime | Requests/s (range) | p50 latency (ms, range) | p95 latency (ms, range) |
| --- | --- | ---: | ---: | ---: |
| `/hello` | LuaJIT | 8,680.1–10,004.3 | 0.760–0.842 | 1.756–1.886 |
| `/hello` | V8 | 3,994.5–4,024.3 | 1.402–1.445 | 2.063–2.183 |
| `/cpu?iters=10000` | LuaJIT | 145.9–161.8 | 49.705–53.711 | 82.497–83.636 |
| `/cpu?iters=10000` | V8 | 2,766.1–2,804.9 | 1.984–2.122 | 3.878–5.039 |

The shared workerd process was ready for both endpoints in 0.060–0.338 seconds across the two runs. The measured host and toolchain were:

- Apple M4 Pro, 12 logical CPUs; macOS 26.6.2 (build 25G83).
- Bazel 9.2.0, `--config=benchmark`, Apple Clang 21.0.0 (`clang-2100.3.34.2`).
- Python 3.14.8 driver; LuaJIT 2.1.0-beta3 and V8 15.4.80.5 as pinned in this checkout.
- The build command used the raw GitHub Bazel Central Registry mirror because this host's JVM trust store could not validate `https://bcr.bazel.build/`:

  ```sh
  bazel build --config=benchmark \
    --registry=https://raw.githubusercontent.com/bazelbuild/bazel-central-registry/main \
    //src/workerd/server:workerd
  python3 samples/lua-v8-benchmark/benchmark.py \
    --workerd bazel-bin/src/workerd/server/workerd
  ```

Across these two runs, the short `/hello` workload had higher measured LuaJIT throughput in this local setup, while V8 handled this particular 10,000-iteration synchronous arithmetic loop at a much higher throughput. These are measurements of these handlers, this workerd build, and this local Python/HTTP setup—not a general ranking of LuaJIT and V8. Latencies include client-side timing, loopback HTTP, and any queueing behind synchronous handlers; the client, CPU frequency/thermal behavior, and other host activity can affect the results. The sample makes no security or sandbox comparison.
