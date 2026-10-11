#!/usr/bin/env python3
"""Compare LuaJIT and V8 HTTP handlers through one workerd process."""

import argparse
import concurrent.futures
import http.client
import math
import pathlib
import statistics
import subprocess
import tempfile
import threading
import time
from collections import defaultdict
from dataclasses import dataclass

HELLO_BODY = b"hello\n"
MODULUS = 1_000_000_007


def expected_body(path: str) -> bytes:
    if path == "/hello":
        return HELLO_BODY
    iterations = int(path.split("iters=", 1)[1])
    result = 0
    for i in range(1, iterations + 1):
        result = (result + i * 17) % MODULUS
    return f'{{"iters":{iterations},"result":{result}}}'.encode()


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


@dataclass(frozen=True)
class LoadTarget:
    host: str
    port: int
    path: str
    expected_content_type: str
    expected_body: bytes


def load_for(
    target: LoadTarget, concurrency: int, duration: float, record: bool
) -> tuple[int, float, list[float]]:
    stop = threading.Event()
    gate = threading.Barrier(concurrency + 1)

    def client() -> tuple[int, list[float]]:
        count = 0
        latencies = []
        connection = http.client.HTTPConnection(target.host, target.port, timeout=15)
        try:
            gate.wait()
            while not stop.is_set():
                started = time.perf_counter()
                connection.request("GET", target.path, headers={"Host": "benchmark"})
                response = connection.getresponse()
                body = response.read()
                elapsed = time.perf_counter() - started
                content_type = response.getheader("content-type")
                if (
                    response.status != 200
                    or content_type != target.expected_content_type
                    or body != target.expected_body
                ):
                    raise RuntimeError(
                        f"unexpected response for {target.path}: status={response.status}, "
                        f"content-type={content_type!r}, body={body!r}"
                    )
                count += 1
                if record:
                    latencies.append(elapsed)
        finally:
            connection.close()
        return count, latencies

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(client) for _ in range(concurrency)]
        gate.wait()
        started = time.perf_counter()
        time.sleep(duration)
        stop.set()
        elapsed = time.perf_counter() - started
        results = [future.result() for future in futures]

    count = sum(item[0] for item in results)
    latencies = [latency for item in results for latency in item[1]]
    return count, elapsed, latencies


def read_server_log(log_file) -> str:
    log_file.flush()
    log_file.seek(0)
    return log_file.read().decode(errors="replace")


def wait_ready(
    process: subprocess.Popen, log_file, host: str, port: int, timeout: float
) -> float:
    started = time.perf_counter()
    deadline = started + timeout
    while time.perf_counter() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"workerd exited during startup:\n{read_server_log(log_file)}"
            )
        try:
            connection = http.client.HTTPConnection(host, port, timeout=1)
            connection.request("GET", "/hello", headers={"Host": "benchmark"})
            response = connection.getresponse()
            body = response.read()
            connection.close()
            if (
                response.status == 200
                and response.getheader("content-type") == "text/plain"
                and body == HELLO_BODY
            ):
                return time.perf_counter() - started
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"workerd did not become ready:\n{read_server_log(log_file)}")


def stop_server(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workerd", type=pathlib.Path, required=True)
    parser.add_argument("--lua-port", type=int, default=18080)
    parser.add_argument("--v8-port", type=int, default=18081)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=10000)
    args = parser.parse_args()

    if args.concurrency < 1 or args.rounds < 1 or args.duration <= 0 or args.warmup < 0:
        parser.error(
            "concurrency/rounds must be positive; duration must be positive; warmup nonnegative"
        )
    if not 1 <= args.iterations <= 1_000_000:
        parser.error("iterations must be between 1 and 1,000,000")
    if args.lua_port == args.v8_port:
        parser.error("Lua and V8 listeners need different ports")

    host = "127.0.0.1"
    sample_dir = pathlib.Path(__file__).resolve().parent
    config = sample_dir / "config.capnp"
    binary = args.workerd.resolve()
    if not binary.is_file():
        parser.error(f"workerd binary does not exist: {binary}")

    command = [
        str(binary),
        "serve",
        str(config),
        "--experimental",
        "--socket-addr",
        f"lua-http={host}:{args.lua_port}",
        "--socket-addr",
        f"v8-http={host}:{args.v8_port}",
    ]
    endpoints = [("LuaJIT", args.lua_port), ("V8", args.v8_port)]
    paths = ["/hello", f"/cpu?iters={args.iterations}"]
    measurements: dict[tuple[str, str], list[tuple[float, list[float]]]] = defaultdict(
        list
    )

    with tempfile.TemporaryFile() as log_file:
        process_started = time.perf_counter()
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=log_file)
        try:
            wait_ready(process, log_file, host, args.lua_port, timeout=20)
            wait_ready(process, log_file, host, args.v8_port, timeout=20)
            startup = time.perf_counter() - process_started
            print(f"shared workerd process ready for both endpoints in {startup:.3f}s")

            for round_index in range(args.rounds):
                ordered_endpoints = (
                    endpoints if round_index % 2 == 0 else list(reversed(endpoints))
                )
                for backend, port in ordered_endpoints:
                    for path in paths:
                        content_type = (
                            "text/plain" if path == "/hello" else "application/json"
                        )
                        target = LoadTarget(
                            host, port, path, content_type, expected_body(path)
                        )
                        if args.warmup:
                            load_for(target, args.concurrency, args.warmup, False)
                        count, elapsed, latencies = load_for(
                            target, args.concurrency, args.duration, True
                        )
                        measurements[(backend, path)].append(
                            (count / elapsed, latencies)
                        )
        finally:
            stop_server(process)

    print("endpoint,backend,requests_per_second,p50_ms,p95_ms")
    for path in paths:
        for backend, _ in endpoints:
            rounds = measurements[(backend, path)]
            throughputs = [item[0] for item in rounds]
            latencies = [latency for item in rounds for latency in item[1]]
            print(
                f"{path},{backend},{statistics.median(throughputs):.1f},"
                f"{percentile(latencies, 0.50) * 1000:.3f},"
                f"{percentile(latencies, 0.95) * 1000:.3f}"
            )


if __name__ == "__main__":
    main()
