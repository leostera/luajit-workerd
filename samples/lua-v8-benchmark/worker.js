const DEFAULT_ITERATIONS = 10000;
const MAX_ITERATIONS = 1000000;
const MODULUS = 1000000007;

function runCpuLoop(iterations) {
  let result = 0;
  for (let i = 1; i <= iterations; i++) {
    result = (result + i * 17) % MODULUS;
  }
  return result;
}

export default {
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/hello") {
      return new Response("hello\n", {
        headers: { "content-type": "text/plain" },
      });
    }

    const parsedIterations = Number(url.searchParams.get("iters"));
    const iterations = Number.isInteger(parsedIterations) && parsedIterations >= 1 &&
        parsedIterations <= MAX_ITERATIONS
      ? parsedIterations
      : DEFAULT_ITERATIONS;
    const result = runCpuLoop(iterations);
    return new Response(JSON.stringify({ iters: iterations, result }), {
      headers: { "content-type": "application/json" },
    });
  },
};
