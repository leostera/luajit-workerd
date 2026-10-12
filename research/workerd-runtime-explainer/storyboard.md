# How workerd works—and how LuaJIT fits

## Viewer and format

- **Audience:** Developers familiar with HTTP and basic server/runtime concepts, but new to workerd.
- **Question:** How does one request move through workerd, and what changed when an experimental LuaJIT worker was added?
- **Format:** 16:9, 1920×1080, 30 fps; eight scenes at 720 frames each (3:12 total).
- **Preview:** The current visual cut is silent. The Gemini-drafted narration below is a recording script, not yet generated audio.
- **Running example:** `GET /hello` travels from a configured listener through a named service to a `fetch(request)` handler and back.
- **Estimated spoken duration:** The Gemini-drafted narration is 357 words, about 2:39 at 135 words per minute, leaving approximately 33 seconds for scene holds and transitions. Validate against synthesized speech before timing the final voice-over; no audio is included in the current preview.
- **Visual direction:** Adapt the supplied reference’s pure-black negative space, restrained italic headings, and fine vector diagrams without copying its frames or assets. Use warm-white labels, ice-blue request paths, amber timer waits, and green responses. Keep the listener → service → runtime continuity across scenes.

## Story beats

### 1. One request, two possible runtimes

- **Viewer question:** What are we following?
- **Narration:** A browser sends a GET request to the slash hello path. The interesting question is where that request travels through workerd, and where a second runtime can fit without replacing the first.
- **Visual action:** A single blue request token enters from a client and pauses before the workerd boundary. Two faint runtime paths appear ahead: V8 and LuaJIT.
- **Evidence or caveat:** Frame LuaJIT as additive and experimental, not a V8 replacement.
- **Transition:** Pull back from the request to reveal the configured server path.

### 2. What is workerd doing?

- **Viewer question:** Is workerd only a language VM?
- **Narration:** workerd is the open-source server runtime behind the Workers programming model. Its configuration connects listening sockets to named services. Our hello request reaches a listener, follows that route, and enters a worker handler. The language runtime is only one part of the server.
- **Visual action:** The request crosses a socket boundary, follows a visible config route into a named service, then stops at a worker boundary.
- **Evidence or caveat:** Config schema defines sockets and their service designator; `Server::Service::startRequest()` creates a per-request `WorkerInterface`.
- **Transition:** Zoom into the existing JavaScript route.

### 3. The established JavaScript path

- **Viewer question:** What did the Lua path need to preserve?
- **Narration:** For ordinary JavaScript workers, workerd uses V8. JSG, the JavaScript Glue layer, connects workerd’s JavaScript APIs to that engine. The server builds its Worker and script machinery, then dispatches requests through this established path. Lua does not imitate that deep stack.
- **Visual action:** Show the request entering the existing Worker → JSG → V8 lane; keep the socket and service positions fixed.
- **Evidence or caveat:** `makeWorkerImpl()` constructs `WorkerdApi`, `Worker::Isolate`, script, and Worker; JSG is V8-oriented. Do not suggest one V8 isolate per request.
- **Transition:** Highlight the server-side HTTP interface before runtime-specific construction.

### 4. Find the host boundary

- **Viewer question:** Where can another runtime attach?
- **Narration:** The Lua extension attaches at a narrower host boundary. For each request, the service provides a `WorkerInterface` adapter. It carries an HTTP request and returns a promise. This is not JavaScript compatibility; it is a shared place for each runtime to handle supported host requests.
- **Visual action:** Two language-specific runtime lanes converge only at the explicitly labeled HTTP `WorkerInterface` seam; show unsupported event ports dimmed.
- **Evidence or caveat:** The Lua adapter implements HTTP and explicitly rejects or omits unsupported features. `WorkerInterface` is broader than the subset Lua currently supports.
- **Transition:** Move from the seam to the `luaScript` configuration choice.

### 5. Add LuaJIT without swapping V8

- **Viewer question:** How is the runtime selected?
- **Narration:** At configuration, `luaScript` selects a Lua worker instead of the JavaScript script fields. Service construction creates one LuaJIT state for that configured service. Each request gets its own coroutine. V8 workers keep their existing path. The Lua module is evaluated once and returns a table with `fetch`.
- **Visual action:** At startup, animate config selecting the Lua service; a persistent LuaJIT state remains behind the service while per-request adapters are created at the edge.
- **Evidence or caveat:** `Server::makeWorker()` branches on the `luaScript` arm before the V8 `makeWorkerImpl()` path; configuration is experimental and unsupported settings are rejected.
- **Transition:** Let the same `/hello` token enter the HTTP adapter.

### 6. Translate the HTTP contract

- **Viewer question:** What does the Lua handler actually receive?
- **Narration:** The adapter maps the HTTP method, URL, headers, and body into a Lua request table. Lua returns status, headers, and body. Workerd validates and converts that result back into an HTTP response. The handler contract is just fetch with one request—no environment or context object.
- **Visual action:** Animate the HTTP envelope transforming into `{ method, url, headers, body }`, then into `{ status, headers, body }`, and back to the response token.
- **Evidence or caveat:** The current implementation limits request and response bodies to 1 MiB and each header list to 128 entries; header validation remains in workerd.
- **Transition:** The request asks to wait, revealing the coroutine bridge.

### 7. Yield, let the event loop work, resume

- **Viewer question:** How does Lua wait without blocking every request?
- **Narration:** Each fetch runs in its own Lua coroutine. Calling `workerd.sleep` asks the host for a timer. Lua suspends, and the event loop can run another request. When the timer finishes, the coroutine resumes. Because globals belong to the shared service state, requests can interleave changes to the same Lua state at yield points.
- **Visual action:** The `/hello` coroutine pauses at `sleep`; an amber timer advances while a second blue request passes; the first coroutine resumes and emits green response.
- **Evidence or caveat:** The implemented async host operation is only `workerd.sleep(milliseconds)`. Ordinary synchronous Lua still runs on the event-loop thread and can block it.
- **Transition:** Pull back to show the small supported surface and its limits.

### 8. What this extension does—and does not—promise

- **Viewer question:** When is this useful, and what remains unfinished?
- **Narration:** This is an additive, HTTP-only experiment—not a replacement for V8 and not a sandbox. It lacks bindings and broader event support. Lua’s standard libraries expose operating-system and native-library access, and there are no CPU or memory limits. Use trusted scripts only; a coroutine is not a security boundary.
- **Visual action:** V8 and LuaJIT remain side by side behind the server seam. Place a clear “experimental / trusted code” boundary around Lua and fade unsupported APIs outside it.
- **Evidence or caveat:** `docs/reference/lua-runtime.md` documents unsupported features and the absence of sandbox/resource controls. Do not equate a coroutine with a security or CPU-isolation boundary.
- **Transition:** Hold on the takeaway: same server path, explicit runtime choice, deliberately limited Lua contract.

## Claim sources

- `README.md`: workerd’s server-first design, JavaScript/Wasm role, configuration model, and warning that workerd alone is not a hardened sandbox.
- `src/workerd/server/workerd.capnp`: sockets map to named services; worker configuration includes the `luaScript` union arm.
- `src/workerd/server/server.c++`: Lua-vs-V8 factory branch, `LuaWorkerService`, and per-request `startRequest()` seam.
- `src/workerd/io/worker-interface.h`: HTTP request interface and `kj::Promise` contract.
- `src/workerd/rt/luajit/lua-runtime.c++`: module loading, per-service Lua state, coroutine creation/resume, `workerd.sleep`, and HTTP conversions.
- `src/workerd/rt/luajit/lua-runtime-test.c++` and `src/workerd/server/server-test.c++`: tests for async yielding, interleaving requests, and server-timer integration.
- `docs/reference/lua-runtime.md`: current experimental feature scope, request/response limits, and sandbox/resource caveats.
