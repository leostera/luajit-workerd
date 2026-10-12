# How workerd works—and how LuaJIT fits

A developer-facing, 16:9 explainer about the request path through workerd and the experimental additive LuaJIT handler. Its sparse black canvas, italic headings, and fine-line diagrams adapt the supplied design reference while keeping all visuals original. The Remotion project renders an original silent preview; `storyboard.md` contains scene questions, evidence, caveats, and a narration script. No voice-over or music is synthesized.

## Render

This is a standalone pnpm package and is not part of the repository's pnpm workspace.

```sh
pnpm install
pnpm typecheck
pnpm render
```

The composition is 1920×1080 at 30 fps and 3:12 long. `pnpm render` writes a silent H.264 preview to `out/workerd-runtime-explainer.mp4`; generated renders are kept local and are not tracked. Use `pnpm studio` to preview the composition.

## Factual boundaries

- The diagram distinguishes config/service dispatch from language execution.
- The existing JavaScript route is shown as Worker/Script with JSG and V8; Lua is an additive branch, not a V8 replacement.
- The runtime implements only HTTP `fetch(request)` and the current async host operation `workerd.sleep(milliseconds)`.
- Lua states are per configured service; requests use coroutines and can interleave at yield points.
- The LuaJIT runtime is experimental, opens standard libraries, and has no CPU/memory limits or sandbox. Synchronous Lua can block the event-loop thread.

These statements are grounded in `README.md`, `src/workerd/server/server.c++`, `src/workerd/io/worker-interface.h`, `src/workerd/rt/luajit/`, and `docs/reference/lua-runtime.md` in the workerd repository. The animation is conceptual and does not depict undocumented internal scheduling details.
