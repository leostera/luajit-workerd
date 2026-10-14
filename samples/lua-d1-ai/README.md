# LuaJIT D1 + Workers AI bridge

This sample reads a prompt from D1 and sends it to `@cf/meta/llama-3.1-8b-instruct-fast` through the Lua binding adapters. `samples/lua-d1-ai/bridge` is a local Cloudflare Worker project that exposes the adapter's D1 and AI service routes.

## Run with your Cloudflare account

`cf dev` runs the bridge locally. Its D1 and AI bindings are configured with `remote: true`, so requests go to the account pinned in `bridge/cloudflare.config.ts`. The Worker itself is not deployed by `cf dev`.

1. Authenticate the Cloudflare CLI for the account pinned in `bridge/cloudflare.config.ts` if you have not already:

   ```sh
   cd samples/lua-d1-ai/bridge
   cf auth login
   cf auth activate work
   ```

   `cf auth activate work` binds the named `work` profile to this project directory. Use your profile name if it differs. The Vite config selects the `cf` authentication store for the remote bindings.

2. Make sure that account has a D1 database named `lua-d1-ai-dev`:

   ```sh
   cf d1 list
   ```

   If it does not exist, create it:

   ```sh
   cf d1 create --name lua-d1-ai-dev
   ```

   Set the `id` in `bridge/cloudflare.config.ts` to that database's UUID. The UUID is required for a remote D1 binding. Use it to create and seed the table:

   ```sh
   cf d1 query <database-uuid> --sql "CREATE TABLE IF NOT EXISTS prompts (id TEXT PRIMARY KEY, answer TEXT NOT NULL)"
   cf d1 query <database-uuid> --sql "INSERT OR REPLACE INTO prompts (id, answer) VALUES ('welcome', 'Explain how Lua, D1, and Workers AI are connected in this local test.')"
   ```

3. Start the local bridge from the `bridge` directory:

   ```sh
   cf dev
   ```

   The Vite server listens on `127.0.0.1:18788`, matching the workerd bridge address. Stop any older Wrangler dev process using that port first.

4. In another terminal, from the repository root, build and start the experimental Lua worker:

   ```sh
   bazel build //src/workerd/server:workerd
   bazel-bin/src/workerd/server/workerd serve samples/lua-d1-ai/config.capnp --experimental
   ```

5. Send a request:

   ```sh
   curl http://127.0.0.1:8080/
   ```

D1 queries and writes go to the configured account database. The model is hosted by Workers AI, so this request does not require AI Gateway Unified Billing. Workers AI includes a free daily neuron allocation; usage beyond it is billed at the model's listed rate. See [Workers AI pricing](https://developers.cloudflare.com/workers-ai/platform/pricing/). This sample is for local development: the bridge routes have no authentication and should not be deployed publicly as-is. `cf deploy` is not needed to run this setup.

## Run without Cloudflare account resources

Use `config-mock.capnp` for a standalone test. Its mock D1 service returns a fixed prompt and its mock AI service returns a fixed response; neither persists data nor performs inference.

```sh
bazel-bin/src/workerd/server/workerd serve samples/lua-d1-ai/config-mock.capnp --experimental
```

In another terminal:

```sh
curl http://127.0.0.1:8080/
```

The `lua-v8-benchmark` sample has no D1 or AI bindings, so it does not need this bridge.
