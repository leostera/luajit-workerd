import { bindings, defineConfig } from 'cf/config';
import * as entrypoint from './src/index.ts' with { type: 'cf-worker' };

export default defineConfig({
  accountId: '776b37f0f2474156eb01cf93c764b2e9',
  worker: {
    name: 'lua-d1-ai-bridge',
    compatibilityDate: '2026-09-30',
    entrypoint,
    env: {
      DB: bindings.d1({
        name: 'lua-d1-ai-dev',
        id: 'a84ce4cc-ae56-4fe2-8f39-c3fa8d319739',
        dev: { remote: true },
      }),
      AI: bindings.ai({ dev: { remote: true } }),
    },
  },
});
