import { env as processEnv } from 'node:process';
import { cloudflare } from '@cloudflare/vite-plugin';
import { defineConfig } from 'vite';

// Use the cf CLI profile store for remote bindings when cf dev delegates to Vite.
processEnv.CLOUDFLARE_CF_AUTH ??= 'true';

export default defineConfig({
  plugins: [cloudflare()],
  server: {
    host: '127.0.0.1',
    port: 18788,
    strictPort: true,
    allowedHosts: ['d1', 'workers-binding.ai'],
  },
});
