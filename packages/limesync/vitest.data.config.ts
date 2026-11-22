import { defineConfig } from 'vitest/config'

import { orezSyncCfHostWasm } from './src/cloudflare-vite-wasm-loader.js'

export default defineConfig({
  plugins: [orezSyncCfHostWasm()],
  test: {
    include: ['src/data/**/*.test.{ts,tsx}'],
    setupFiles: ['./src/data/testSetup.ts'],
  },
})
