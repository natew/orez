import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: false,
    environment: 'node',
    include: ['src/**/*.test.ts', 'packages/limesync/src/**/*.test.ts'],
    setupFiles: ['src/test-setup.ts'],
    exclude: ['**/demo/**', '**/node_modules/**', 'packages/limesync/src/data/**'],
    testTimeout: 15_000,
    fileParallelism: false,
    disableConsoleIntercept: true,
  },
})
