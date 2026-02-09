import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: false,
    environment: 'node',
    include: ['src/**/*.test.ts'],
    exclude: ['**/demo/**', '**/node_modules/**'],
    testTimeout: 15_000,
    disableConsoleIntercept: true,
  },
})
