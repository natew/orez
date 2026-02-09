import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: false,
    environment: 'node',
    include: ['test/**/*.test.ts'],
    testTimeout: 30_000,
    disableConsoleIntercept: true,
  },
})
