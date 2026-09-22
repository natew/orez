#!/usr/bin/env node

const { spawnSync } = require('node:child_process')
const command = require.resolve('./dist/index.js')
const result = spawnSync(process.execPath, [command, ...process.argv.slice(2)], {
  stdio: 'inherit',
})
process.exit(result.status ?? 1)
