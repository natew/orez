import { spawnSync } from 'node:child_process'

import { cmd } from './cmd'

await cmd`ensure zero-sqlite3 native module is available`.run(() => {
  const result = spawnSync(
    'node',
    [
      '--input-type=commonjs',
      '-e',
      `const { createRequire } = require('node:module');
const { resolve } = require('node:path');
const load = createRequire(resolve('package.json'));
const Database = load('@rocicorp/zero-sqlite3');
const database = new Database(':memory:');
database.close();`,
    ],
    { stdio: 'inherit' }
  )

  if (result.error) {
    throw result.error
  }
  if (result.status !== 0) {
    throw new Error(
      'zero-sqlite3 native module unavailable; install a compatible prebuilt for the current Node runtime'
    )
  }
})
