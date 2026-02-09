import { startZeroLite } from '../../src/index'
import { join } from 'node:path'

const lite = await startZeroLite({
  dataDir: join(import.meta.dir, '../.zero-lite'),
  pgPort: 6435,
  zeroPort: 4849,
  s3Port: 10202,
  webPort: 3456,
  migrationsDir: join(import.meta.dir, 'database/migrations'),
  seedFile: '',
  skipZeroCache: true,
})

console.info('zerolite backend running on port 6435')
console.info('press ctrl+c to stop')

process.on('SIGINT', async () => {
  await lite.stop()
  process.exit(0)
})
