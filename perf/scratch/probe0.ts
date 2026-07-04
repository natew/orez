import { PGlite } from '@electric-sql/pglite'

import {
  installChangeTracking,
  getChangesSince,
} from '../../src/replication/change-tracker.ts'

const db = new PGlite({ dataDir: 'memory://', relaxedDurability: true })
await db.waitReady
await db.exec('CREATE EXTENSION IF NOT EXISTS plpgsql')
await db.exec(`CREATE TABLE message (id text primary key, body text, updated_at bigint)`)
await installChangeTracking(db)
const t0 = performance.now()
await db.query(`INSERT INTO message (id, body, updated_at) VALUES ($1,$2,$3)`, [
  'a',
  'hello',
  Date.now(),
])
console.log('insert ms', (performance.now() - t0).toFixed(3))
const ch = await getChangesSince(db, 0, 1000)
console.log('changes', ch.length, JSON.stringify(ch[0]).slice(0, 120))
await db.close()
console.log('OK')
