import { PGlite } from '@electric-sql/pglite'
import { createBrowserProxy } from './dist/pg-proxy-browser.js'
import { installChangeTracking } from './dist/replication/change-tracker.js'
import postgres from 'postgres'
import { createSocketFactory } from './dist/worker/shims/postgres-socket.js'
import { Buffer } from 'buffer'
globalThis.Buffer = Buffer

const db = await new PGlite('memory://')
await db.waitReady
await installChangeTracking(db)
console.log('ready')

const proxy = await createBrowserProxy(
  { postgres: db, cvr: db, cdb: db },
  { pgPassword: 'pw', pgUser: 'user' }
)

const connectFn = (port) => proxy.handleConnection(port)

// replication connection
const replSql = postgres({
  socket: createSocketFactory(connectFn),
  host: '127.0.0.1', port: 0, database: 'postgres',
  username: 'user', password: 'pw', ssl: false,
  max: 1, fetch_types: false,
  connection: { application_name: 'test', replication: 'database' },
})

console.log('querying wal_sender_timeout...')
try {
  const r = await replSql`SELECT 1 as test`.simple()
  console.log('simple query on repl conn:', r)
} catch (err) {
  console.error('FAILED:', err.message)
}

await replSql.end()
proxy.close()
await db.close()
