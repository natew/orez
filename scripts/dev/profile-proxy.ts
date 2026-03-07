/**
 * profile the pg-proxy hot path.
 * connects via postgres.js (already a dep) and measures query latencies.
 *
 * usage: bun scripts/dev/profile-proxy.ts [pg-port]
 */

import postgres from 'postgres'

const port = Number(process.argv[2]) || 6032

async function profile() {
  console.log(`connecting to proxy at 127.0.0.1:${port}...`)

  const sql = postgres({
    host: '127.0.0.1',
    port,
    user: 'user',
    password: 'password',
    database: 'postgres',
    max: 1,
  })

  // warm up
  await sql`SELECT 1`
  console.log('connected\n')

  // setup
  await sql`CREATE SCHEMA IF NOT EXISTS _orez`
  await sql`CREATE TABLE IF NOT EXISTS _orez._profile_test (id serial primary key, val text)`
  await sql`DELETE FROM _orez._profile_test`

  // profile different query types
  const tests: Array<{ name: string; fn: () => Promise<unknown>; n: number }> = [
    { name: 'select-1', fn: () => sql`SELECT 1`, n: 200 },
    { name: 'pg_class-count', fn: () => sql`SELECT count(*) FROM pg_class`, n: 100 },
    {
      name: 'insert-single',
      fn: () => sql`INSERT INTO _orez._profile_test (val) VALUES ('test')`,
      n: 200,
    },
    {
      name: 'select-count',
      fn: () => sql`SELECT count(*) FROM _orez._profile_test`,
      n: 100,
    },
    {
      name: 'insert+select',
      fn: async () => {
        await sql`INSERT INTO _orez._profile_test (val) VALUES ('combo')`
        await sql`SELECT count(*) FROM _orez._profile_test`
      },
      n: 100,
    },
  ]

  for (const test of tests) {
    const times: number[] = []
    for (let i = 0; i < test.n; i++) {
      const t0 = performance.now()
      await test.fn()
      times.push(performance.now() - t0)
    }
    times.sort((a, b) => a - b)
    const avg = times.reduce((a, b) => a + b, 0) / times.length
    const p50 = times[Math.floor(times.length * 0.5)]
    const p95 = times[Math.floor(times.length * 0.95)]
    const p99 = times[Math.floor(times.length * 0.99)]
    const min = times[0]
    const max = times[times.length - 1]
    console.log(
      `${test.name.padEnd(16)} avg=${avg.toFixed(2)}ms  p50=${p50.toFixed(2)}ms  p95=${p95.toFixed(2)}ms  p99=${p99.toFixed(2)}ms  min=${min.toFixed(2)}  max=${max.toFixed(2)}  (n=${test.n})`
    )
  }

  // profile concurrent access (multiple connections)
  console.log('\n--- concurrent query throughput ---')
  const concurrencyLevels = [1, 2, 4, 8]
  for (const concurrency of concurrencyLevels) {
    const connections = Array.from({ length: concurrency }, () =>
      postgres({
        host: '127.0.0.1',
        port,
        user: 'user',
        password: 'password',
        database: 'postgres',
        max: 1,
      })
    )

    // warm up
    await Promise.all(connections.map((c) => c`SELECT 1`))

    const queriesPerConn = 50
    const t0 = performance.now()
    await Promise.all(
      connections.map(async (c) => {
        for (let i = 0; i < queriesPerConn; i++) {
          await c`SELECT 1`
        }
      })
    )
    const totalMs = performance.now() - t0
    const totalQueries = concurrency * queriesPerConn
    const qps = (totalQueries / totalMs) * 1000
    console.log(
      `  c=${concurrency}: ${totalQueries} queries in ${totalMs.toFixed(0)}ms = ${qps.toFixed(0)} q/s (${(totalMs / totalQueries).toFixed(2)}ms/q avg)`
    )

    for (const c of connections) await c.end()
  }

  // profile a realistic mutation pipeline (insert server+channel+member)
  console.log('\n--- mutation pipeline (3 inserts + signal latency) ---')
  await sql`CREATE TABLE IF NOT EXISTS _orez._profile_server (id text primary key, name text)`
  await sql`CREATE TABLE IF NOT EXISTS _orez._profile_channel (id text primary key, server_id text, name text)`
  await sql`CREATE TABLE IF NOT EXISTS _orez._profile_member (id text primary key, server_id text, user_id text)`

  const pipelineTimes: number[] = []
  for (let i = 0; i < 50; i++) {
    const id = `prof-${i}`
    const t0 = performance.now()
    await sql`INSERT INTO _orez._profile_server (id, name) VALUES (${id}, ${'test'})`
    await sql`INSERT INTO _orez._profile_channel (id, server_id, name) VALUES (${id}, ${id}, ${'general'})`
    await sql`INSERT INTO _orez._profile_member (id, server_id, user_id) VALUES (${id}, ${id}, ${'user1'})`
    pipelineTimes.push(performance.now() - t0)
  }
  pipelineTimes.sort((a, b) => a - b)
  const avg = pipelineTimes.reduce((a, b) => a + b, 0) / pipelineTimes.length
  console.log(
    `  3-insert pipeline: avg=${avg.toFixed(2)}ms  p50=${pipelineTimes[Math.floor(pipelineTimes.length * 0.5)].toFixed(2)}ms  p95=${pipelineTimes[Math.floor(pipelineTimes.length * 0.95)].toFixed(2)}ms  max=${pipelineTimes[pipelineTimes.length - 1].toFixed(2)}ms`
  )

  // cleanup
  await sql`DROP TABLE IF EXISTS _orez._profile_test, _orez._profile_server, _orez._profile_channel, _orez._profile_member`
  await sql.end()
  console.log('\ndone')
}

profile().catch(console.error)
