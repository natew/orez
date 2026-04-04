/**
 * benchmark: proxy throughput
 *
 * measures raw query throughput through the TCP proxy pipeline:
 *   TCP socket → main thread proxy → worker thread → PGlite WASM → back
 *
 * tests both serial and concurrent query patterns to expose
 * single-thread bottlenecks and mutex contention.
 *
 * run: bun src/bench/proxy-throughput.bench.ts
 */

import { createConnection, type Socket } from 'node:net'

import { startZeroLite } from '../index.js'

import type { PGlite } from '@electric-sql/pglite'

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

// --- wire protocol helpers ---

function buildSimpleQuery(sql: string): Uint8Array {
  const queryBytes = textEncoder.encode(sql + '\0')
  const buf = new Uint8Array(5 + queryBytes.length)
  buf[0] = 0x51 // 'Q'
  new DataView(buf.buffer).setInt32(1, 4 + queryBytes.length)
  buf.set(queryBytes, 5)
  return buf
}

function buildStartupMessage(user: string, database: string): Buffer {
  const params = `user\0${user}\0database\0${database}\0\0`
  const paramsBytes = Buffer.from(params, 'utf-8')
  const len = 4 + 4 + paramsBytes.length // length + protocol version + params
  const buf = Buffer.alloc(len)
  buf.writeInt32BE(len, 0)
  buf.writeInt32BE(196608, 4) // protocol 3.0
  paramsBytes.copy(buf, 8)
  return buf
}

function buildPasswordMessage(password: string): Buffer {
  const passBytes = Buffer.from(password + '\0', 'utf-8')
  const len = 4 + passBytes.length
  const buf = Buffer.alloc(1 + len)
  buf[0] = 0x70 // 'p'
  buf.writeInt32BE(len, 1)
  passBytes.copy(buf, 5)
  return buf
}

// read messages from socket until we get ReadyForQuery (0x5a)
function readUntilReady(socket: Socket): Promise<Buffer[]> {
  return new Promise((resolve, reject) => {
    const messages: Buffer[] = []
    let buffer = Buffer.alloc(0)
    const timeout = setTimeout(
      () => reject(new Error('timeout waiting for ReadyForQuery')),
      10000
    )

    const onData = (data: Buffer) => {
      buffer = Buffer.concat([buffer, data])
      // parse complete messages
      while (buffer.length >= 5) {
        const msgType = buffer[0]
        const msgLen = buffer.readInt32BE(1)
        const totalLen = 1 + msgLen
        if (buffer.length < totalLen) break
        const msg = buffer.subarray(0, totalLen)
        messages.push(Buffer.from(msg))
        buffer = buffer.subarray(totalLen)
        if (msgType === 0x5a) {
          // ReadyForQuery
          clearTimeout(timeout)
          socket.off('data', onData)
          resolve(messages)
          return
        }
      }
    }
    socket.on('data', onData)
    socket.on('error', (err) => {
      clearTimeout(timeout)
      reject(err)
    })
  })
}

async function connectPg(
  port: number,
  user: string,
  password: string,
  database = 'postgres'
): Promise<Socket> {
  const socket = await new Promise<Socket>((resolve, reject) => {
    const s = createConnection({ host: '127.0.0.1', port }, () => resolve(s))
    s.setMaxListeners(0) // suppress MaxListenersExceeded warnings for benchmarks
    s.on('error', reject)
  })
  socket.setNoDelay(true)

  // startup
  socket.write(buildStartupMessage(user, database))

  // wait for auth request
  await new Promise<void>((resolve) => {
    const onData = (data: Buffer) => {
      if (data[0] === 0x52) {
        // AuthenticationCleartextPassword
        socket.off('data', onData)
        resolve()
      }
    }
    socket.on('data', onData)
  })

  // send password
  socket.write(buildPasswordMessage(password))

  // wait for ReadyForQuery
  await readUntilReady(socket)
  return socket
}

async function sendQuery(
  socket: Socket,
  sql: string
): Promise<{ messages: Buffer[]; elapsed: number }> {
  const t0 = performance.now()
  socket.write(buildSimpleQuery(sql))
  const messages = await readUntilReady(socket)
  return { messages, elapsed: performance.now() - t0 }
}

// --- benchmarks ---

interface BenchResult {
  name: string
  ops: number
  totalMs: number
  opsPerSec: number
  avgLatencyMs: number
  p50Ms: number
  p99Ms: number
}

function percentile(sorted: number[], p: number): number {
  const idx = Math.ceil(sorted.length * p) - 1
  return sorted[Math.max(0, idx)]
}

function formatResult(r: BenchResult) {
  return [
    `  ${r.name}`,
    `    ops:        ${r.ops}`,
    `    total:      ${r.totalMs.toFixed(1)}ms`,
    `    throughput:  ${r.opsPerSec.toFixed(0)} ops/sec`,
    `    avg latency: ${r.avgLatencyMs.toFixed(2)}ms`,
    `    p50:        ${r.p50Ms.toFixed(2)}ms`,
    `    p99:        ${r.p99Ms.toFixed(2)}ms`,
  ].join('\n')
}

async function benchSerial(
  socket: Socket,
  sql: string,
  ops: number,
  name: string
): Promise<BenchResult> {
  const latencies: number[] = []
  const t0 = performance.now()
  for (let i = 0; i < ops; i++) {
    const { elapsed } = await sendQuery(socket, sql)
    latencies.push(elapsed)
  }
  const totalMs = performance.now() - t0
  latencies.sort((a, b) => a - b)
  return {
    name,
    ops,
    totalMs,
    opsPerSec: (ops / totalMs) * 1000,
    avgLatencyMs: totalMs / ops,
    p50Ms: percentile(latencies, 0.5),
    p99Ms: percentile(latencies, 0.99),
  }
}

async function benchConcurrent(
  sockets: Socket[],
  sql: string,
  opsPerSocket: number,
  name: string
): Promise<BenchResult> {
  const allLatencies: number[] = []
  const t0 = performance.now()
  await Promise.all(
    sockets.map(async (socket) => {
      for (let i = 0; i < opsPerSocket; i++) {
        const { elapsed } = await sendQuery(socket, sql)
        allLatencies.push(elapsed)
      }
    })
  )
  const totalMs = performance.now() - t0
  const ops = sockets.length * opsPerSocket
  allLatencies.sort((a, b) => a - b)
  return {
    name,
    ops,
    totalMs,
    opsPerSec: (ops / totalMs) * 1000,
    avgLatencyMs: allLatencies.reduce((s, v) => s + v, 0) / ops,
    p50Ms: percentile(allLatencies, 0.5),
    p99Ms: percentile(allLatencies, 0.99),
  }
}

async function run() {
  console.log('\n=== Proxy Throughput Benchmark ===\n')

  const pgPort = 25000 + Math.floor(Math.random() * 1000)
  const zeroPort = pgPort + 100
  const dataDir = `.orez-bench-proxy-${Date.now()}`

  console.log(`starting orez (pg:${pgPort}, skipZero)...`)
  const result = await startZeroLite({
    pgPort,
    zeroPort,
    dataDir,
    logLevel: 'error',
    skipZeroCache: true, // pure proxy benchmark, no zero-cache overhead
  })

  const db = result.db as PGlite
  const user = 'user'
  const password = 'password'

  try {
    // set up test table
    await db.exec(`
      CREATE TABLE bench_rows (
        id SERIAL PRIMARY KEY,
        value TEXT,
        num INTEGER
      );
      INSERT INTO bench_rows (value, num)
      SELECT 'row-' || i, i FROM generate_series(1, 1000) AS i;
    `)

    // warmup
    const warmupSocket = await connectPg(pgPort, user, password)
    for (let i = 0; i < 50; i++) {
      await sendQuery(warmupSocket, 'SELECT 1')
    }
    warmupSocket.destroy()

    const results: BenchResult[] = []

    // --- serial benchmarks (1 connection) ---
    console.log('running serial benchmarks...')
    const s1 = await connectPg(pgPort, user, password)

    results.push(await benchSerial(s1, 'SELECT 1', 500, 'serial: SELECT 1 (ping)'))
    results.push(
      await benchSerial(
        s1,
        'SELECT * FROM bench_rows LIMIT 10',
        500,
        'serial: SELECT 10 rows'
      )
    )
    results.push(
      await benchSerial(s1, 'SELECT * FROM bench_rows', 200, 'serial: SELECT 1000 rows')
    )
    results.push(
      await benchSerial(
        s1,
        "INSERT INTO bench_rows (value, num) VALUES ('x', 1)",
        200,
        'serial: INSERT'
      )
    )

    s1.destroy()

    // --- concurrent benchmarks (multiple connections, same db) ---
    console.log('running concurrent benchmarks...')
    const concSockets: Socket[] = []
    for (let i = 0; i < 4; i++) {
      concSockets.push(await connectPg(pgPort, user, password))
    }

    results.push(
      await benchConcurrent(concSockets, 'SELECT 1', 200, 'concurrent 4x: SELECT 1')
    )
    results.push(
      await benchConcurrent(
        concSockets,
        'SELECT * FROM bench_rows LIMIT 10',
        200,
        'concurrent 4x: SELECT 10 rows'
      )
    )

    for (const s of concSockets) s.destroy()

    // --- report ---
    console.log('\n=== Results ===\n')
    for (const r of results) {
      console.log(formatResult(r))
      console.log()
    }

    // summary: serial vs concurrent throughput ratio
    const serialPing = results.find((r) => r.name.includes('serial: SELECT 1'))!
    const concPing = results.find((r) => r.name.includes('concurrent 4x: SELECT 1'))!
    const serialReal = results.find((r) => r.name.includes('serial: SELECT 10'))!
    const concReal = results.find((r) => r.name.includes('concurrent 4x: SELECT 10'))!
    const pingRatio = concPing.opsPerSec / serialPing.opsPerSec
    const realRatio = concReal.opsPerSec / serialReal.opsPerSec
    console.log(`  === scaling analysis (ideal = 4.0x with 4 connections) ===`)
    console.log(
      `  ping (no mutex/pglite):  ${pingRatio.toFixed(2)}x  ← main thread parallelism`
    )
    console.log(`  real queries (mutex):    ${realRatio.toFixed(2)}x  ← bottleneck here`)
    console.log()
  } finally {
    await result.stop()
    const { rmSync } = await import('node:fs')
    try {
      rmSync(dataDir, { recursive: true, force: true })
    } catch {}
  }
}

run().catch((err) => {
  console.error('benchmark failed:', err)
  process.exit(1)
})
