// bounded longevity soak for the native rust-local host. a fixed pool of tasks
// is seeded once, a fixed set of clients hydrates it, and a few writers push
// rank updates continuously for ~20-30 minutes. the working set is bounded on
// purpose: a soak must hold memory and pull cost flat so the ceiling catches a
// real leak rather than legitimate data growth, and so throughput does not decay
// under an ever-larger view. at every checkpoint the lane enforces hard pass/fail
// invariants, and a final convergence barrier proves zero lost writes:
//
//   - no client divergence: each checkpoint quiesces the writers and drains
//     every outstanding ack (a real barrier, not a race against in-flight
//     optimistic writes), then requires every client's (id, rank) view to equal
//     the SQL oracle exactly — so a lost or stale update is caught, not just a
//     lost row;
//   - memory ceiling: the native process RSS stays under a fixed bound (large
//     headroom over the bounded working set's footprint), so a leak trips it;
//   - watermark monotonic: the server-confirmed change-log watermark (read via a
//     raw null-cookie pull) never decreases between checkpoints;
//   - zero lost writes: after quiescing and a unique sentinel update, the oracle
//     and every client plus a fresh late client agree on every row's final rank.
//
// this is a distinct lane, not the rust-cf wasm-memory soaks (memory-soak.ts,
// push-memory-soak.ts): those measure wasm linear memory on the Durable Object
// with no clients, oracle, RSS, or watermark. the shared building blocks are
// startRustLocal, the fixture mutators/queries, and the watch/oracle/eventually
// patterns from bench.ts and state-machine.ts.
//
//   bun src/longevity.ts --target rust-local --duration-min 25
//   bun src/longevity.ts --target rust-local --duration-min 1 --checkpoint-sec 20
import { execFileSync } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { canonical } from './canonical.js'
import { mutators, queries } from './fixture.js'
import { startRustLocal } from './targets/rust-local.js'

import type { FixtureZero } from './target.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'rust-local' },
    'duration-min': { type: 'string', default: '25' },
    'checkpoint-sec': { type: 'string', default: '60' },
    clients: { type: 'string', default: '6' },
    writers: { type: 'string', default: '3' },
    pool: { type: 'string', default: '1000' }, // fixed number of seeded tasks
    rate: { type: 'string', default: '8' }, // rank updates/sec per writer
    'rss-ceiling-mb': { type: 'string', default: '400' },
  },
})

if (args.target !== 'rust-local')
  throw new Error('longevity soak target must be rust-local')
const durationMs = Math.round(Number(args['duration-min']) * 60_000)
const checkpointMs = Math.round(Number(args['checkpoint-sec']) * 1000)
const clients = Number(args.clients)
const writers = Math.min(Number(args.writers), clients)
const poolSize = Number(args.pool)
const rate = Number(args.rate)
const rssCeilingMb = Number(args['rss-ceiling-mb'])
if (
  !Number.isFinite(durationMs) ||
  durationMs < 1 ||
  !Number.isSafeInteger(checkpointMs) ||
  checkpointMs < 1 ||
  !Number.isSafeInteger(clients) ||
  clients < 1 ||
  !Number.isSafeInteger(writers) ||
  writers < 1 ||
  !Number.isSafeInteger(poolSize) ||
  poolSize < 1 ||
  !Number.isSafeInteger(rate) ||
  rate < 1 ||
  !Number.isSafeInteger(rssCeilingMb) ||
  rssCeilingMb < 1
)
  throw new Error('longevity soak requires positive numeric bounds')

// the shared project pool every client watches and every writer updates within.
const projectPool = ['p0', 'p1', 'p2', 'p3', 'p4', 'p5']
const poolIDs = Array.from({ length: poolSize }, (_, i) => `lv-${i}`)
const projectOf = (i: number) => projectPool[i % projectPool.length]!

type View = {
  snapshot(): { complete: boolean; sig: string; count: number }
  destroy(): void
}

// cheap listener: keep only id -> rank (a bounded map), never a deep copy of the
// whole row set, so the event loop is not starved as updates stream.
function watch(client: FixtureZero, projectIDs: string[]): View {
  const view = client.materialize(queries.tasksInProjects({ projectIds: projectIDs }), {
    ttl: 0,
  })
  let complete = false
  let ranks = new Map<string, number>()
  view.addListener((data, resultType) => {
    const rows = data as readonly { id: string; rank: number }[]
    const next = new Map<string, number>()
    for (const row of rows) next.set(row.id, row.rank)
    ranks = next
    if (resultType === 'complete') complete = true
  })
  return {
    snapshot: () => ({
      complete,
      count: ranks.size,
      sig: canonical([...ranks.entries()].sort(([a], [b]) => a.localeCompare(b))),
    }),
    destroy: () => view.destroy(),
  }
}

async function eventually(
  check: () => void | Promise<void>,
  label: string,
  timeoutMs = 60_000
) {
  const started = Date.now()
  let lastError: unknown
  while (Date.now() - started < timeoutMs) {
    try {
      await check()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
  }
  throw new Error(`timeout waiting for ${label}: ${String(lastError)}`)
}

function rssMb(pid: number): number {
  // ps reports RSS in KiB on macOS and Linux.
  const out = execFileSync('ps', ['-o', 'rss=', '-p', String(pid)], { encoding: 'utf8' })
  const kib = Number(out.trim())
  if (!Number.isFinite(kib) || kib <= 0)
    throw new Error(`could not read RSS for pid ${pid}`)
  return Math.round(kib / 1024)
}

const target = await startRustLocal({
  pullIntervalMs: 75,
  queryAware: true,
  retainChanges: 8,
})

// server-confirmed change-log watermark: a null-cookie snapshot pull returns the
// current watermark as its cookie. this is authority, not the client overlay.
async function servedWatermark(label: string): Promise<bigint> {
  const response = await fetch(`${target.origin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-longevity-user',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: `wm-probe-${label}`,
      clientGroupID: 'wm-probe-group',
      cookie: null,
    }),
    signal: AbortSignal.timeout(10_000),
  })
  if (!response.ok)
    throw new Error(`watermark probe (${label}) pull failed ${response.status}`)
  const body = (await response.json()) as { cookie?: number | string | null }
  return BigInt(body.cookie ?? 0)
}

// authority (id -> rank) for the pool, the oracle every client is compared to.
async function oracleSig(): Promise<{ sig: string; count: number }> {
  const quoted = projectPool.map((id) => `'${id}'`).join(',')
  const rows = (await target.oracle(
    `SELECT id, rank FROM task WHERE "projectId" IN (${quoted}) ORDER BY id`
  )) as { id: string; rank: number }[]
  return {
    count: rows.length,
    sig: canonical(
      rows.map(({ id, rank }) => [id, Number(rank)] as const)
    ),
  }
}

type Checkpoint = {
  atMs: number
  updates: number
  rssMb: number
  watermark: string
  rows: number
}

const t0 = Date.now()
const zeros: FixtureZero[] = []
const views: View[] = []
let issuedCount = 0
let ackedCount = 0
let mutationErrors = 0
const checkpoints: Checkpoint[] = []
let failed = false

try {
  // seed the fixed pool directly upstream (bypasses sync; the triggers still
  // capture each row so clients receive it on the next pull). ranks start at 0.
  for (let start = 0; start < poolSize; start += 200) {
    const end = Math.min(start + 200, poolSize)
    const valuesSql = poolIDs
      .slice(start, end)
      .map(
        (id, offset) =>
          `('${id}', '${projectOf(start + offset)}', 'seed ${id}', 0, 0, NULL, NULL)`
      )
      .join(',')
    await target.sql(
      `INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt") VALUES ${valuesSql}`
    )
  }

  for (let index = 0; index < clients; index++) {
    const zero = target.createClient(`longevity-user-${index}`)
    zeros.push(zero)
    views.push(watch(zero, projectPool))
  }
  // hydrate: every client must see the whole seeded pool.
  await eventually(() => {
    for (const view of views) {
      const got = view.snapshot()
      if (!got.complete || got.count < poolSize)
        throw new Error(`hydrating ${got.count}/${poolSize}`)
    }
  }, 'hydration', 120_000)

  const baselineRss = rssMb(target.pid)
  console.log(
    `[longevity] start pid=${target.pid} baselineRss=${baselineRss}MB ceiling=${rssCeilingMb}MB pool=${poolSize} clients=${clients} writers=${writers} rate=${rate}/s duration=${Math.round(durationMs / 60_000)}min`
  )
  if (baselineRss > rssCeilingMb)
    throw new Error(
      `baseline RSS ${baselineRss}MB already exceeds ceiling ${rssCeilingMb}MB`
    )

  // writers update ranks within the fixed pool for the whole duration. `paused`
  // lets a checkpoint quiesce the workload so its divergence check is a real
  // convergence barrier, not a race against in-flight optimistic writes.
  let stop = false
  let paused = false
  let rankSeq = 0
  const intervalMs = 1000 / rate
  const writerLoops = Array.from({ length: writers }, async (_, w) => {
    while (!stop && Date.now() - t0 < durationMs) {
      while (paused && !stop) await new Promise((r) => setTimeout(r, 20))
      if (stop || Date.now() - t0 >= durationMs) break
      const id = poolIDs[(rankSeq * writers + w) % poolSize]!
      const rank = 1 + (rankSeq % 100000)
      rankSeq++
      const started = Date.now()
      issuedCount++
      const request = zeros[w]!.mutate(mutators.task.setRank({ id, rank }))
      request.server.then(
        (result) => {
          if ((result as { type: string }).type === 'success') ackedCount++
          else mutationErrors++
        },
        () => mutationErrors++
      )
      await request.client
      const elapsed = Date.now() - started
      if (elapsed < intervalMs)
        await new Promise((r) => setTimeout(r, intervalMs - elapsed))
    }
  })

  // drain outstanding acks, then compare every client to the oracle. used by both
  // the periodic checkpoints and the final barrier.
  const drainAndCompare = async (label: string) => {
    if (mutationErrors > 0) throw new Error(`${mutationErrors} mutation server ack(s) failed`)
    await eventually(() => {
      if (ackedCount + mutationErrors < issuedCount)
        throw new Error(`draining ${issuedCount - ackedCount - mutationErrors} in-flight writes`)
    }, `${label} drain`, 60_000)
    if (mutationErrors > 0) throw new Error(`${mutationErrors} mutation server ack(s) failed`)
    let rows = 0
    await eventually(async () => {
      const oracle = await oracleSig()
      for (const [slot, view] of views.entries()) {
        const got = view.snapshot()
        if (!got.complete) throw new Error(`client ${slot} incomplete`)
        if (got.sig !== oracle.sig)
          throw new Error(`client ${slot} diverged: ${got.count} rows vs oracle ${oracle.count}`)
      }
      rows = oracle.count
    }, `${label} convergence`, 60_000)
    return rows
  }

  // checkpoint loop: enforce the invariants on a cadence until the duration ends.
  let prevWatermark = await servedWatermark('start')
  while (Date.now() - t0 < durationMs) {
    await new Promise((r) =>
      setTimeout(r, Math.min(checkpointMs, durationMs - (Date.now() - t0)))
    )
    if (Date.now() - t0 >= durationMs) break

    paused = true
    const rows = await drainAndCompare(`checkpoint at ${Math.round((Date.now() - t0) / 1000)}s`)

    const rss = rssMb(target.pid)
    if (rss > rssCeilingMb)
      throw new Error(`RSS ${rss}MB exceeded ceiling ${rssCeilingMb}MB`)

    const watermark = await servedWatermark(`cp-${checkpoints.length}`)
    if (watermark < prevWatermark)
      throw new Error(`watermark regressed ${prevWatermark} -> ${watermark}`)
    prevWatermark = watermark
    paused = false

    const checkpoint: Checkpoint = {
      atMs: Date.now() - t0,
      updates: issuedCount,
      rssMb: rss,
      watermark: watermark.toString(),
      rows,
    }
    checkpoints.push(checkpoint)
    console.log(
      `[longevity] checkpoint ${checkpoints.length}: t=${Math.round(checkpoint.atMs / 1000)}s updates=${checkpoint.updates} rss=${rss}MB watermark=${checkpoint.watermark} rows=${rows}`
    )
  }

  // stop the workload and settle before the final barrier.
  stop = true
  await Promise.all(writerLoops)

  // final barrier: a unique sentinel rank on one pool row after every tracked
  // update, then require the oracle, every client, and a fresh late client to
  // agree on every row's final rank. zero lost writes means the last acked
  // update for every row is durable and visible everywhere.
  const sentinelID = poolIDs[0]!
  const sentinelRank = 900000 + (Date.now() % 1000)
  const sentinel = zeros[0]!.mutate(mutators.task.setRank({ id: sentinelID, rank: sentinelRank }))
  const sentinelOutcome = await sentinel.server
  if ((sentinelOutcome as { type: string }).type !== 'success')
    throw new Error('sentinel mutation did not succeed')
  ackedCount++
  issuedCount++

  await drainAndCompare('final barrier')
  await eventually(async () => {
    const oracle = (await target.oracle(
      `SELECT rank FROM task WHERE id = '${sentinelID}'`
    )) as { rank: number }[]
    if (Number(oracle[0]?.rank) !== sentinelRank)
      throw new Error('sentinel rank not durable in oracle')
    for (const [slot, view] of views.entries()) {
      const sig = view.snapshot().sig
      if (!sig.includes(`["${sentinelID}",${sentinelRank}]`))
        throw new Error(`client ${slot} missing sentinel rank`)
    }
  }, 'sentinel visibility', 60_000)

  // fresh late client must equal the authority too.
  const late = target.createClient('longevity-late')
  const lateView = watch(late, projectPool)
  await eventually(async () => {
    const oracle = await oracleSig()
    const got = lateView.snapshot()
    if (!got.complete || got.count < poolSize) throw new Error(`late hydrating ${got.count}`)
    if (got.sig !== oracle.sig) throw new Error('late client diverged')
  }, 'fresh late-client equality', 60_000)
  lateView.destroy()

  const oracle = await oracleSig()
  const result = {
    lane: 'longevity-soak',
    result: 'PASS' as const,
    target: 'rust-local',
    durationMin: Math.round(durationMs / 60_000),
    poolSize,
    clients,
    writers,
    ratePerWriter: rate,
    baselineRssMb: baselineRss,
    rssCeilingMb,
    peakRssMb: Math.max(baselineRss, ...checkpoints.map((c) => c.rssMb)),
    checkpoints: checkpoints.length,
    updates: issuedCount,
    acked: ackedCount,
    oracleRows: oracle.count,
    finalWatermark: prevWatermark.toString(),
    samples: checkpoints,
  }
  const resultsDir = join(import.meta.dirname, '..', 'results')
  mkdirSync(resultsDir, { recursive: true })
  writeFileSync(join(resultsDir, 'longevity-rust-local.json'), JSON.stringify(result, null, 2))
  console.log(`[longevity] ${JSON.stringify({ ...result, samples: undefined })}`)
  console.log(
    `[longevity] PASS rust-local: ${checkpoints.length} checkpoints, ${issuedCount} updates on ${poolSize} rows, peak RSS ${result.peakRssMb}MB <= ${rssCeilingMb}MB, watermark monotonic, zero lost writes`
  )
  for (const view of views) view.destroy()
} catch (error) {
  failed = true
  console.error('[longevity] FAIL:', error)
} finally {
  await target.close()
}

process.exit(failed ? 1 : 0)
