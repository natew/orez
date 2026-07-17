// bounded longevity soak for the native rust-local host. a fixed set of clients
// hydrates a shared query and a few writers push tasks continuously for ~20-30
// minutes. at every checkpoint the lane enforces hard pass/fail invariants, and
// a final convergence barrier proves zero lost writes:
//
//   - no client divergence: every client's materialized view equals the SQL
//     oracle for the watched projects at each checkpoint;
//   - memory ceiling: the native process RSS stays under a fixed bound (large
//     headroom over the host's steady-state footprint), so a leak under
//     sustained load trips it;
//   - watermark monotonic: the server-confirmed change-log watermark (read via a
//     raw null-cookie pull) never decreases between checkpoints;
//   - zero lost writes: after quiescing, a unique sentinel commit, and a proven
//     barrier, the oracle contains every acknowledged write and every client and
//     a fresh late client converge to it.
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
    rate: { type: 'string', default: '5' }, // task creates/sec per writer
    'rss-ceiling-mb': { type: 'string', default: '400' },
  },
})

if (args.target !== 'rust-local')
  throw new Error('longevity soak target must be rust-local')
const durationMs = Math.round(Number(args['duration-min']) * 60_000)
const checkpointMs = Math.round(Number(args['checkpoint-sec']) * 1000)
const clients = Number(args.clients)
const writers = Math.min(Number(args.writers), clients)
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
  !Number.isSafeInteger(rate) ||
  rate < 1 ||
  !Number.isSafeInteger(rssCeilingMb) ||
  rssCeilingMb < 1
)
  throw new Error('longevity soak requires positive numeric bounds')

// the shared project pool every client watches and every writer writes into, so
// any row dropped from any client's incremental view diverges from the oracle.
const projectPool = ['p0', 'p1', 'p2', 'p3', 'p4', 'p5']

type View = {
  snapshot(): { complete: boolean; ids: string[] }
  destroy(): void
}

function watch(client: FixtureZero, projectIDs: string[]): View {
  const view = client.materialize(queries.tasksInProjects({ projectIds: projectIDs }), {
    ttl: 0,
  })
  let complete = false
  let rows: { id: string }[] = []
  view.addListener((data, resultType) => {
    rows = JSON.parse(JSON.stringify(data)) as { id: string }[]
    if (resultType === 'complete') complete = true
  })
  return {
    snapshot: () => ({ complete, ids: rows.map(({ id }) => id).sort() }),
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

async function oracleIDs(projectIDs: string[]) {
  const quoted = projectIDs.map((id) => `'${id}'`).join(',')
  const rows = (await target.oracle(
    `SELECT id FROM task WHERE "projectId" IN (${quoted}) ORDER BY id`
  )) as { id: string }[]
  return rows.map(({ id }) => id).sort()
}

type Checkpoint = {
  atMs: number
  writes: number
  rssMb: number
  watermark: string
  clientRows: number
}

const t0 = Date.now()
const zeros: FixtureZero[] = []
const views: View[] = []
const issued = new Set<string>()
const acked = new Set<string>()
let mutationErrors = 0
const checkpoints: Checkpoint[] = []
let failed = false

try {
  for (let index = 0; index < clients; index++) {
    const zero = target.createClient(`longevity-user-${index}`)
    zeros.push(zero)
    views.push(watch(zero, projectPool))
  }
  await eventually(() => {
    for (const view of views)
      if (!view.snapshot().complete) throw new Error('not hydrated')
  }, 'hydration')

  const baselineRss = rssMb(target.pid)
  console.log(
    `[longevity] start pid=${target.pid} baselineRss=${baselineRss}MB ceiling=${rssCeilingMb}MB clients=${clients} writers=${writers} rate=${rate}/s duration=${Math.round(durationMs / 60_000)}min`
  )
  if (baselineRss > rssCeilingMb)
    throw new Error(
      `baseline RSS ${baselineRss}MB already exceeds ceiling ${rssCeilingMb}MB`
    )

  // writers push tasks into the shared pool for the whole duration; acks are
  // tracked so the final barrier can prove every acknowledged write survived.
  // `paused` lets a checkpoint quiesce the workload so its divergence check is a
  // real convergence barrier, not a race against in-flight optimistic writes.
  let stop = false
  let paused = false
  const intervalMs = 1000 / rate
  const writerLoops = Array.from({ length: writers }, async (_, w) => {
    let seq = 0
    while (!stop && Date.now() - t0 < durationMs) {
      while (paused && !stop) await new Promise((r) => setTimeout(r, 20))
      if (stop || Date.now() - t0 >= durationMs) break
      const id = `lv-${w}-${seq++}`
      const projectID = projectPool[(w + seq) % projectPool.length]!
      const started = Date.now()
      issued.add(id)
      const request = zeros[w]!.mutate(
        mutators.task.create({
          id,
          projectId: projectID,
          title: `longevity ${id}`,
          rank: (seq % 100) + 0.5,
          done: false,
        })
      )
      request.server.then(
        (result) => {
          if ((result as { type: string }).type === 'success') acked.add(id)
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

  // checkpoint loop: enforce the invariants on a cadence until the duration ends.
  let prevWatermark = await servedWatermark('start')
  while (Date.now() - t0 < durationMs) {
    await new Promise((r) =>
      setTimeout(r, Math.min(checkpointMs, durationMs - (Date.now() - t0)))
    )
    if (Date.now() - t0 >= durationMs) break

    // no client divergence: quiesce the workload so this is a real convergence
    // barrier, not a race against in-flight optimistic writes. pause the writers,
    // drain every outstanding ack, then require every client to equal the oracle
    // exactly. a persistent gap (a lost or dropped row) fails the checkpoint.
    paused = true
    if (mutationErrors > 0)
      throw new Error(`${mutationErrors} mutation server ack(s) failed`)
    await eventually(
      () => {
        if (acked.size + mutationErrors < issued.size)
          throw new Error(
            `draining ${issued.size - acked.size - mutationErrors} in-flight writes`
          )
      },
      `checkpoint drain at ${Math.round((Date.now() - t0) / 1000)}s`,
      30_000
    )
    if (mutationErrors > 0)
      throw new Error(`${mutationErrors} mutation server ack(s) failed`)
    let clientRows = 0
    await eventually(
      async () => {
        const want = await oracleIDs(projectPool)
        for (const [slot, view] of views.entries()) {
          const got = view.snapshot()
          if (!got.complete) throw new Error(`client ${slot} incomplete`)
          if (canonical(got.ids) !== canonical(want))
            throw new Error(
              `client ${slot} diverged: ${got.ids.length} rows vs oracle ${want.length}`
            )
        }
        clientRows = want.length
      },
      `checkpoint divergence at ${Math.round((Date.now() - t0) / 1000)}s`,
      30_000
    )
    paused = false

    // memory ceiling.
    const rss = rssMb(target.pid)
    if (rss > rssCeilingMb)
      throw new Error(`RSS ${rss}MB exceeded ceiling ${rssCeilingMb}MB`)

    // watermark monotonic.
    const watermark = await servedWatermark(`cp-${checkpoints.length}`)
    if (watermark < prevWatermark)
      throw new Error(`watermark regressed ${prevWatermark} -> ${watermark}`)
    prevWatermark = watermark

    const checkpoint: Checkpoint = {
      atMs: Date.now() - t0,
      writes: issued.size,
      rssMb: rss,
      watermark: watermark.toString(),
      clientRows,
    }
    checkpoints.push(checkpoint)
    console.log(
      `[longevity] checkpoint ${checkpoints.length}: t=${Math.round(checkpoint.atMs / 1000)}s writes=${checkpoint.writes} rss=${rss}MB watermark=${checkpoint.watermark} rows=${clientRows}`
    )
  }

  // stop the workload and drain outstanding acks before the barrier.
  stop = true
  await Promise.all(writerLoops)
  if (mutationErrors > 0)
    throw new Error(`${mutationErrors} mutation server ack(s) failed`)

  // final barrier: a unique sentinel commit after every tracked write, then wait
  // until the oracle holds every acknowledged write plus the sentinel and every
  // client and a fresh late client converge to it. zero lost writes means every
  // acked id is durably present and visible everywhere.
  const sentinelID = `lv-sentinel-${Date.now()}`
  const sentinel = zeros[0]!.mutate(
    mutators.task.create({
      id: sentinelID,
      projectId: projectPool[0]!,
      title: 'longevity sentinel',
      rank: 999,
      done: true,
    })
  )
  const sentinelOutcome = await sentinel.server
  if ((sentinelOutcome as { type: string }).type !== 'success')
    throw new Error('sentinel mutation did not succeed')
  acked.add(sentinelID)

  await eventually(
    async () => {
      const oracle = new Set(await oracleIDs(projectPool))
      for (const id of acked)
        if (!oracle.has(id)) throw new Error(`lost write ${id} missing from oracle`)
      const want = [...oracle].sort()
      for (const [slot, view] of views.entries()) {
        const got = view.snapshot()
        if (!got.complete) throw new Error(`barrier: client ${slot} incomplete`)
        if (!got.ids.includes(sentinelID))
          throw new Error(`barrier: client ${slot} missing sentinel`)
        if (canonical(got.ids) !== canonical(want))
          throw new Error(`barrier: client ${slot} diverged from oracle`)
      }
    },
    'final convergence barrier',
    120_000
  )

  // fresh late client must equal the authority too.
  const late = target.createClient('longevity-late')
  const lateView = watch(late, projectPool)
  await eventually(
    async () => {
      const want = await oracleIDs(projectPool)
      const got = lateView.snapshot()
      if (!got.complete) throw new Error('late client incomplete')
      if (!got.ids.includes(sentinelID)) throw new Error('late client missing sentinel')
      if (canonical(got.ids) !== canonical(want)) throw new Error('late client diverged')
    },
    'fresh late-client equality',
    60_000
  )
  lateView.destroy()

  const oracleFinal = await oracleIDs(projectPool)
  const result = {
    lane: 'longevity-soak',
    result: 'PASS' as const,
    target: 'rust-local',
    durationMin: Math.round(durationMs / 60_000),
    clients,
    writers,
    ratePerWriter: rate,
    baselineRssMb: baselineRss,
    rssCeilingMb,
    peakRssMb: Math.max(baselineRss, ...checkpoints.map((c) => c.rssMb)),
    checkpoints: checkpoints.length,
    issued: issued.size,
    acked: acked.size,
    oracleRows: oracleFinal.length,
    finalWatermark: prevWatermark.toString(),
    samples: checkpoints,
  }
  const resultsDir = join(import.meta.dirname, '..', 'results')
  mkdirSync(resultsDir, { recursive: true })
  writeFileSync(
    join(resultsDir, 'longevity-rust-local.json'),
    JSON.stringify(result, null, 2)
  )
  console.log(`[longevity] ${JSON.stringify({ ...result, samples: undefined })}`)
  console.log(
    `[longevity] PASS rust-local: ${checkpoints.length} checkpoints, ${issued.size} writes, peak RSS ${result.peakRssMb}MB <= ${rssCeilingMb}MB, watermark monotonic, zero lost writes`
  )
  for (const view of views) view.destroy()
} catch (error) {
  failed = true
  console.error('[longevity] FAIL:', error)
} finally {
  await target.close()
}

process.exit(failed ? 1 : 0)
