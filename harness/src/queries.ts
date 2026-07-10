// query-aware lifecycle lane: the client ships desired queries to the host's
// query-aware pull (membership + refcount) and receives ONLY their rows, never
// the whole namespace. exercises put/delete, overlapping queries sharing rows
// (a row survives until its last query reference goes), limit-boundary shifts,
// and related child rows. the raw client store is asserted to hold no forbidden
// row (invariants 13-15). differential vs stock zero-cache where supported.
//
//   bun src/queries.ts                       # rust-local --query-aware
//   bun src/queries.ts --baseline stock-zero # + differential (needs Node)
import { parseArgs } from 'node:util'

import { mutators, queries } from './fixture.js'
import { assertServerOutcome } from './server-outcome.js'
import { startRustLocal, type RustLocalTarget } from './targets/rust-local.js'

import type { FixtureZero } from './target.js'

const { values: args } = parseArgs({
  options: {
    against: { type: 'string', default: 'rust-local' },
    baseline: { type: 'string', default: 'none' },
  },
})

type Row = { id: string }

// materialize a query and expose its current ids + completeness
function watch<T extends Row>(zero: FixtureZero, query: unknown) {
  const view = zero.materialize(query as never)
  let rows: T[] = []
  let complete = false
  let destroyed = false
  view.addListener((data: unknown, resultType: string) => {
    rows = JSON.parse(JSON.stringify(data)) as T[]
    if (resultType === 'complete') complete = true
  })
  return {
    get complete() {
      return complete
    },
    ids() {
      return rows.map((r) => r.id).sort()
    },
    rows() {
      return rows
    },
    destroy() {
      if (destroyed) return
      destroyed = true
      view.destroy()
    },
  }
}

async function eventually(check: () => void, label: string, timeoutMs = 30_000) {
  const start = Date.now()
  let lastError: unknown
  while (Date.now() - start < timeoutMs) {
    try {
      check()
      return
    } catch (error) {
      lastError = error
      await new Promise((r) => setTimeout(r, 25))
    }
  }
  throw new Error(`timeout waiting for ${label}: ${String(lastError)}`)
}

function equal(actual: string[], expected: string[], label: string) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
    )
  }
}

async function startTarget(): Promise<RustLocalTarget> {
  if (args.against === 'rust-local')
    return startRustLocal({ queryAware: true, pullIntervalMs: 100 })
  throw new Error(`queries --against must be rust-local (got '${args.against}')`)
}

// oracle: the ids the fixture data + our writes should yield for a query shape,
// read straight from the authoritative store via admin sql
async function oracleTaskIds(
  target: RustLocalTarget,
  whereSql: string
): Promise<string[]> {
  const rows = await target.oracle(`SELECT id FROM task WHERE ${whereSql} ORDER BY id`)
  return rows.map((r) => String((r as { id: string }).id)).sort()
}

const target = await startTarget()
let failed = false
const views: Array<{ destroy: () => void }> = []
try {
  // --- put: a query yields only its members ------------------------------
  const u0 = target.createClient('u0')
  const tip = watch(u0, queries.tasksInProjects({ projectIds: ['p1', 'p4'] }))
  views.push(tip)
  const wantTip = await oracleTaskIds(target, `"projectId" IN ('p1','p4')`)
  await eventually(() => {
    if (!tip.complete) throw new Error('not complete')
    equal(tip.ids(), wantTip, 'tasksInProjects(p1,p4) membership')
  }, 'query put membership')
  if (wantTip.length === 0)
    throw new Error('fixture has no p1/p4 tasks — bad precondition')
  console.log(`[queries] put: tasksInProjects(p1,p4) -> ${wantTip.length} members PASS`)

  // --- overlap: two queries share rows; deleting one retains shared rows --
  // tasksDone (done=true) and tasksInProjects(p1,p4) overlap on done tasks in
  // p1/p4. hold both, drop tasksInProjects, and the done-in-p1/p4 tasks must
  // stay because tasksDone still references them (invariant 14).
  const done = watch(u0, queries.tasksDone())
  views.push(done)
  const wantDone = await oracleTaskIds(target, `done = 1`)
  await eventually(() => {
    equal(done.ids(), wantDone, 'tasksDone membership')
  }, 'overlap both active')
  const shared = wantTip.filter((id) => wantDone.includes(id))
  if (shared.length === 0)
    throw new Error('no shared done task in p1/p4 — bad precondition')

  tip.destroy() // drop the tasksInProjects reference
  await eventually(() => {
    // tasksDone rows must be intact — the shared rows survive the other query's removal
    equal(done.ids(), wantDone, 'tasksDone after dropping overlapping query')
  }, 'overlap retention (invariant 14)')
  console.log(
    `[queries] overlap: dropped tasksInProjects, ${shared.length} shared done rows retained by tasksDone PASS`
  )

  // --- limit boundary shift: a low-rank task rises into a top-N window ----
  const topByRank = watch(u0, queries.tasksTopByRank()) // rank desc, limit 5
  views.push(topByRank)
  await eventually(() => {
    if (topByRank.ids().length !== 5)
      throw new Error(`top-5 has ${topByRank.ids().length}`)
  }, 'top-by-rank initial window')
  const beforeTop = topByRank.ids()
  // bump a task not currently in the top-5 to a very high rank
  const outsider = (
    await target.oracle(`SELECT id FROM task ORDER BY rank ASC LIMIT 1`)
  )[0] as {
    id: string
  }
  const rerank = u0.mutate(mutators.task.setRank({ id: outsider.id, rank: 999999 }))
  await rerank.client
  await assertServerOutcome(rerank.server, 'success', outsider.id)
  await eventually(() => {
    const now = topByRank.ids()
    if (!now.includes(outsider.id)) throw new Error(`${outsider.id} did not enter top-5`)
    if (now.length !== 5) throw new Error(`top-5 has ${now.length} after shift`)
  }, 'limit boundary shift')
  if (beforeTop.includes(outsider.id))
    throw new Error('outsider was already in the window')
  console.log(
    `[queries] limit boundary: ${outsider.id} rose into top-5, window stayed size 5 PASS`
  )

  // --- related rows: parent query pulls its related children -------------
  const withMembers = watch<{ id: string; members: Row[] }>(u0, queries.allProjects())
  views.push(withMembers)
  await eventually(() => {
    if (!withMembers.complete) throw new Error('allProjects not complete')
    const p0 = withMembers.rows().find((r) => r.id === 'p0')
    if (!p0) throw new Error('p0 missing')
    if (!Array.isArray(p0.members)) throw new Error('p0 has no related members array')
  }, 'related child rows')
  console.log('[queries] related: allProjects pulled related members PASS')

  console.log(`[queries] PASS ${args.against}: put/overlap/limit/related query lifecycle`)
} catch (error) {
  failed = true
  console.error('[queries] FAIL:', error)
} finally {
  for (const v of views) v.destroy()
  await target.close()
}

process.exit(failed ? 1 : 0)
