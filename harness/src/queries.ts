// query-aware lifecycle lane: the client ships desired queries to the host's
// query-aware pull (membership + refcount) and receives ONLY their rows, never
// the whole namespace. each section uses a fresh client so the raw client-store
// assertions (a forbidden row must never physically be present — invariants
// 13-15) are clean.
//
// covers: query put (members only) + forbidden-row raw store; overlapping
// queries sharing a row (drop one, the row survives via the other's reference);
// limit-boundary shift; related child rows; query delete (rows leave the raw
// store); permission expansion + contraction via membership change (a revoked
// row leaves the raw store); reconnect replaying desires.
//
//   bun src/queries.ts                    # rust-local --query-aware
//   bun src/queries.ts --against rust-cf  # sol-m0's CF host
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { mutators, queries } from './fixture.js'
import { persistentKVStoreProvider } from './persistent-kv.js'
import { rawClientIds } from './raw-store.js'
import { assertServerOutcome } from './server-outcome.js'

import type { FixtureZero, SyncTarget } from './target.js'

const { values: args } = parseArgs({
  options: { against: { type: 'string', default: 'rust-local' } },
})

type Row = { id: string }

// the query lanes need the shared fault hooks (both rust-local and rust-cf
// expose them); narrow to what this lane calls.
type QueryTarget = SyncTarget & { dropNextPushResponse(): Promise<void> }

async function startTarget(): Promise<SyncTarget> {
  if (args.against === 'rust-local') {
    return (await import('./targets/rust-local.js')).startRustLocal({
      queryAware: true,
      pullIntervalMs: 100,
    })
  }
  if (args.against === 'rust-cf') {
    return (await import('./targets/rust-cf.js')).startRustCf({
      queryAware: true,
      pullIntervalMs: 300,
    } as never)
  }
  throw new Error(
    `queries --against must be rust-local or rust-cf (got '${args.against}')`
  )
}

function watch<T extends Row>(zero: FixtureZero, query: unknown, ttl?: unknown) {
  const view =
    ttl === undefined
      ? zero.materialize(query as never)
      : zero.materialize(query as never, { ttl } as never)
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

async function eventually(
  check: () => void | Promise<void>,
  label: string,
  timeoutMs = 30_000
) {
  const start = Date.now()
  let lastError: unknown
  while (Date.now() - start < timeoutMs) {
    try {
      await check()
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

async function oracleIds(target: SyncTarget, sql: string): Promise<string[]> {
  const rows = await target.oracle(sql)
  return rows.map((r) => String((r as { id: string }).id)).sort()
}

const target = await startTarget()
const clients: FixtureZero[] = []
function client(userID: string, storage?: Parameters<SyncTarget['createClient']>[1]) {
  const zero = target.createClient(userID, storage)
  clients.push(zero)
  return zero
}

let failed = false
try {
  // --- put + forbidden-row raw store -------------------------------------
  {
    const u0 = client('u0')
    const tip = watch(u0, queries.tasksInProjects({ projectIds: ['p1', 'p4'] }))
    const want = await oracleIds(
      target,
      `SELECT id FROM task WHERE "projectId" IN ('p1','p4')`
    )
    if (want.length === 0) throw new Error('fixture has no p1/p4 tasks')
    await eventually(() => {
      if (!tip.complete) throw new Error('not complete')
      equal(tip.ids(), want, 'tasksInProjects(p1,p4) view')
    }, 'query put membership')
    // the client physically holds ONLY the query's task rows, and no rows of
    // untouched tables — no forbidden row is present even transiently.
    equal(await rawClientIds(u0, 'task'), want, 'raw task store = query members only')
    equal(
      await rawClientIds(u0, 'project'),
      [],
      'raw project store empty (task query pulls no projects)'
    )
    tip.destroy()
    console.log(
      `[queries] put + forbidden-row: ${want.length} members, raw store holds only them PASS`
    )
  }

  // --- overlap: a shared row survives dropping one of two queries --------
  {
    const u = client('u1')
    const tip = watch(u, queries.tasksInProjects({ projectIds: ['p1', 'p4'] }))
    const done = watch(u, queries.tasksDone())
    const wantDone = await oracleIds(target, `SELECT id FROM task WHERE done = 1`)
    const wantTip = await oracleIds(
      target,
      `SELECT id FROM task WHERE "projectId" IN ('p1','p4')`
    )
    await eventually(
      () => equal(done.ids(), wantDone, 'tasksDone'),
      'overlap both active'
    )
    const shared = wantTip.filter((id) => wantDone.includes(id))
    if (shared.length === 0) throw new Error('no shared done task in p1/p4')
    tip.destroy() // drop the tasksInProjects reference
    await eventually(
      () => equal(done.ids(), wantDone, 'tasksDone after drop'),
      'overlap retention (invariant 14)'
    )
    // the shared rows are still physically present (referenced by tasksDone)
    for (const id of shared) {
      if (!(await rawClientIds(u, 'task')).includes(id)) {
        throw new Error(
          `shared row ${id} left the raw store after dropping the other query`
        )
      }
    }
    done.destroy()
    console.log(
      `[queries] overlap: ${shared.length} shared rows retained by tasksDone PASS`
    )
  }

  // --- limit boundary shift ----------------------------------------------
  {
    const u = client('u2')
    const top = watch(u, queries.tasksTopByRank()) // rank desc, limit 5
    await eventually(() => {
      if (top.ids().length !== 5) throw new Error(`top-5 has ${top.ids().length}`)
    }, 'top-by-rank window')
    const before = top.ids()
    const outsider = (
      await target.oracle(`SELECT id FROM task ORDER BY rank ASC LIMIT 1`)
    )[0] as {
      id: string
    }
    if (before.includes(outsider.id)) throw new Error('lowest-rank task already in top-5')
    const rerank = u.mutate(mutators.task.setRank({ id: outsider.id, rank: 999999 }))
    await rerank.client
    await assertServerOutcome(rerank.server, 'success', outsider.id)
    await eventually(() => {
      const now = top.ids()
      if (!now.includes(outsider.id))
        throw new Error(`${outsider.id} did not enter top-5`)
      if (now.length !== 5) throw new Error(`top-5 has ${now.length}`)
    }, 'limit boundary shift')
    top.destroy()
    console.log(
      `[queries] limit boundary: ${outsider.id} rose into top-5, window stayed 5 PASS`
    )
  }

  // --- related child rows -------------------------------------------------
  {
    const u = client('u3')
    const withMembers = watch<{ id: string; members: Row[] }>(u, queries.allProjects())
    await eventually(() => {
      if (!withMembers.complete) throw new Error('allProjects not complete')
      const p0 = withMembers.rows().find((r) => r.id === 'p0')
      if (!p0 || !Array.isArray(p0.members)) throw new Error('p0 has no related members')
    }, 'related child rows')
    withMembers.destroy()
    console.log('[queries] related: allProjects pulled related child members PASS')
  }

  // --- query delete: rows leave the raw store ----------------------------
  {
    const u = client('u4')
    const del = watch(u, queries.tasksInProjects({ projectIds: ['p9'] }), 0) // ttl 0
    const want = await oracleIds(target, `SELECT id FROM task WHERE "projectId" = 'p9'`)
    if (want.length === 0) throw new Error('fixture has no p9 tasks')
    await eventually(
      () => equal(del.ids(), want, 'p9 tasks present'),
      'delete precondition'
    )
    del.destroy() // remove the desired query (ttl 0 -> prompt removal)
    await eventually(
      async () => equal(await rawClientIds(u, 'task'), [], 'raw task store cleared'),
      'query delete clears the raw store'
    )
    console.log(
      `[queries] delete: ${want.length} rows left the raw store after query removal PASS`
    )
  }

  // --- reconnect replays desires -----------------------------------------
  {
    const dir = mkdtempSync(join(tmpdir(), 'zharness-q-'))
    const kvStore = persistentKVStoreProvider(dir)
    const storageKey = `q-reconnect-${Date.now()}`
    try {
      const first = client('u7', { kvStore, storageKey })
      const v1 = watch(first, queries.tasksInProjects({ projectIds: ['p0'] }))
      const want = await oracleIds(target, `SELECT id FROM task WHERE "projectId" = 'p0'`)
      await eventually(
        () => equal(v1.ids(), want, 'p0 tasks before reconnect'),
        'reconnect precondition'
      )
      v1.destroy()
      await first.close()
      // reopen the same persisted store: the client re-sends its desired set,
      // the transport re-ships it, and the query rehydrates from the server.
      const second = client('u7', { kvStore, storageKey })
      const v2 = watch(second, queries.tasksInProjects({ projectIds: ['p0'] }))
      await eventually(
        () => equal(v2.ids(), want, 'p0 tasks after reconnect'),
        'reconnect desire replay'
      )
      v2.destroy()
      console.log('[queries] reconnect: desired query replayed after reopen PASS')
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  }

  // --- permission expansion + contraction (membership-gated query) -------
  // projectsWithUserMember(u6) = projects where u6 is a member (a cross-table
  // whereExists — Chat's permission shape). adding u6 to a project expands the
  // result; removing them contracts it, and the lost project must LEAVE the raw
  // store (invariant 15). the client re-evaluates the EXISTS locally, so this
  // needs the engine to sync the correlated member rows (m1 slice, in progress).
  {
    const u = client('u6')
    const memberOf = watch(u, queries.projectsWithUserMember({ userId: 'u6' }))
    const initial = await oracleIds(
      target,
      `SELECT DISTINCT "projectId" id FROM member WHERE "userId" = 'u6'`
    )
    await eventually(
      () => equal(memberOf.ids(), initial, 'initial member projects'),
      'perm initial'
    )
    const grantProject = (
      await target.oracle(
        `SELECT id FROM project WHERE id NOT IN (SELECT "projectId" FROM member WHERE "userId"='u6') ORDER BY id LIMIT 1`
      )
    )[0] as { id: string }
    // EXPANSION
    await target.sql(
      `INSERT INTO member (id, "projectId", "userId") VALUES ('perm-q-u6', '${grantProject.id}', 'u6')`
    )
    await eventually(() => {
      if (!memberOf.ids().includes(grantProject.id))
        throw new Error('granted project not visible')
    }, 'permission expansion')
    if (!(await rawClientIds(u, 'project')).includes(grantProject.id)) {
      throw new Error('granted project not in the raw store')
    }
    // CONTRACTION
    await target.sql(`DELETE FROM member WHERE id = 'perm-q-u6'`)
    await eventually(() => {
      if (memberOf.ids().includes(grantProject.id))
        throw new Error('revoked project still visible')
    }, 'permission contraction')
    if ((await rawClientIds(u, 'project')).includes(grantProject.id)) {
      throw new Error(
        `revoked project ${grantProject.id} lingered in the raw store (invariant 15)`
      )
    }
    memberOf.destroy()
    console.log(
      `[queries] permission: expand+contract via membership, revoked row left raw store PASS`
    )
  }

  // --- lost push response, query-aware client ----------------------------
  // commit a mutation but drop its HTTP response; the client must reconnect and
  // settle via replay, and the woken query must converge on the new row without
  // a duplicate — the query membership recovers through the pull, not the ack.
  {
    const u = client('u8')
    const view = watch(u, queries.tasksInProjects({ projectIds: ['p3'] }))
    const before = await oracleIds(target, `SELECT id FROM task WHERE "projectId" = 'p3'`)
    await eventually(
      () => equal(view.ids(), before, 'p3 tasks before'),
      'lost-response precondition'
    )
    const newId = `q-lost-${Date.now().toString(36)}`
    await (target as QueryTarget).dropNextPushResponse()
    const req = u.mutate(
      mutators.task.create({
        id: newId,
        projectId: 'p3',
        title: 'lost response task',
        rank: 1,
        done: false,
      })
    )
    await req.client
    await assertServerOutcome(req.server, 'success', newId)
    await eventually(() => {
      if (!view.ids().includes(newId)) throw new Error('recovered row not in query')
    }, 'lost-response query convergence')
    const rows = await target.oracle(`SELECT id FROM task WHERE id = '${newId}'`)
    if (rows.length !== 1) throw new Error('lost-response mutation was duplicated')
    view.destroy()
    console.log(
      '[queries] lost-response: query converged on the recovered row, no duplicate PASS'
    )
  }

  console.log(`[queries] PASS ${args.against}: full query-lifecycle matrix`)
} catch (error) {
  failed = true
  console.error('[queries] FAIL:', error)
} finally {
  for (const zero of clients) await zero.close().catch(() => {})
  await target.close()
}

process.exit(failed ? 1 : 0)
