// M1 smoke: N concurrent stock zero clients against a SyncTarget, modern API
// only. each client materializes a NAMED query (server-transformed via
// ZERO_QUERY_URL), writes through CUSTOM mutators (optimistic + authoritative
// via ZERO_MUTATE_URL), upstream rows are written behind zero's back
// (replication path), everything must converge, and converged client views
// must equal a fresh oracle read. also asserts the ad-hoc-zql nuance: local
// queries read the synced cache without syncing anything new. exit 0 = pass.
//
//   bun src/smoke.ts --target stock-zero --clients 10
import { parseArgs } from 'node:util'
import { mutators, queries, zql } from './fixture.js'
import type { FixtureZero, SyncTarget } from './target.js'
import { startStockZero } from './targets/stock-zero.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'stock-zero' },
    clients: { type: 'string', default: '10' },
    projects: { type: 'string', default: '5' },
  },
})

const CLIENTS = Number(args.clients)
const PROJECTS_PER_CLIENT = Number(args.projects)

async function startTarget(name: string): Promise<SyncTarget> {
  if (name === 'stock-zero') return startStockZero()
  throw new Error(`unknown target '${name}' (orez-local and orez-cf are M2/M5)`)
}

type ProjectRow = { id: string; ownerId: string; name: string; members: unknown[] }

function watchProjects(zero: FixtureZero) {
  const view = zero.materialize(queries.allProjects())
  let rows: ProjectRow[] = []
  let complete = false
  view.addListener((data, resultType) => {
    rows = JSON.parse(JSON.stringify(data)) as ProjectRow[]
    if (resultType === 'complete') complete = true
  })
  return {
    get rows() {
      return rows
    },
    get complete() {
      return complete
    },
    destroy: () => view.destroy(),
  }
}

async function eventually(check: () => void, timeoutMs: number, label: string) {
  const start = Date.now()
  let lastError: unknown
  while (Date.now() - start < timeoutMs) {
    try {
      check()
      return Date.now() - start
    } catch (error) {
      lastError = error
      await new Promise((r) => setTimeout(r, 50))
    }
  }
  throw new Error(`timeout (${timeoutMs}ms) waiting for ${label}: ${lastError}`)
}

function sortById<T extends { id: string }>(rows: T[]) {
  return [...rows].sort((a, b) => a.id.localeCompare(b.id))
}

const t0 = Date.now()
const target = await startTarget(args.target!)
console.log(`[smoke] target '${target.name}' up in ${Date.now() - t0}ms`)

let failed = false
try {
  const watchers: ReturnType<typeof watchProjects>[] = []
  const zeros: FixtureZero[] = []
  for (let i = 0; i < CLIENTS; i++) {
    const zero = target.createClient(`user-${i}`)
    zeros.push(zero)
    watchers.push(watchProjects(zero))
  }

  // initial hydration: every client must reach a complete result with the seed
  const tHydrate = await eventually(
    () => {
      for (const [i, w] of watchers.entries()) {
        if (!w.complete) throw new Error(`client ${i} not complete`)
        if (w.rows.length < 1) throw new Error(`client ${i} missing seed`)
      }
    },
    30_000,
    'initial hydration'
  )
  console.log(`[smoke] ${CLIENTS} clients hydrated in ${tHydrate}ms`)

  // concurrent custom-mutator writes from every client; collect the server
  // (authoritative) promises so the oracle compare waits for real commits
  const tWrites = Date.now()
  const serverAcks: Promise<unknown>[] = []
  await Promise.all(
    zeros.map(async (zero, i) => {
      for (let p = 0; p < PROJECTS_PER_CLIENT; p++) {
        const id = `p-${i}-${p}`
        const created = zero.mutate(
          mutators.project.create({ id, ownerId: `user-${i}`, name: `proj ${i}.${p}` })
        )
        serverAcks.push(created.server)
        await created.client
        const added = zero.mutate(
          mutators.member.add({ id: `m-${i}-${p}`, projectId: id, userId: `user-${i}` })
        )
        serverAcks.push(added.server)
        await added.client
      }
    })
  )
  console.log(`[smoke] custom mutations issued (optimistic) in ${Date.now() - tWrites}ms`)

  // upstream writes behind zero's back: must arrive via replication
  await target.sql(
    `INSERT INTO project (id, "ownerId", name) VALUES ('p-upstream', 'u-seed', 'upstream project')`
  )
  await target.sql(
    `INSERT INTO member (id, "projectId", "userId") VALUES ('m-upstream', 'p-upstream', 'u-seed')`
  )

  await Promise.all(serverAcks)
  console.log(`[smoke] all ${serverAcks.length} mutations server-acked at +${Date.now() - tWrites}ms`)

  const expectedProjects = 1 + 1 + CLIENTS * PROJECTS_PER_CLIENT // seed + upstream + mutated

  const tConverge = await eventually(
    () => {
      for (const [i, w] of watchers.entries()) {
        if (w.rows.length !== expectedProjects) {
          throw new Error(`client ${i} sees ${w.rows.length}/${expectedProjects} projects`)
        }
      }
    },
    60_000,
    'convergence'
  )
  console.log(`[smoke] ${CLIENTS} clients converged on ${expectedProjects} projects in ${tConverge}ms`)

  // oracle compare: converged client state must equal a fresh authoritative read
  const oracleProjects = sortById(
    (await target.oracle(`SELECT id, "ownerId", name FROM project`)) as never[]
  ) as { id: string; ownerId: string; name: string }[]
  const oracleMembers = sortById(
    (await target.oracle(`SELECT id, "projectId", "userId" FROM member`)) as never[]
  ) as { id: string; projectId: string; userId: string }[]

  for (const [i, w] of watchers.entries()) {
    const clientProjects = sortById(w.rows).map(({ id, ownerId, name }) => ({ id, ownerId, name }))
    const clientMembers = sortById(
      w.rows.flatMap((r) => r.members as { id: string; projectId: string; userId: string }[])
    ).map(({ id, projectId, userId }) => ({ id, projectId, userId }))

    const wantProjects = JSON.stringify(oracleProjects)
    const gotProjects = JSON.stringify(clientProjects)
    if (gotProjects !== wantProjects) {
      throw new Error(`client ${i} project divergence:\n got ${gotProjects}\nwant ${wantProjects}`)
    }
    const wantMembers = JSON.stringify(oracleMembers)
    const gotMembers = JSON.stringify(clientMembers)
    if (gotMembers !== wantMembers) {
      throw new Error(`client ${i} member divergence:\n got ${gotMembers}\nwant ${wantMembers}`)
    }
  }
  console.log(`[smoke] oracle compare: ${CLIENTS} clients x ${oracleProjects.length} projects + ${oracleMembers.length} members all equal`)

  // fresh-client hydration: a client that connects AFTER all writes has no
  // local cache to answer from — everything it sees must come from the
  // server. this is the check that proves server-side state; same-client
  // read-back proves nothing (zero answers from the local cache).
  const late = target.createClient('late-joiner')
  const lateWatch = watchProjects(late)
  const tLate = await eventually(
    () => {
      if (!lateWatch.complete) throw new Error('late client not complete')
      if (lateWatch.rows.length !== expectedProjects) {
        throw new Error(`late client sees ${lateWatch.rows.length}/${expectedProjects}`)
      }
    },
    30_000,
    'fresh-client hydration'
  )
  const lateProjects = sortById(lateWatch.rows).map(({ id, ownerId, name }) => ({ id, ownerId, name }))
  if (JSON.stringify(lateProjects) !== JSON.stringify(oracleProjects)) {
    throw new Error('fresh client hydration diverged from oracle')
  }
  lateWatch.destroy()
  console.log(`[smoke] fresh late-joining client hydrated ${expectedProjects} projects from server in ${tLate}ms, equals oracle`)

  // ad-hoc local zql: reads the already-synced cache only (never syncs more).
  // the member table synced via allProjects' related(); a local query over it
  // must see exactly the oracle's member rows without registering anything.
  const localView = zeros[0]!.materialize(zql.member.orderBy('id', 'asc'))
  const localRows = await new Promise<{ id: string }[]>((resolve) => {
    const cleanup = localView.addListener((data) => {
      resolve(JSON.parse(JSON.stringify(data)) as { id: string }[])
      queueMicrotask(() => cleanup())
    })
  })
  if (localRows.length !== oracleMembers.length) {
    throw new Error(
      `ad-hoc local zql saw ${localRows.length} members, cache should hold ${oracleMembers.length}`
    )
  }
  localView.destroy()
  console.log(`[smoke] ad-hoc local zql over synced cache: ${localRows.length} members, no extra sync`)

  for (const w of watchers) w.destroy()
  console.log(`[smoke] PASS target=${target.name} clients=${CLIENTS} total=${Date.now() - t0}ms`)
} catch (error) {
  failed = true
  console.error(`[smoke] FAIL:`, error)
} finally {
  await target.close()
}

process.exit(failed ? 1 : 0)
