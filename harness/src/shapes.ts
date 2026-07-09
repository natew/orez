// M3 shapes lane: cross-implementation differential. both targets boot with
// the IDENTICAL deterministic dataset, one client per target registers the
// whole query corpus (named queries — ad-hoc zql wouldn't sync on stock-zero),
// then a deterministic write script runs the same mutations + upstream SQL on
// both. after every barrier, every corpus query's materialized result must be
// deep-equal across targets, and a fresh late-joining client per target must
// hydrate to the same answers (incremental == fresh).
//
// stock-zero evaluates queries server-side (view-syncer IVM row selection);
// the orez targets ship full snapshots and evaluate client-side. equal
// results is exactly the conformance property the rewrite must hold.
//
//   bun src/shapes.ts                      # stock-zero vs orez-local
//   bun src/shapes.ts --against orez-cf    # stock-zero vs the CF DO host
import { parseArgs } from 'node:util'
import { canonical } from './canonical.js'
import { mutators, queries, queryCorpus } from './fixture.js'
import type { FixtureZero, SyncTarget } from './target.js'
import { startStockZero } from './targets/stock-zero.js'

const { values: args } = parseArgs({
  options: {
    against: { type: 'string', default: 'orez-local' },
  },
})

// registry entries take the RAW args value in 1.6.1 (the {args, ctx} options
// object is the DEFINITION-side signature; wrapping here double-wraps and
// every filter silently matches nothing — this lane caught that as all
// arg-taking queries returning empty)
function invokeQuery(name: string, args: unknown) {
  const def = (queries as unknown as Record<string, (args?: unknown) => unknown>)[name]!
  return args === undefined ? def() : def(args)
}

type CorpusViews = Map<string, { rows: () => unknown; complete: () => boolean }>

function materializeCorpus(zero: FixtureZero): { views: CorpusViews; destroy: () => void } {
  const views: CorpusViews = new Map()
  const destroys: Array<() => void> = []
  for (const { name, args } of queryCorpus) {
    const view = zero.materialize(invokeQuery(name, args) as never)
    let rows: unknown = null
    let complete = false
    view.addListener((data: unknown, resultType: string) => {
      rows = JSON.parse(JSON.stringify(data ?? null))
      if (resultType === 'complete') complete = true
    })
    views.set(name, { rows: () => rows, complete: () => complete })
    destroys.push(() => view.destroy())
  }
  return { views, destroy: () => destroys.forEach((d) => d()) }
}

async function eventually(check: () => void, timeoutMs: number, label: string) {
  const start = Date.now()
  let lastError: unknown
  while (Date.now() - start < timeoutMs) {
    try {
      check()
      return
    } catch (error) {
      lastError = error
      await new Promise((r) => setTimeout(r, 50))
    }
  }
  throw new Error(`timeout (${timeoutMs}ms): ${label}: ${lastError}`)
}

function diffCorpus(phase: string, a: CorpusViews, b: CorpusViews, bName: string): string[] {
  const failures: string[] = []
  for (const { name } of queryCorpus) {
    const left = canonical(a.get(name)!.rows())
    const right = canonical(b.get(name)!.rows())
    if (left !== right) {
      failures.push(
        `[${phase}] ${name} diverged:\n  stock-zero: ${left?.slice(0, 400)}\n  ${bName}: ${right?.slice(0, 400)}`
      )
    }
  }
  return failures
}

// deterministic write script: mutations through each target's own client +
// upstream sql behind zero's back, same order on both targets
async function runWriteScript(target: SyncTarget, zero: FixtureZero) {
  const acks: Promise<unknown>[] = []
  const mutate = (req: { client: Promise<unknown>; server: Promise<unknown> }) => {
    acks.push(req.server)
    return req.client
  }

  await mutate(zero.mutate(mutators.project.create({ id: 'pw1', ownerId: 'u1', name: 'written α' })))
  await mutate(
    zero.mutate(
      mutators.task.create({
        id: 'tw1',
        projectId: 'pw1',
        title: 'fix written task',
        rank: 7.25,
        done: false,
        meta: { from: 'script', n: [1, 2] },
        dueAt: 1755000000000,
      })
    )
  )
  await mutate(zero.mutate(mutators.task.toggle({ id: 'tw1' })))
  await mutate(zero.mutate(mutators.task.toggle({ id: 't3' })))
  await mutate(zero.mutate(mutators.project.rename({ id: 'p3', name: 'renamed ζ' })))
  await mutate(zero.mutate(mutators.member.add({ id: 'mw1', projectId: 'p2', userId: 'u7' })))
  await mutate(zero.mutate(mutators.member.remove({ id: 'm2' })))
  await mutate(zero.mutate(mutators.project.delete({ id: 'p9' })))
  // window churn: shove rows across the tasksTopByRank/tasksAfterCursor
  // boundaries so the incremental maintenance path has to add AND evict
  await mutate(zero.mutate(mutators.task.setRank({ id: 't11', rank: 99.5 })))
  await mutate(zero.mutate(mutators.task.setRank({ id: 't13', rank: 98.25 })))
  await mutate(zero.mutate(mutators.task.setRank({ id: 't20', rank: -99 })))
  // app-error path: duplicate create must reject server-side + roll back
  await mutate(zero.mutate(mutators.project.create({ id: 'pw1', ownerId: 'u1', name: 'dupe' })))

  // upstream writes behind zero's back (replication path on stock, version
  // bump on orez-local)
  await target.sql(`INSERT INTO project (id, "ownerId", name) VALUES ('pu1', 'u2', 'upstream β')`)
  await target.sql(
    `INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt")
     VALUES ('tu1', 'pu1', 'upstream fix task', 3.5, true, '{"src":"sql"}', NULL)`
  )
  await target.sql(`UPDATE task SET rank = 9.75 WHERE id = 't5'`)
  await target.sql(`DELETE FROM task WHERE id = 't7'`)
  // null-semantics churn: rows must cross tasksDueNull/tasksDueBefore
  await target.sql(`UPDATE task SET "dueAt" = NULL WHERE id = 't2'`)
  await target.sql(`UPDATE task SET "dueAt" = 1750000005000 WHERE id = 't6'`)

  await Promise.allSettled(acks) // the dupe create rejects; the rest must resolve
}

async function startAgainst(name: string): Promise<SyncTarget> {
  if (name === 'orez-local') {
    return (await import('./targets/orez-local.js')).startOrezLocal({ pullIntervalMs: 150 })
  }
  if (name === 'orez-cf') {
    return (await import('./targets/orez-cf.js')).startOrezCf({ pullIntervalMs: 150 })
  }
  throw new Error(`unknown --against target '${name}'`)
}

const t0 = Date.now()
console.log(`[shapes] booting stock-zero and ${args.against}...`)
const [stock, other] = await Promise.all([startStockZero(), startAgainst(args.against!)])
const targets: Array<{ target: SyncTarget; zero: FixtureZero }> = [
  { target: stock, zero: stock.createClient('user-1') },
  { target: other, zero: other.createClient('user-1') },
]

let failures: string[] = []
try {
  const [stockViews, localViews] = targets.map(({ zero }) => materializeCorpus(zero))

  await eventually(
    () => {
      for (const views of [stockViews!, localViews!]) {
        for (const { name } of queryCorpus) {
          if (!views.views.get(name)!.complete()) throw new Error(`${name} not complete`)
        }
      }
    },
    60_000,
    'corpus hydration on both targets'
  )
  failures.push(...diffCorpus('hydrate', stockViews!.views, localViews!.views, other.name))
  console.log(
    `[shapes] hydrate: ${queryCorpus.length} queries on both targets, ${failures.length} divergences`
  )

  // sanity: the dataset must actually exercise the shapes (no vacuous greens)
  const empty = queryCorpus.filter(({ name }) => {
    const rows = stockViews!.views.get(name)!.rows()
    return rows === null || (Array.isArray(rows) && rows.length === 0)
  })
  console.log(
    `[shapes] ${queryCorpus.length - empty.length}/${queryCorpus.length} corpus queries return data` +
      (empty.length ? ` (empty: ${empty.map((e) => e.name).join(', ')})` : '')
  )
  if (empty.length > 0) {
    failures.push(`empty corpus queries (dataset must exercise every shape): ${empty.map((e) => e.name).join(', ')}`)
  }

  console.log('[shapes] running write script on both targets...')
  await Promise.all(targets.map(({ target, zero }) => runWriteScript(target, zero)))

  // barrier: both sides converge on the post-script dataset (t7 deleted, p9
  // deleted, pw1/pu1 added...) — use a distinctive marker each side must see
  await eventually(
    () => {
      for (const views of [stockViews!, localViews!]) {
        const all = JSON.stringify(views.views.get('allProjects')!.rows())
        if (!all.includes('pu1')) throw new Error('upstream project not visible')
        if (all.includes('"p9"')) throw new Error('deleted project still visible')
        const tasks = JSON.stringify(views.views.get('tasksNotDoneByDue')!.rows())
        if (tasks.includes('"t7"')) throw new Error('deleted task still visible')
      }
    },
    60_000,
    'post-script convergence'
  )
  failures.push(...diffCorpus('post-writes', stockViews!.views, localViews!.views, other.name))
  console.log(`[shapes] post-writes: compared, total ${failures.length} divergences`)

  // incremental == fresh: a late client per target must match the long-lived
  // client's corpus exactly
  for (const { target, zero: longLived } of targets) {
    const late = target.createClient('user-1')
    const lateViews = materializeCorpus(late)
    await eventually(
      () => {
        for (const { name } of queryCorpus) {
          if (!lateViews.views.get(name)!.complete()) throw new Error(`${name} late not complete`)
        }
      },
      60_000,
      `${target.name} late corpus hydration`
    )
    const longViews = target === stock ? stockViews! : localViews!
    for (const { name } of queryCorpus) {
      const fresh = canonical(lateViews.views.get(name)!.rows())
      const maintained = canonical(longViews.views.get(name)!.rows())
      if (fresh !== maintained) {
        failures.push(
          `[incremental==fresh] ${target.name} ${name}:\n  maintained: ${maintained?.slice(0, 400)}\n  fresh: ${fresh?.slice(0, 400)}`
        )
      }
    }
    lateViews.destroy()
    console.log(`[shapes] ${target.name}: incremental == fresh checked`)
  }

  stockViews!.destroy()
  localViews!.destroy()
} catch (error) {
  failures.push(`fatal: ${error}`)
} finally {
  await Promise.allSettled(targets.map(({ target }) => target.close()))
}

if (failures.length > 0) {
  console.error(`[shapes] FAIL — ${failures.length} divergences:`)
  for (const f of failures) console.error(f)
  process.exit(1)
}
console.log(
  `[shapes] PASS: ${queryCorpus.length} query shapes x (hydrate + writes + incremental==fresh) equal across stock-zero and ${other.name} in ${Date.now() - t0}ms`
)
process.exit(0)
