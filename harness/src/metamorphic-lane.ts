// PRODUCT / REFERENCE CONFORMANCE audit lane (NON-GATING). Runs the metamorphic
// relations from metamorphic.ts against ONE real target over the wire — no
// oracle, no second implementation — to expose per-target query-evaluation bugs
// the stock-vs-orez differential can miss: bugs in an axis the sweep generator
// never emits, and the harder class where both targets share a wrong behavior.
// EMPIRICALLY it caught #6121 on the stock 1.7.0 reference (a start cursor
// anchored on a NULL-sorted row returns empty server-side; orez-local passes).
//
// IMPORTANT (per manager guardrail): this lane is NON-GATING. The 1.7.0 pin may
// genuinely contain #6121, so a FAIL here is a CLASSIFIED KNOWN-GAP / repro, not
// a CI break. Do NOT wire this into the gating CI harness job. The checker
// itself is validated separately and green in metamorphic.selftest.ts; keep that
// distinction. Run this manually or in a nightly audit; a failure writes a
// classified repro artifact and reports the finding.
//
//   bun src/metamorphic-lane.ts                       # audit orez-local
//   bun src/metamorphic-lane.ts --against orez-cf
//   bun src/metamorphic-lane.ts --mutate startSuffix  # plant #6121 live: proves
//                                                       # the wiring catches it
import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { SEED, queries, type GenSpec } from './fixture.js'
import { metamorphicChecks, RELATIONS, type Relation } from './metamorphic.js'
import { startStockZero } from './targets/stock-zero.js'

import type { FixtureZero, SyncTarget } from './target.js'

const { values: args } = parseArgs({
  options: {
    against: { type: 'string', default: 'orez-local' },
    // plant a bug in the live result of one relation to prove the lane catches
    // it end-to-end (demonstration; not for normal runs)
    mutate: { type: 'string' },
  },
})

// validate --mutate against the closed relation vocabulary so a typo cannot
// silently run unmutated and falsely claim a wiring proof.
if (args.mutate !== undefined && !RELATIONS.includes(args.mutate as Relation)) {
  console.error(
    `[metamorphic] invalid --mutate '${args.mutate}'. valid relations: ${RELATIONS.join(', ')}`
  )
  process.exit(2)
}

// ---------------------------------------------------------------------------
// focused generator: the blind-spot shapes. deterministic (real seed rows as
// cursors), heavy on nullable-column start cursors so the NULL-sorted region
// (#6121) is actually exercised.
// ---------------------------------------------------------------------------

const nullDueTasks = SEED.task.filter((t) => t.dueAt === null).slice(0, 3)
const valueDueTasks = SEED.task.filter((t) => t.dueAt !== null).slice(0, 3)
const rankCursors = SEED.task.slice(0, 2)

type LaneSpec = { spec: GenSpec; nullAnchored: boolean; label: string }

function startSpecs(): LaneSpec[] {
  const out: LaneSpec[] = []
  const cursorSets: { rows: typeof SEED.task; nullAnchored: boolean; col: string }[] = [
    { rows: nullDueTasks, nullAnchored: true, col: 'dueAt' },
    { rows: valueDueTasks, nullAnchored: false, col: 'dueAt' },
    { rows: rankCursors, nullAnchored: false, col: 'rank' },
  ]
  for (const { rows, nullAnchored, col } of cursorSets) {
    for (const cursor of rows) {
      for (const dir of ['asc', 'desc'] as const) {
        for (const inclusive of [false, true]) {
          for (const limit of [undefined, 4] as const) {
            const cursorVal = (cursor as Record<string, unknown>)[col]
            out.push({
              nullAnchored,
              label: `start ${col} ${dir} ${inclusive ? 'incl' : 'excl'}${limit ? ' lim' + limit : ''} @${cursor.id}(${JSON.stringify(cursorVal)})`,
              spec: {
                table: 'task',
                orderBy: [
                  [col, dir],
                  ['id', 'asc'],
                ],
                start: {
                  row: { [col]: cursorVal, id: cursor.id },
                  inclusive: inclusive || undefined,
                },
                ...(limit !== undefined ? { limit } : {}),
              },
            })
          }
        }
      }
    }
  }
  return out
}

// a few equal-invariant + limitPrefix breadth shapes (task where/and/limit)
function breadthSpecs(): LaneSpec[] {
  return [
    {
      nullAnchored: false,
      label: 'and-where',
      spec: {
        table: 'task',
        where: {
          op: 'and',
          children: [
            { op: 'cmp', col: 'done', cmp: '=', value: false },
            { op: 'cmp', col: 'rank', cmp: '>', value: 5 },
          ],
        },
        orderBy: [['id', 'asc']],
      },
    },
    {
      nullAnchored: false,
      label: 'plain-limit',
      spec: {
        table: 'task',
        orderBy: [
          ['rank', 'desc'],
          ['id', 'asc'],
        ],
        limit: 5,
      },
    },
    {
      nullAnchored: false,
      label: 'projects',
      spec: {
        table: 'project',
        orderBy: [
          ['name', 'asc'],
          ['id', 'asc'],
        ],
        limit: 4,
      },
    },
  ]
}

// ---------------------------------------------------------------------------
// plumbing (mirrors sweep.ts)
// ---------------------------------------------------------------------------

async function startAgainst(name: string): Promise<SyncTarget> {
  // stock-zero is the REFERENCE: confirming a finding here places it in the
  // stock 1.7.0 reference path, not an Orez-specific artifact.
  if (name === 'stock-zero') return startStockZero()
  if (name === 'orez-local')
    return (await import('./targets/orez-local.js')).startOrezLocal({
      pullIntervalMs: 150,
    })
  if (name === 'orez-cf')
    return (await import('./targets/orez-cf.js')).startOrezCf({ pullIntervalMs: 150 })
  if (name === 'rust-local')
    return (await import('./targets/rust-local.js')).startRustLocal({
      pullIntervalMs: 150,
    })
  if (name === 'rust-cf')
    return (await import('./targets/rust-cf.js')).startRustCf({ pullIntervalMs: 150 })
  throw new Error(`unknown --against target '${name}'`)
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

async function materialize(zero: FixtureZero, spec: GenSpec): Promise<unknown> {
  const view = zero.materialize(queries.generated(spec) as never)
  let rows: unknown = null
  let complete = false
  view.addListener((data: unknown, resultType: string) => {
    rows = JSON.parse(JSON.stringify(data ?? null))
    if (resultType === 'complete') complete = true
  })
  await eventually(
    () => {
      if (!complete) throw new Error('not complete')
    },
    30_000,
    `materialize ${JSON.stringify(spec).slice(0, 120)}`
  )
  view.destroy()
  return rows
}

const REGRESSIONS_DIR = join(import.meta.dirname, '..', 'regressions')

function recordRepro(entry: Record<string, unknown>): string {
  mkdirSync(REGRESSIONS_DIR, { recursive: true })
  const file = join(
    REGRESSIONS_DIR,
    `metamorphic-${args.against}-${entry.relation}-${Date.now()}.json`
  )
  writeFileSync(
    file,
    JSON.stringify({ kind: 'metamorphic-known-gap', ...entry }, null, 2)
  )
  return file
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

console.log('┌─────────────────────────────────────────────────────────────────────┐')
console.log('│ metamorphic conformance audit — NON-GATING.                          │')
console.log('│ a FAIL is a classified known-gap/repro (e.g. #6121 in the 1.7.0 pin),│')
console.log('│ NOT a CI break. checker validation is separate (metamorphic.selftest)│')
console.log('└─────────────────────────────────────────────────────────────────────┘')

const specs = [...startSpecs(), ...breadthSpecs()]
const target = await startAgainst(args.against!)

const tally: Record<string, { pass: number; fail: number; skip: number }> = {}
const bump = (rel: Relation, r: 'pass' | 'fail' | 'skip') => {
  ;(tally[rel] ??= { pass: 0, fail: 0, skip: 0 })[r]++
}
const failures: string[] = []
let appliedNullStartSuffix = 0
let mutatedCount = 0
let mutatedFailCount = 0

try {
  const zero = target.createClient('user-1')

  for (const { spec, nullAnchored, label } of specs) {
    const baseRows = await materialize(zero, spec)
    for (const check of metamorphicChecks(spec)) {
      const variantRows = await materialize(zero, check.variant)
      // optional live mutation: prove the wiring catches a broken engine result
      const doMutate =
        args.mutate === check.relation &&
        (check.relation !== 'startSuffix' || nullAnchored)
      if (doMutate) mutatedCount++
      const seenBase = doMutate ? [] : baseRows // simulate #6121 empty-continuation
      const outcome = check.relate(seenBase, variantRows)
      bump(check.relation, outcome.result)
      if (doMutate && outcome.result === 'fail') mutatedFailCount++
      if (check.relation === 'startSuffix' && nullAnchored && outcome.result !== 'skip') {
        appliedNullStartSuffix++
      }
      if (outcome.result === 'fail') {
        if (doMutate) {
          // SYNTHETIC injected failure (--mutate). Prove the wiring catches it
          // via the count + nonzero exit, but NEVER write it into the tracked
          // corpus as a real known-gap/product repro.
          failures.push(
            `[${check.relation}] ${label} FAIL (synthetic --mutate injection; no repro written)`
          )
        } else {
          // REAL conformance finding on a clean run: record a classified repro.
          const file = recordRepro({
            relation: check.relation,
            target: target.name,
            label,
            nullAnchored,
            spec,
            variant: check.variant,
            base: JSON.stringify(seenBase).slice(0, 4000),
            expected: JSON.stringify(outcome.expected).slice(0, 4000),
            detail: outcome.detail,
            note:
              nullAnchored && check.relation === 'startSuffix'
                ? 'NULL-cursor blind spot (candidate #6121 in the pinned zqlite). Classified known-gap; flips green once the pin advances past mono d4f33d6a6.'
                : 'metamorphic invariant violated on the target evaluator.',
            replay: `bun src/metamorphic-lane.ts --against ${args.against}`,
          })
          failures.push(
            `[${check.relation}] ${label} FAIL (repro ${file})\n  detail: ${outcome.detail}`
          )
        }
      }
    }
  }
} catch (error) {
  failures.push(`fatal: ${error}`)
} finally {
  await target.close()
}

console.log('\n[metamorphic] relation tallies:')
for (const [rel, t] of Object.entries(tally)) {
  console.log(`  ${rel.padEnd(18)} pass=${t.pass} fail=${t.fail} skip=${t.skip}`)
}
console.log(
  `[metamorphic] null-anchored startSuffix checks actually applied: ${appliedNullStartSuffix}`
)

// anti-vacuous gate — ALWAYS enforced (including under --mutate): the lane MUST
// have exercised the NULL-cursor region, or it proves nothing about the blind
// spot.
if (appliedNullStartSuffix < 4) {
  console.error(
    `[metamorphic] VACUOUS: only ${appliedNullStartSuffix} null-anchored startSuffix checks applied (expected >= 4). The lane did not exercise the blind spot.`
  )
  process.exit(2)
}

// under --mutate, the injected bug must have actually FIRED and been CAUGHT, or
// the wiring proof is empty (a no-op mutation must never look like success).
if (args.mutate) {
  console.log(
    `[metamorphic] --mutate ${args.mutate}: ${mutatedCount} check(s) mutated, ${mutatedFailCount} caught as fail`
  )
  if (mutatedCount === 0) {
    console.error(
      `[metamorphic] --mutate ${args.mutate} matched no applied check — nothing was mutated; wiring UNPROVEN.`
    )
    process.exit(2)
  }
  if (mutatedFailCount === 0) {
    console.error(
      `[metamorphic] --mutate ${args.mutate} fired ${mutatedCount}x but no failure was detected — the lane did not catch the injected bug; wiring UNPROVEN.`
    )
    process.exit(2)
  }
}

if (failures.length) {
  console.error(
    `\n[metamorphic] ${failures.length} conformance finding(s) against ${target.name} (NON-GATING; classified known-gap/repro):`
  )
  for (const f of failures) console.error(f)
  process.exit(1)
}
console.log(
  `\n[metamorphic] PASS — ${target.name} is self-consistent under every applied metamorphic relation (incl. ${appliedNullStartSuffix} null-anchored cursors).`
)
process.exit(0)
