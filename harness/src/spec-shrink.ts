// PURE structure-aware shrinker for the sweep query generator's GenSpec, ported
// black-box from rocicorp/mono chinook/fuzz/shrink.ts (audited at origin/main
// 7139287da3). On a sweep divergence, delta-debug the failing spec to a minimal
// still-reproducing spec. CARDINALITY-AWARE so every candidate stays within the
// frozen sweep generator grammar (a one-relation sub keeps one:true; a many-
// relation sub keeps its orderBy) — otherwise the shrunk repro would be rejected
// by spec-corpus.parseCorpusEntry.
//
// PURE: type-only GenSpec import (erased) + the zero-free schema graph + canonical
// for dedup. Parameterized by a `stillDiverges` predicate, so it is unit- and
// mutant-testable without booting anything. See shrink-corpus-contract.md.
import { canonical } from './canonical.js'
import { type Card, RELATIONSHIPS } from './fixture-graph.js'

import type { GenSpec, GenSubSpec, GenWhere } from './fixture.js'

// ---------------------------------------------------------------------------
// complexity measure (upstream constructCount): 1 per where condition node, per
// exists entry PLUS its nested where, per NON-id orderBy term, per limit/start/
// one flag, per related child (recursively).
// ---------------------------------------------------------------------------
function condCount(c: GenWhere): number {
  return c.op === 'cmp' ? 1 : 1 + c.children.reduce((s, cc) => s + condCount(cc), 0)
}
function subConstructCount(sub: GenSubSpec): number {
  let n = 0
  if (sub.where) n += condCount(sub.where)
  n += (sub.orderBy ?? []).filter(([col]) => col !== 'id').length
  if (sub.limit !== undefined) n++
  if (sub.one) n++
  for (const r of sub.related ?? []) n += 1 + (r.sub ? subConstructCount(r.sub) : 0)
  return n
}
export function constructCount(spec: GenSpec): number {
  let n = subConstructCount(spec)
  for (const e of spec.exists ?? []) n += 1 + (e.where ? condCount(e.where) : 0)
  if (spec.start !== undefined) n++
  return n
}

// ---------------------------------------------------------------------------
// one-step shrinks (upstream oneStepShrinks): every candidate is a strictly-
// smaller, still-buildable, still-in-grammar spec. deterministic; canonical-
// deduped.
// ---------------------------------------------------------------------------
function omit<T extends object>(obj: T, key: keyof T): T {
  const copy = { ...obj }
  delete copy[key]
  return copy
}

function condShrinks(c: GenWhere): GenWhere[] {
  if (c.op === 'cmp') return []
  const out: GenWhere[] = []
  if (c.children.length >= 2) {
    for (let i = 0; i < c.children.length; i++) {
      const kept = c.children.filter((_, j) => j !== i)
      out.push(kept.length === 1 ? kept[0]! : { op: c.op, children: kept })
    }
  }
  for (let i = 0; i < c.children.length; i++) {
    for (const s of condShrinks(c.children[i]!)) {
      out.push({ op: c.op, children: c.children.map((cc, j) => (j === i ? s : cc)) })
    }
  }
  return out
}

// shrinks of a nested related sub. `card` fixes the grammar: a ONE-relation sub
// is {one:true, related?} — only its nested related may shrink (one:true stays).
// a MANY-relation sub is {orderBy(required), where?, limit?, related?} — its
// non-id order term / where / limit / related may shrink, but the orderBy is
// never dropped whole and one is never added.
function oneStepShrinksSub(sub: GenSubSpec, table: string, card: Card): GenSubSpec[] {
  const out: GenSubSpec[] = []
  const rel = sub.related ?? []
  if (card === 'many') {
    if (sub.limit !== undefined) out.push(omit(sub, 'limit'))
    const ob = sub.orderBy ?? []
    for (let i = 0; i < ob.length; i++) {
      if (ob[i]![0] !== 'id') out.push({ ...sub, orderBy: ob.filter((_, j) => j !== i) })
    }
    if (sub.where) {
      out.push(omit(sub, 'where'))
      for (const w of condShrinks(sub.where)) out.push({ ...sub, where: w })
    }
  }
  for (let i = 0; i < rel.length; i++) {
    // omit `related` entirely when removing the last entry (an empty array is
    // outside the generator grammar).
    const nr = rel.filter((_, j) => j !== i)
    out.push(nr.length ? { ...sub, related: nr } : omit(sub, 'related'))
  }
  for (let i = 0; i < rel.length; i++) {
    const child = rel[i]!
    const info = RELATIONSHIPS[table]?.[child.rel]
    if (child.sub && info) {
      for (const s of oneStepShrinksSub(child.sub, info.child, info.card)) {
        out.push({ ...sub, related: rel.map((r, j) => (j === i ? { ...r, sub: s } : r)) })
      }
    }
  }
  return out
}

export function oneStepShrinks(spec: GenSpec): GenSpec[] {
  const out: GenSpec[] = []
  const table = spec.table
  if (spec.limit !== undefined) out.push(omit(spec, 'limit'))
  if (spec.start !== undefined) out.push(omit(spec, 'start'))
  if (spec.one) out.push(omit(spec, 'one'))
  // remove a non-id orderBy term ONLY when there is no start cursor (the cursor
  // seeks in the orderBy order); ALWAYS retain the trailing id tie-break and
  // never drop the whole (always-present) root orderBy.
  const ob = spec.orderBy ?? []
  if (spec.start === undefined) {
    for (let i = 0; i < ob.length; i++) {
      if (ob[i]![0] !== 'id') out.push({ ...spec, orderBy: ob.filter((_, j) => j !== i) })
    }
  }
  // omit `exists`/`related` entirely when removing the last entry (an empty
  // array is outside the generator grammar).
  const ex = spec.exists ?? []
  for (let i = 0; i < ex.length; i++) {
    const ne = ex.filter((_, j) => j !== i)
    out.push(ne.length ? { ...spec, exists: ne } : omit(spec, 'exists'))
  }
  // KEEP the exists entry but shrink its nested where (drop it, or simplify it) —
  // a divergence needing the relationship plus one leaf must reduce to that leaf,
  // not be stuck at the full where because only whole-exists removal was tried.
  for (let i = 0; i < ex.length; i++) {
    const e = ex[i]!
    if (e.where) {
      out.push({ ...spec, exists: ex.map((x, j) => (j === i ? omit(x, 'where') : x)) })
      for (const w of condShrinks(e.where)) {
        out.push({
          ...spec,
          exists: ex.map((x, j) => (j === i ? { ...x, where: w } : x)),
        })
      }
    }
  }
  const rel = spec.related ?? []
  for (let i = 0; i < rel.length; i++) {
    const nr = rel.filter((_, j) => j !== i)
    out.push(nr.length ? { ...spec, related: nr } : omit(spec, 'related'))
  }
  for (let i = 0; i < rel.length; i++) {
    const child = rel[i]!
    const info = RELATIONSHIPS[table]?.[child.rel]
    if (child.sub && info) {
      for (const s of oneStepShrinksSub(child.sub, info.child, info.card)) {
        out.push({
          ...spec,
          related: rel.map((r, j) => (j === i ? { ...r, sub: s } : r)),
        })
      }
    }
  }
  if (spec.where) {
    out.push(omit(spec, 'where'))
    for (const w of condShrinks(spec.where)) out.push({ ...spec, where: w })
  }
  // deterministic + canonical-deduped; defensively drop any candidate that is
  // not strictly smaller.
  const base = constructCount(spec)
  const seen = new Set<string>()
  const deduped: GenSpec[] = []
  for (const c of out) {
    if (constructCount(c) >= base) continue
    const key = canonical(c)
    if (seen.has(key)) continue
    seen.add(key)
    deduped.push(c)
  }
  return deduped
}

// ---------------------------------------------------------------------------
// greedy delta-debug to a fixpoint, bounded by an explicit evaluation budget.
// complete:true ONLY when a full candidate pass reaches a fixpoint. budget
// exhaustion returns complete:false — NOT a claimed local minimum.
// ---------------------------------------------------------------------------
export async function shrinkSpec(
  spec: GenSpec,
  stillDiverges: (s: GenSpec) => Promise<boolean>,
  budget: number
): Promise<{ spec: GenSpec; evaluations: number; complete: boolean }> {
  if (!Number.isSafeInteger(budget) || budget < 0 || Object.is(budget, -0)) {
    throw new Error(
      `shrinkSpec budget must be a safe non-negative integer, got ${budget}`
    )
  }
  let current = spec
  let evaluations = 0
  for (;;) {
    let advanced = false
    for (const cand of oneStepShrinks(current)) {
      if (evaluations >= budget) return { spec: current, evaluations, complete: false }
      evaluations++
      if (await stillDiverges(cand)) {
        current = cand
        advanced = true
        break
      }
    }
    if (!advanced) return { spec: current, evaluations, complete: true }
  }
}
