// HTTP pull cookie-contract conformance. The runner owns only the protocol
// sequence; adapters own endpoint auth, state changes, and row normalization,
// so an external implementation with a different schema can use the same lane.
//
//   bun src/pull-dialect-conformance.ts --target rust-local
import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { startRustLocal } from './targets/rust-local.js'

type JsonObject = Record<string, unknown>

export type PullResult = {
  status: number
  body: JsonObject
}

export type RowPatch =
  | { op: 'clear' }
  | { op: 'put'; key: string; value: JsonObject }
  | { op: 'del'; key: string }

export type PullDialectTarget = {
  readonly name: string
  pull(cookie: number | null): Promise<PullResult>
  change(label: string): Promise<void>
  readRows(): Promise<JsonObject[]>
  rowPatches(body: JsonObject): RowPatch[]
  restart(): Promise<void>
  close(): Promise<void>
}

type CheckReceipt = {
  check: string
  result: 'PASS' | 'FAIL'
  input: string[]
  evidence?: JsonObject
  error?: string
}

const checkInputs: Record<string, string[]> = {
  'cookies-strictly-ascend-across-state-changes': [
    'raw HTTP pull responses',
    'adapter-confirmed state changes',
  ],
  'unchanged-pull-keeps-identical-cookie': ['raw HTTP pull responses'],
  'older-cookie-converges-with-fresh-rows': [
    'raw HTTP pull responses',
    'adapter-normalized row patches',
    'fresh authority read',
  ],
  'foreign-cookie-resets-once-then-joins-sequence': ['raw HTTP pull responses'],
  'restart-never-regresses-served-cookie': [
    'raw HTTP pull responses',
    'same-store target restart',
  ],
  'max-safe-cookie-cannot-wedge-next-state-change': [
    'raw HTTP pull responses',
    'adapter-confirmed state change',
  ],
}

function object(value: unknown, label: string): JsonObject {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`${label} is not an object`)
  }
  return value as JsonObject
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}

function cookieOf(response: PullResult, label: string): number {
  if (response.status !== 200) {
    throw new Error(
      `${label} returned HTTP ${response.status}: ${JSON.stringify(response.body)}`
    )
  }
  const cookie = response.body.cookie
  if (!Number.isSafeInteger(cookie) || (cookie as number) < 0) {
    throw new Error(`${label} returned an invalid cookie: ${String(cookie)}`)
  }
  return cookie as number
}

function stable(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stable)
  if (typeof value !== 'object' || value === null) return value
  return Object.fromEntries(
    Object.entries(value as JsonObject)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => [key, stable(entry)])
  )
}

function canonicalRows(rows: Iterable<JsonObject>): string[] {
  return [...rows].map((row) => JSON.stringify(stable(row))).sort()
}

function applyPatches(
  rows: Map<string, JsonObject>,
  patches: RowPatch[]
): Map<string, JsonObject> {
  for (const patch of patches) {
    if (patch.op === 'clear') rows.clear()
    else if (patch.op === 'put') rows.set(patch.key, patch.value)
    else rows.delete(patch.key)
  }
  return rows
}

function cookieEvidence(cookie: number) {
  return String(cookie)
}

export async function runPullDialectConformance(target: PullDialectTarget) {
  const receipts: CheckReceipt[] = []
  const startedAt = new Date().toISOString()
  const started = performance.now()
  let failed = false

  async function check(
    name: keyof typeof checkInputs,
    probe: () => Promise<JsonObject>
  ): Promise<JsonObject> {
    try {
      const evidence = await probe()
      const receipt: CheckReceipt = {
        check: name,
        result: 'PASS',
        input: checkInputs[name],
        evidence,
      }
      receipts.push(receipt)
      console.log(JSON.stringify({ lane: 'http-pull-dialect-conformance', ...receipt }))
      return evidence
    } catch (error) {
      const receipt: CheckReceipt = {
        check: name,
        result: 'FAIL',
        input: checkInputs[name],
        error: errorMessage(error),
      }
      receipts.push(receipt)
      console.error(JSON.stringify({ lane: 'http-pull-dialect-conformance', ...receipt }))
      throw error
    }
  }

  try {
    let baselineBody: JsonObject = {}
    let baselineCookie = -1
    let currentCookie = -1

    await check('cookies-strictly-ascend-across-state-changes', async () => {
      const baseline = await target.pull(null)
      baselineBody = baseline.body
      baselineCookie = cookieOf(baseline, 'baseline pull')

      await target.change('ascending-a')
      const first = cookieOf(await target.pull(baselineCookie), 'first changed pull')
      if (first <= baselineCookie) {
        throw new Error(
          `cookie did not ascend after first change: ${baselineCookie} -> ${first}`
        )
      }

      await target.change('ascending-b')
      const second = cookieOf(await target.pull(first), 'second changed pull')
      if (second <= first) {
        throw new Error(
          `cookie did not ascend after second change: ${first} -> ${second}`
        )
      }
      currentCookie = second
      return {
        cookies: [baselineCookie, first, second].map(cookieEvidence),
        stateChanges: 2,
      }
    })

    await check('unchanged-pull-keeps-identical-cookie', async () => {
      const unchanged = await target.pull(currentCookie)
      const cookie = cookieOf(unchanged, 'unchanged pull')
      if (cookie !== currentCookie) {
        throw new Error(`unchanged pull changed cookie: ${currentCookie} -> ${cookie}`)
      }
      if (unchanged.body.unchanged !== true) {
        throw new Error(
          `same-cookie pull was not marked unchanged: ${JSON.stringify(unchanged.body)}`
        )
      }
      return {
        before: cookieEvidence(currentCookie),
        after: cookieEvidence(cookie),
      }
    })

    await check('older-cookie-converges-with-fresh-rows', async () => {
      const oldRows = applyPatches(new Map(), target.rowPatches(baselineBody))
      const catchup = await target.pull(baselineCookie)
      currentCookie = cookieOf(catchup, 'older-cookie catch-up pull')
      applyPatches(oldRows, target.rowPatches(catchup.body))

      const fresh = await target.pull(null)
      const freshCookie = cookieOf(fresh, 'fresh comparison pull')
      const freshRows = applyPatches(new Map(), target.rowPatches(fresh.body))
      const authorityRows = await target.readRows()
      const oldCanonical = canonicalRows(oldRows.values())
      const freshCanonical = canonicalRows(freshRows.values())
      const authorityCanonical = canonicalRows(authorityRows)
      if (JSON.stringify(oldCanonical) !== JSON.stringify(freshCanonical)) {
        throw new Error(
          `older client diverged from fresh pull: old=${JSON.stringify(oldCanonical)} fresh=${JSON.stringify(freshCanonical)}`
        )
      }
      if (JSON.stringify(freshCanonical) !== JSON.stringify(authorityCanonical)) {
        throw new Error(
          `fresh pull diverged from authority: fresh=${JSON.stringify(freshCanonical)} authority=${JSON.stringify(authorityCanonical)}`
        )
      }
      currentCookie = freshCookie
      return {
        oldCookie: cookieEvidence(baselineCookie),
        convergedCookie: cookieEvidence(freshCookie),
        rowCount: freshCanonical.length,
      }
    })

    await check('foreign-cookie-resets-once-then-joins-sequence', async () => {
      const foreignCookie = 8_000_000_000_000_000
      let resets = 0
      const rejected = await target.pull(foreignCookie)
      if (rejected.status === 409) resets++
      else {
        throw new Error(
          `foreign cookie expected one HTTP 409 reset, got ${rejected.status}: ${JSON.stringify(rejected.body)}`
        )
      }

      const recovered = await target.pull(null)
      if (recovered.status === 409) resets++
      const recoveredCookie = cookieOf(recovered, 'foreign-cookie recovery pull')
      const sequenced = await target.pull(recoveredCookie)
      if (sequenced.status === 409) resets++
      const sequencedCookie = cookieOf(sequenced, 'foreign-cookie sequenced pull')
      if (resets !== 1)
        throw new Error(`foreign cookie produced ${resets} reset responses`)
      if (sequencedCookie !== recoveredCookie || sequenced.body.unchanged !== true) {
        throw new Error(
          `client did not join sequence after reset: ${recoveredCookie} -> ${sequencedCookie}`
        )
      }
      currentCookie = sequencedCookie
      return {
        foreignCookie: cookieEvidence(foreignCookie),
        resetStatus: rejected.status,
        resetResponses: resets,
        sequenceCookie: cookieEvidence(sequencedCookie),
      }
    })

    await check('restart-never-regresses-served-cookie', async () => {
      const before = currentCookie
      await target.restart()
      const after = cookieOf(await target.pull(before), 'post-restart pull')
      if (after < before)
        throw new Error(`served cookie regressed across restart: ${before} -> ${after}`)
      currentCookie = after
      return { before: cookieEvidence(before), after: cookieEvidence(after) }
    })

    await check('max-safe-cookie-cannot-wedge-next-state-change', async () => {
      const rejected = await target.pull(Number.MAX_SAFE_INTEGER)
      if (rejected.status !== 409) {
        throw new Error(
          `MAX_SAFE_INTEGER cookie expected HTTP 409 reset, got ${rejected.status}: ${JSON.stringify(rejected.body)}`
        )
      }
      const recoveredCookie = cookieOf(
        await target.pull(null),
        'MAX_SAFE_INTEGER recovery pull'
      )
      await target.change('max-safe-recovery')
      const advanced = cookieOf(
        await target.pull(recoveredCookie),
        'post-MAX_SAFE_INTEGER changed pull'
      )
      if (advanced <= recoveredCookie) {
        throw new Error(
          `cookie did not advance after MAX_SAFE_INTEGER recovery: ${recoveredCookie} -> ${advanced}`
        )
      }
      currentCookie = advanced
      return {
        rejectedCookie: cookieEvidence(Number.MAX_SAFE_INTEGER),
        resetStatus: rejected.status,
        recoveredCookie: cookieEvidence(recoveredCookie),
        advancedCookie: cookieEvidence(advanced),
      }
    })
  } catch {
    failed = true
  } finally {
    const result = failed ? 'FAIL' : 'PASS'
    const artifact = {
      schemaVersion: 1,
      lane: 'http-pull-dialect-conformance',
      target: target.name,
      startedAt,
      finishedAt: new Date().toISOString(),
      durationMs: Math.round(performance.now() - started),
      result,
      checks: receipts,
    }
    const directory = join(process.cwd(), 'target', 'consistency')
    mkdirSync(directory, { recursive: true })
    const artifactPath = join(directory, `pull-dialect-${target.name}.json`)
    writeFileSync(artifactPath, `${JSON.stringify(artifact, null, 2)}\n`)
    console.log(
      JSON.stringify({
        lane: artifact.lane,
        target: target.name,
        result,
        checks: receipts.length,
        artifact: artifactPath,
        durationMs: artifact.durationMs,
      })
    )
    await target.close()
  }

  if (failed) throw new Error('HTTP pull dialect conformance failed')
  return receipts
}

function rustLocalRowPatches(body: JsonObject): RowPatch[] {
  const raw = body.rowsPatch
  if (!Array.isArray(raw)) return []
  const patches: RowPatch[] = []
  for (const entry of raw) {
    const patch = object(entry, 'rowsPatch entry')
    if (patch.op === 'clear') {
      patches.push({ op: 'clear' })
      continue
    }
    if (patch.tableName !== 'project') continue
    if (patch.op === 'put') {
      const value = object(patch.value, 'project put value')
      if (value.ownerId !== 'u0') continue
      if (typeof value.id !== 'string') throw new Error('project put has no string id')
      patches.push({ op: 'put', key: value.id, value })
      continue
    }
    if (patch.op === 'del') {
      const id =
        typeof patch.id === 'string'
          ? patch.id
          : typeof patch.id === 'object' && patch.id !== null
            ? (patch.id as JsonObject).id
            : undefined
      if (typeof id !== 'string') throw new Error('project delete has no string id')
      patches.push({ op: 'del', key: id })
    }
  }
  return patches
}

export async function startRustLocalPullDialectTarget(): Promise<PullDialectTarget> {
  const target = await startRustLocal({ pullIntervalMs: 0 })
  const stamp = crypto.randomUUID()
  const clientID = `pull-dialect-${stamp}`
  const clientGroupID = `pull-dialect-group-${stamp}`

  return {
    name: target.name,
    async pull(cookie) {
      const response = await fetch(`${target.origin}/pull`, {
        method: 'POST',
        headers: {
          authorization: 'Bearer token-u0',
          'content-type': 'application/json',
        },
        body: JSON.stringify({ clientID, clientGroupID, cookie }),
        signal: AbortSignal.timeout(10_000),
      })
      const body = object(await response.json(), 'pull response')
      return { status: response.status, body }
    },
    async change(label) {
      const id = `pull-dialect-${label}-${stamp}`
      await target.sql(
        `INSERT INTO project (id, "ownerId", name) VALUES ('${id}', 'u0', '${label}')`
      )
    },
    readRows() {
      return target.oracle(
        `SELECT id, "ownerId", name FROM project WHERE "ownerId" = 'u0' ORDER BY id`
      )
    },
    rowPatches: rustLocalRowPatches,
    restart: () => target.restart(),
    close: () => target.close(),
  }
}

async function main() {
  const { values: args } = parseArgs({
    options: { target: { type: 'string', default: 'rust-local' } },
  })
  if (args.target !== 'rust-local') {
    throw new Error(`pull dialect lane supports only rust-local, got '${args.target}'`)
  }
  await runPullDialectConformance(await startRustLocalPullDialectTarget())
}

if (import.meta.main) {
  await main()
}
