// Capped-diff cut lane: the one system lane that pulls with a row cap small
// enough to split a mutation's row effect from its lmid ack across two pulls.
// This is the system-level net for the two engine cut-path bugs that only
// cargo caught before (see docs/sync/mutation-matrix.md, M4 and O2):
//
//   M4 — lmid advances in _zsync_clients but no lmid change row is written, so
//        the ack never ships. Caught here: the ack never arrives on any pull.
//   O2 — acks are derived from the full raw scan beyond the cut, so an ack
//        leads its effect. Caught here: the first capped diff carries the ack
//        together with (or ahead of) the effect it should trail.
//
// Every observation comes from a raw HTTP pull as a NON-writing observer client
// in the writer's group (server-confirmed state, never the writer's optimistic
// overlay). The writer's row effect and its lmid ride separate change rows, so
// a max-change-rows=1 host cuts between them exactly as the cargo
// engine-invariant tests do at the library boundary.
//
//   bun src/capped-diff-lane.ts --target rust-local
import { parseArgs } from 'node:util'

import { mutators } from './fixture.js'
import { assertServerOutcome } from './server-outcome.js'
import { startRustLocal } from './targets/rust-local.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'rust-local' },
  },
})
if (args.target !== 'rust-local') {
  throw new Error(`capped-diff lane supports only rust-local, got '${args.target}'`)
}

type PullResponse = {
  cookie: number | null
  unchanged?: boolean
  lastMutationIDChanges?: Record<string, number>
  rowsPatch?: Array<{ op: string }>
}

const target = await startRustLocal({
  // the whole point of this lane: a one-row diff cap so effect and ack cut apart
  maxChangeRows: 1,
  // drive pulls by hand; no background polling to race the raw observer
  pullIntervalMs: 0,
})

let failed = false
try {
  const writerUser = 'capped-writer'
  const writer = target.createClient(writerUser)
  const writerId = writer.clientID
  const group = await writer.clientGroupID
  // a second client (tab) of the SAME user shares the writer's client group, so
  // it derives the writer's lmid acks; a different user would be a foreign group.
  const observerId = 'capped-observer'

  // raw pull as the observer, in the writer's group, reading server-confirmed
  // state directly off the pull dialect (not the writer's optimistic cache).
  async function rawPull(cookie: number | null): Promise<PullResponse> {
    const response = await fetch(`${target.origin}/pull`, {
      method: 'POST',
      headers: {
        authorization: `Bearer token-${writerUser}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ clientID: observerId, clientGroupID: group, cookie }),
    })
    if (!response.ok)
      throw new Error(`observer pull HTTP ${response.status}: ${await response.text()}`)
    return (await response.json()) as PullResponse
  }
  const ackOf = (pull: PullResponse) => pull.lastMutationIDChanges?.[writerId]
  const putCount = (pull: PullResponse) =>
    (pull.rowsPatch ?? []).filter((op) => op.op === 'put').length

  // mutation A: establish a non-zero baseline watermark for the observer to
  // diff from. its effect + ack are already settled before the probe runs.
  const seed = writer.mutate(
    mutators.task.create({
      id: 'capped-seed',
      projectId: 'p0',
      title: 'capped seed',
      rank: 1,
      done: false,
    })
  )
  await assertServerOutcome(seed.server, 'success', 'task.create capped-seed')
  await seed.client

  // baseline snapshot (cookie=null): full state, absolute lmids. captures the
  // writer's acked lmid after A and a numeric cookie to diff from.
  const baseline = await rawPull(null)
  const baseCookie = baseline.cookie
  const baseAck = ackOf(baseline)
  if (typeof baseCookie !== 'number') {
    throw new Error(`baseline snapshot cookie is not numeric: ${String(baseCookie)}`)
  }
  if (typeof baseAck !== 'number') {
    throw new Error('baseline snapshot did not carry the writer lmid ack')
  }

  // mutation B (the probe): one row effect + one lmid ack, two change rows.
  const probe = writer.mutate(
    mutators.task.create({
      id: 'capped-probe',
      projectId: 'p0',
      title: 'capped probe',
      rank: 2,
      done: false,
    })
  )
  await assertServerOutcome(probe.server, 'success', 'task.create capped-probe')
  await probe.client

  // first capped diff from the baseline cookie. the cap admits ONE change row:
  // the probe's effect. its lmid ack rides the next watermark, beyond the cut.
  const firstDiff = await rawPull(baseCookie)
  if (putCount(firstDiff) < 1) {
    throw new Error(
      `first capped diff did not deliver the probe effect (rowsPatch ${JSON.stringify(firstDiff.rowsPatch)})`
    )
  }
  const firstAck = ackOf(firstDiff)
  if (firstAck !== undefined && firstAck > baseAck) {
    // O2: the ack was derived from the full raw scan beyond the cut, so it
    // shipped together with — or ahead of — the effect it must trail.
    throw new Error(
      `O2 ack-beyond-cap: first capped diff delivered the probe effect AND lmid ack ${firstAck} ` +
        `(baseline ack ${baseAck}); an ack led its effect under a one-row cap`
    )
  }

  // second capped diff: now the lmid change row is inside the cut, so the ack
  // must finally arrive and advance past the baseline.
  const secondDiff = await rawPull(firstDiff.cookie)
  const secondAck = ackOf(secondDiff)
  if (secondAck === undefined || secondAck <= baseAck) {
    // M4: the lmid change row was never written, so the ack ships on no pull.
    throw new Error(
      `M4 lmid-no-change-row: the probe's lmid ack never shipped ` +
        `(baseline ack ${baseAck}, second diff ack ${String(secondAck)}); ` +
        `the effect committed but its ack is unreachable`
    )
  }

  console.log(
    `[capped-diff] PASS target=${target.name} maxChangeRows=1 ` +
      `baseAck=${baseAck} effect-then-ack cut across two pulls (firstAck=${String(firstAck)}, secondAck=${secondAck})`
  )
} catch (error) {
  failed = true
  console.error('[capped-diff] FAIL:', error)
} finally {
  await target.close()
}

process.exit(failed ? 1 : 0)
