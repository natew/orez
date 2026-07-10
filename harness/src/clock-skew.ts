// Application timestamp skew lane. Sync ordering must remain mutation-ID and
// watermark based even when application rows carry clocks far in the future or
// past.
import { parseArgs } from 'node:util'

import { mutators } from './fixture.js'
import { assertServerOutcome } from './server-outcome.js'
import { startRustCf } from './targets/rust-cf.js'
import { startRustLocal } from './targets/rust-local.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'rust-local' },
    'clock-skew-hours': { type: 'string', default: '24' },
  },
})
const hours = Number(args['clock-skew-hours'])
if (!Number.isFinite(hours) || hours <= 0)
  throw new Error('clock-skew-hours must be positive')

const target =
  args.target === 'rust-cf'
    ? await startRustCf()
    : args.target === 'rust-local'
      ? await startRustLocal()
      : (() => {
          throw new Error('target must be rust-local or rust-cf')
        })()

try {
  const zero = target.createClient('clock-user')
  const offset = Math.round(hours * 60 * 60 * 1000)
  const now = Date.now()
  const ids = [`clock-past-${crypto.randomUUID()}`, `clock-future-${crypto.randomUUID()}`]
  const due = [now - offset, now + offset]

  for (let index = 0; index < ids.length; index++) {
    const request = zero.mutate(
      mutators.task.create({
        id: ids[index]!,
        projectId: 'p0',
        title: 'clock skew probe',
        rank: index,
        done: false,
        dueAt: due[index],
      })
    )
    await request.client
    await assertServerOutcome(request.server, 'success', ids[index]!)
  }

  const rows = await target.oracle(
    `SELECT id, "dueAt" FROM task WHERE id IN ('${ids[0]}', '${ids[1]}') ORDER BY id`
  )
  if (rows.length !== 2) throw new Error(`expected two skew rows, got ${rows.length}`)
  const byID = new Map(rows.map((row) => [String(row.id), Number(row.dueAt)]))
  for (let index = 0; index < ids.length; index++) {
    if (byID.get(ids[index]!) !== due[index]) {
      throw new Error(`application timestamp changed for skew row ${index}`)
    }
  }
  const clients = await target.oracle(
    'SELECT CAST(MAX(lastMutationID) AS TEXT) AS lmid FROM _zsync_clients'
  )
  if (String(clients[0]?.lmid) !== '2') {
    throw new Error(`skewed application timestamps affected mutation ordering`)
  }

  console.log(
    JSON.stringify({
      lane: 'clock-skew',
      result: 'PASS',
      target: args.target,
      skewHours: hours,
      mutationCount: 2,
      lmid: 2,
    })
  )
} finally {
  await target.close()
}
