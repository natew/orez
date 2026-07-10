// Real shared-client-group lane: multiple stock Zero instances use the same
// persisted storage key concurrently, like browser tabs. Verifies distinct
// client IDs join one client group, concurrent pushes settle independently,
// group LMIDs include every tab, and a replacement tab resumes the group.
//
//   bun src/multi-tab.ts
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { mutators, queries } from './fixture.js'
import { persistentKVStoreProvider } from './persistent-kv.js'
import { assertServerOutcome } from './server-outcome.js'
import { startOrezLocal } from './targets/orez-local.js'

import type { FixtureZero } from './target.js'

type ProjectRow = { id: string }

function watchProjects(zero: FixtureZero) {
  const view = zero.materialize(queries.allProjects())
  let rows: ProjectRow[] = []
  let complete = false
  let destroyed = false
  view.addListener((data, resultType) => {
    rows = JSON.parse(JSON.stringify(data)) as ProjectRow[]
    if (resultType === 'complete') complete = true
  })
  return {
    get complete() {
      return complete
    },
    hasEvery(ids: string[]) {
      const present = new Set(rows.map(({ id }) => id))
      return ids.every((id) => present.has(id))
    },
    destroy() {
      if (destroyed) return
      destroyed = true
      view.destroy()
    },
  }
}

async function eventually(check: () => void, label: string, timeoutMs = 30_000) {
  const started = Date.now()
  let lastError: unknown
  while (Date.now() - started < timeoutMs) {
    try {
      check()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
  }
  throw new Error(`timeout waiting for ${label}: ${String(lastError)}`)
}

async function mutateProjects(
  tab: string,
  zero: FixtureZero,
  count: number,
  allIDs: string[]
) {
  const clientApplies: Promise<unknown>[] = []
  const serverOutcomes: Promise<void>[] = []
  for (let i = 0; i < count; i++) {
    const id = `multi-${tab}-${i}`
    allIDs.push(id)
    const request = zero.mutate(
      mutators.project.create({ id, ownerId: 'u0', name: `multi tab ${tab}.${i}` })
    )
    clientApplies.push(request.client)
    serverOutcomes.push(assertServerOutcome(request.server, 'success', id))
  }
  await Promise.all(clientApplies)
  await Promise.all(serverOutcomes)
}

const storageDir = mkdtempSync(join(tmpdir(), 'zharness-tabs-'))
const kvStore = persistentKVStoreProvider(storageDir)
const storageKey = `zharness-tabs-${Date.now()}`
const target = await startOrezLocal({ pullIntervalMs: 75 })
const views: ReturnType<typeof watchProjects>[] = []

try {
  const tabA = target.createClient('u0', { kvStore, storageKey })
  const tabB = target.createClient('u0', { kvStore, storageKey })
  const viewA = watchProjects(tabA)
  const viewB = watchProjects(tabB)
  views.push(viewA, viewB)

  const [groupA, groupB] = await Promise.all([tabA.clientGroupID, tabB.clientGroupID])
  if (groupA !== groupB)
    throw new Error(`tabs forked client groups: ${groupA} / ${groupB}`)
  if (tabA.clientID === tabB.clientID) throw new Error('tabs reused one clientID')

  await eventually(() => {
    if (!viewA.complete || !viewB.complete)
      throw new Error('initial tab views incomplete')
  }, 'two-tab hydration')

  const allIDs: string[] = []
  await Promise.all([
    mutateProjects('a', tabA, 6, allIDs),
    mutateProjects('b', tabB, 6, allIDs),
    target.sql(
      `INSERT INTO project (id, "ownerId", name) VALUES ('multi-upstream', 'u0', 'multi tab upstream')`
    ),
  ])
  allIDs.push('multi-upstream')

  await eventually(() => {
    if (!viewA.hasEvery(allIDs)) throw new Error('tab A has not converged')
    if (!viewB.hasEvery(allIDs)) throw new Error('tab B has not converged')
  }, 'two-tab convergence')

  const initialLMIDs = await target.oracle(
    `SELECT clientID, lastMutationID FROM _zsync_clients
     WHERE clientGroupID = '${groupA}' ORDER BY clientID`
  )
  const initialByClient = new Map(
    initialLMIDs.map((row) => [String(row.clientID), Number(row.lastMutationID)])
  )
  if (
    initialByClient.get(tabA.clientID) !== 6 ||
    initialByClient.get(tabB.clientID) !== 6
  ) {
    throw new Error(`group LMIDs missing tabs: ${JSON.stringify(initialLMIDs)}`)
  }
  console.log('[multi-tab] shared group + concurrent per-tab LMIDs PASS')

  viewA.destroy()
  await tabA.close()
  const tabC = target.createClient('u0', { kvStore, storageKey })
  const viewC = watchProjects(tabC)
  views.push(viewC)
  const groupC = await tabC.clientGroupID
  if (groupC !== groupA)
    throw new Error(`replacement tab forked group ${groupA} -> ${groupC}`)
  if (tabC.clientID === tabA.clientID || tabC.clientID === tabB.clientID) {
    throw new Error('replacement tab did not get a distinct clientID')
  }

  await eventually(() => {
    if (!viewC.complete) throw new Error('replacement tab incomplete')
    if (!viewC.hasEvery(allIDs)) throw new Error('replacement tab missing persisted rows')
  }, 'replacement tab resume')

  await Promise.all([
    mutateProjects('b2', tabB, 2, allIDs),
    mutateProjects('c', tabC, 2, allIDs),
  ])
  await eventually(() => {
    if (!viewB.hasEvery(allIDs))
      throw new Error('surviving tab missed replacement writes')
    if (!viewC.hasEvery(allIDs))
      throw new Error('replacement tab missed surviving writes')
  }, 'replacement/survivor convergence')

  const finalLMIDs = await target.oracle(
    `SELECT clientID, lastMutationID FROM _zsync_clients
     WHERE clientGroupID = '${groupA}' ORDER BY clientID`
  )
  const finalByClient = new Map(
    finalLMIDs.map((row) => [String(row.clientID), Number(row.lastMutationID)])
  )
  if (finalByClient.get(tabB.clientID) !== 8 || finalByClient.get(tabC.clientID) !== 2) {
    throw new Error(`replacement group LMIDs wrong: ${JSON.stringify(finalLMIDs)}`)
  }

  const oracle = await target.oracle(
    `SELECT id FROM project WHERE id LIKE 'multi-%' ORDER BY id`
  )
  if (oracle.length !== allIDs.length) {
    throw new Error(`expected ${allIDs.length} authoritative rows, got ${oracle.length}`)
  }
  console.log('[multi-tab] replacement tab resume + survivor convergence PASS')
  console.log('[multi-tab] PASS: real persisted client group across three stock clients')
} finally {
  for (const view of views) view.destroy()
  await target.close()
  rmSync(storageDir, { recursive: true, force: true })
}
