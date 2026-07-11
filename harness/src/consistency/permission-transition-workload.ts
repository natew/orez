// Pure, dependency-light helpers for the populated-cache permission-transition
// live lane: a deterministic scenario derived from a seed, the projection from
// observed client view rows into the checker's sorted-unique row/marker shape,
// scenario self-validation, and the replay command. No @rocicorp/zero import so
// this stays unit-testable without booting a sync stack.
import { createHash } from 'node:crypto'

import {
  PERMISSION_TRANSITION_PROFILE,
  PROTECTED_IDS,
  protectedRowId,
  type ProtectedView,
} from './permission-transition.js'

// mount route segment grammar (createSyncServerMount): databaseIDs must match.
const DATABASE_ID = /^[A-Za-z0-9_-]{1,64}$/

export type PermissionScenario = {
  seed: string
  digest: string
  namespaces: { transition: string; stable: string }
  principals: { owner: string; subjectTransition: string; subjectStable: string }
  markers: { transition: string; stable: string }
  // one fresh sentinel marker per epoch (0, 1, 2)
  sentinelMarkers: [string, string, string]
  // protected ids (identical across namespaces) and the disjoint sentinel ids
  protectedIds: typeof PROTECTED_IDS
  sentinel: {
    project: string
    task: string
    members: { transition: string[]; stable: string[] }
  }
}

function nonempty(value: unknown, label: string): asserts value is string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`${label} must be a nonempty string`)
  }
}

export function derivePermissionScenario(seed: string): PermissionScenario {
  nonempty(seed, 'seed')
  const digest = createHash('sha256').update(seed).digest('hex').slice(0, 16)
  return {
    seed,
    digest,
    namespaces: { transition: `nsa-${digest}`, stable: `nsb-${digest}` },
    principals: {
      owner: `owner-${digest}`,
      subjectTransition: `subject-a-${digest}`,
      subjectStable: `subject-b-${digest}`,
    },
    markers: { transition: `mk-a-${digest}`, stable: `mk-b-${digest}` },
    sentinelMarkers: [`sn0-${digest}`, `sn1-${digest}`, `sn2-${digest}`],
    protectedIds: PROTECTED_IDS,
    sentinel: {
      project: 'sn-project',
      task: 'sn-task',
      members: {
        transition: ['sn-owner', 'sn-subject-a'],
        stable: ['sn-subject-b'],
      },
    },
  }
}

// The sentinel ACL identities (checker `member:<id>` form), sorted unique, that
// an oracle read must return unchanged at every epoch.
export function sentinelAclRows(
  scenario: PermissionScenario,
  ns: 'transition' | 'stable'
): string[] {
  return scenario.sentinel.members[ns].map((id) => protectedRowId('member', id)).sort()
}

// A member row has no marker column, so its marker is absent; project and task
// rows carry the namespace marker in their name/title.
export type ObservedRow = { id: string; marker?: string }
export type ProtectedObservation = Record<ProtectedView, ObservedRow[]>

// Faithfully encode what the three protected views returned into the checker's
// sorted-unique rows (`${view}:${id}`) and markers. It does not filter to the
// expected ids: a leaked or contaminated row is encoded and left for the
// checker to reject.
export function projectProtectedObservation(observed: ProtectedObservation): {
  rows: string[]
  markers: string[]
} {
  const rows = new Set<string>()
  const markers = new Set<string>()
  for (const view of Object.keys(PROTECTED_IDS) as ProtectedView[]) {
    for (const row of observed[view]) {
      nonempty(row.id, `${view} row id`)
      rows.add(protectedRowId(view, row.id))
      if (row.marker !== undefined) {
        nonempty(row.marker, `${view} row marker`)
        markers.add(row.marker)
      }
    }
  }
  return {
    rows: [...rows].sort(),
    markers: [...markers].sort(),
  }
}

export function validatePermissionScenario(scenario: PermissionScenario): void {
  const namespaces = [scenario.namespaces.transition, scenario.namespaces.stable]
  for (const [index, ns] of namespaces.entries()) {
    nonempty(ns, `namespace ${index}`)
    if (!DATABASE_ID.test(ns)) {
      throw new Error(`namespace ${ns} is not a valid mount database id`)
    }
  }
  if (namespaces[0] === namespaces[1]) {
    throw new Error('the two namespaces must be distinct')
  }

  const principals = [
    scenario.principals.owner,
    scenario.principals.subjectTransition,
    scenario.principals.subjectStable,
  ]
  const principalSet = new Set(principals)
  for (const principal of principals) nonempty(principal, 'principal')
  if (principalSet.size !== principals.length) {
    throw new Error('participating principals must be distinct')
  }

  nonempty(scenario.markers.transition, 'transition marker')
  nonempty(scenario.markers.stable, 'stable marker')
  if (scenario.markers.transition === scenario.markers.stable) {
    throw new Error('the two namespaces must carry distinct markers')
  }

  const sentinelMarkers = new Set(scenario.sentinelMarkers)
  if (scenario.sentinelMarkers.length !== 3 || sentinelMarkers.size !== 3) {
    throw new Error('each epoch requires a distinct fresh sentinel marker')
  }

  const protectedIds = Object.values(scenario.protectedIds)
  const sentinelIds = [
    scenario.sentinel.project,
    scenario.sentinel.task,
    ...scenario.sentinel.members.transition,
    ...scenario.sentinel.members.stable,
  ]
  for (const id of [...protectedIds, ...sentinelIds]) nonempty(id, 'scope id')
  const overlap = protectedIds.filter((id) => sentinelIds.includes(id))
  if (overlap.length > 0) {
    throw new Error(
      `sentinel scope is not disjoint from protected ids: ${overlap.join(', ')}`
    )
  }
  if (
    scenario.sentinel.members.transition.length === 0 ||
    scenario.sentinel.members.stable.length === 0
  ) {
    throw new Error('the sentinel scope must grant every participant in both namespaces')
  }
}

export function permissionReplayCommand(target: string, seed: string): string {
  return `bun src/permission-transition-lane.ts --target ${target} --seed=${seed} --replay`
}

export { PERMISSION_TRANSITION_PROFILE }
