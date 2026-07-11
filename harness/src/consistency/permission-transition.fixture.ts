// Shared test scaffolding for the permission-transition checker and its
// artifact writer: the canonical frozen valid history plus small mutation
// helpers. Not imported by any production module (name ends in .fixture).
import {
  PERMISSION_HISTORY_SCHEMA_VERSION,
  type PermissionEvent,
} from './permission-transition.js'

// protected identity universe (same ids in both namespaces, distinct markers)
export const PROJECT = 'project:pt-project'
export const TASK = 'task:pt-task'
export const MEMBER = 'member:pt-member'
export const OWNER_BASE = [PROJECT, TASK].sort()
export const FULL = [MEMBER, PROJECT, TASK].sort()

type Spec = Omit<PermissionEvent, 'v' | 'index' | 'host'>

// Two child namespaces on one host: nsA runs the grant->revoke transition, nsB
// stays authorized. Original owner/subject A + subject B clients live across
// every epoch; fresh clients appear after the grant and after the revoke. A
// disjoint sentinel scope permanently grants every participant; its ACL never
// drifts while the marker advances.
export function validPermissionHistory(): PermissionEvent[] {
  const raw: Spec[] = []
  const ownerRows = (epoch: 0 | 1 | 2) => (epoch === 1 ? FULL : OWNER_BASE)
  const subjectRows = (epoch: 0 | 1 | 2) => (epoch === 1 ? FULL : [])

  raw.push({
    type: 'authority',
    scope: 'protected-membership',
    opId: 'auth-grant',
    epoch: 1,
    namespace: 'nsA',
    principal: 'subjectA',
    count: 1,
  })
  raw.push({
    type: 'authority',
    scope: 'protected-membership',
    opId: 'auth-revoke',
    epoch: 2,
    namespace: 'nsA',
    principal: 'subjectA',
    count: 0,
  })
  raw.push({
    type: 'change',
    opId: 'grant',
    epoch: 1,
    namespace: 'nsA',
    principal: 'subjectA',
    action: 'grant',
    phase: 'ok',
    sqlReturned: true,
    authorityRef: 'auth-grant',
  })
  raw.push({
    type: 'change',
    opId: 'revoke',
    epoch: 2,
    namespace: 'nsA',
    principal: 'subjectA',
    action: 'revoke',
    phase: 'ok',
    sqlReturned: true,
    authorityRef: 'auth-revoke',
  })
  for (const epoch of [0, 1, 2] as const) {
    raw.push({
      type: 'authority',
      scope: 'sentinel-acl',
      opId: `acl-A-${epoch}`,
      epoch,
      namespace: 'nsA',
      rows: ['member:sn-ownerA', 'member:sn-subjectA'],
    })
    raw.push({
      type: 'authority',
      scope: 'sentinel-acl',
      opId: `acl-B-${epoch}`,
      epoch,
      namespace: 'nsB',
      rows: ['member:sn-subjectB'],
    })
  }

  const client = (
    id: string,
    ns: string,
    principal: string,
    fresh: boolean,
    epoch: 0 | 1 | 2,
    rows: string[],
    markers: string[]
  ) => {
    for (const origin of ['named', 'raw'] as const) {
      raw.push({
        type: 'client',
        opId: `${id}-${origin}-${epoch}`,
        epoch,
        namespace: ns,
        principal,
        clientId: id,
        groupId: `${id}-g`,
        storageKey: `${id}-sk`,
        origin,
        rows,
        markers,
        sentinelMarker: `sn-${epoch}`,
        complete: true,
        fresh,
        pullEchoed: true,
      })
    }
  }

  for (const epoch of [0, 1, 2] as const) {
    client('cAo', 'nsA', 'ownerA', false, epoch, ownerRows(epoch), ['mk-A'])
    const sr = subjectRows(epoch)
    client('cAs', 'nsA', 'subjectA', false, epoch, sr, sr.length ? ['mk-A'] : [])
    client('cBs', 'nsB', 'subjectB', false, epoch, FULL, ['mk-B'])
  }
  client('fAs1', 'nsA', 'subjectA', true, 1, FULL, ['mk-A'])
  client('fBs1', 'nsB', 'subjectB', true, 1, FULL, ['mk-B'])
  client('fAs2', 'nsA', 'subjectA', true, 2, [], [])
  client('fBs2', 'nsB', 'subjectB', true, 2, FULL, ['mk-B'])

  // observationRefs are exactly the client observation opIds present at each
  // epoch, derived from what was pushed above so the barrier can never drift
  const observationRefsAt = (epoch: 0 | 1 | 2): string[] =>
    raw
      .filter((e) => e.type === 'client' && e.epoch === epoch)
      .map((e) => e.opId)
      .sort()

  raw.push({
    type: 'barrier',
    opId: 'bar-0',
    epoch: 0,
    marker: 'sn-0',
    complete: true,
    observers: ['cAo', 'cAs', 'cBs'],
    observationRefs: observationRefsAt(0),
    changeRef: null,
    authorityRef: null,
  })
  raw.push({
    type: 'barrier',
    opId: 'bar-1',
    epoch: 1,
    marker: 'sn-1',
    complete: true,
    observers: ['cAo', 'cAs', 'cBs', 'fAs1', 'fBs1'],
    observationRefs: observationRefsAt(1),
    changeRef: 'grant',
    authorityRef: 'auth-grant',
  })
  raw.push({
    type: 'barrier',
    opId: 'bar-2',
    epoch: 2,
    marker: 'sn-2',
    complete: true,
    observers: ['cAo', 'cAs', 'cBs', 'fAs2', 'fBs2'],
    observationRefs: observationRefsAt(2),
    changeRef: 'revoke',
    authorityRef: 'auth-revoke',
  })

  return raw.map((e, index) => ({
    v: PERMISSION_HISTORY_SCHEMA_VERSION,
    host: 'H',
    index,
    ...e,
  })) as PermissionEvent[]
}

export function reindex(events: PermissionEvent[]): PermissionEvent[] {
  return events.map((e, index) => ({ ...e, index })) as PermissionEvent[]
}

export function patch(
  events: PermissionEvent[],
  opId: string,
  changes: Record<string, unknown>
): PermissionEvent[] {
  return events.map((e) =>
    e.opId === opId ? ({ ...e, ...changes } as PermissionEvent) : e
  )
}
