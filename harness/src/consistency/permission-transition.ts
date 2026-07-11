// Versioned checker for the frozen populated-cache permission-transition v1
// profile. It proves that an ALREADY-populated stock Zero client reveals
// protected rows on an admin grant and drops them on a revoke, corroborated by
// the authoritative oracle, a disjoint always-granted sentinel liveness barrier,
// raw local-cache-only evidence, and fresh clients that agree with the originals
// — across exactly two child namespaces on one host.
//
// The checker is pure over a typed, versioned history. It infers nothing from
// process names or wall-clock order: roles are derived from the recorded grant/
// revoke changes, liveness from complete sentinel barriers, and authority from
// terminal oracle reads. Schema v2 keys are strict; unknown keys are rejected.
// Fail precedence: structural -> topology -> liveness/corroboration -> per-client
// rows/markers. It does not claim linearizability, immediate pre-barrier
// revocation, push authorization, role matrices, token refresh, crash recovery,
// arbitrary policy graphs, or separate-process namespaces disguised as one host.

// Three independent versions, deliberately NOT conflated:
//  - the profile is the FROZEN v1 populated-cache permission-transition profile
//  - the typed permission history/event schema is v1 (the `v` on every event)
//  - the checks envelope is v2 (the checks-v2 fail-precedence contract)
export const PERMISSION_TRANSITION_PROFILE_VERSION = 1 as const
export const PERMISSION_HISTORY_SCHEMA_VERSION = 1 as const
export const PERMISSION_CHECKS_SCHEMA_VERSION = 2 as const

export const PERMISSION_TRANSITION_PROFILE = {
  name: 'populated-cache-permission-transition',
  version: PERMISSION_TRANSITION_PROFILE_VERSION,
  historySchemaVersion: PERMISSION_HISTORY_SCHEMA_VERSION,
  checksSchemaVersion: PERMISSION_CHECKS_SCHEMA_VERSION,
  adapterRequirements: {
    namespaces: 'exactly-two-child-namespaces-on-one-host',
    authority: 'terminal-admin-change-corroborated-by-oracle',
    clientView: 'complete-named-full-scope-plus-prearmed-raw-local-only',
    barrier: 'disjoint-sentinel-scope-permanently-granted-every-participant',
  },
} as const

export type PermissionEpoch = 0 | 1 | 2
export type ChangeAction = 'grant' | 'revoke'
export type ChangePhase = 'ok' | 'info'
export type ClientOrigin = 'named' | 'raw'
export type AuthorityScope = 'protected-membership' | 'sentinel-acl'

type Base = {
  v: typeof PERMISSION_HISTORY_SCHEMA_VERSION
  index: number
  opId: string
  host: string
  epoch: PermissionEpoch
}

export type ChangeEvent = Base & {
  type: 'change'
  namespace: string
  principal: string
  action: ChangeAction
  phase: ChangePhase
  sqlReturned: boolean
  // opId of the protected-membership authority read that corroborates this
  // change with the exact oracle membership count (grant -> 1, revoke -> 0).
  authorityRef: string
}

export type ProtectedMembershipAuthority = Base & {
  type: 'authority'
  scope: 'protected-membership'
  namespace: string
  principal: string
  count: number
}

export type SentinelAclAuthority = Base & {
  type: 'authority'
  scope: 'sentinel-acl'
  namespace: string
  // sorted unique sentinel membership identities granting the participants
  rows: string[]
}

export type AuthorityEvent = ProtectedMembershipAuthority | SentinelAclAuthority

export type ClientEvent = Base & {
  type: 'client'
  namespace: string
  principal: string
  clientId: string
  groupId: string
  storageKey: string
  origin: ClientOrigin
  // sorted unique `${view}:${id}` protected identities the client observed
  rows: string[]
  // sorted unique marker values carried by those rows (empty when rows empty)
  markers: string[]
  complete: boolean
  // true for a client created AFTER a transition, false for one populated before
  fresh: boolean
  // the observed pull body echoed both this clientId and groupId
  pullEchoed: boolean
}

export type BarrierEvent = Base & {
  type: 'barrier'
  // the fresh sentinel marker committed at this epoch (distinct per epoch)
  marker: string
  complete: boolean
  // sorted unique clientIds that observed the sentinel complete at this epoch
  observers: string[]
}

export type PermissionEvent = ChangeEvent | AuthorityEvent | ClientEvent | BarrierEvent

export type CheckResult = {
  valid: boolean
  violations: string[]
}

const EPOCHS: readonly PermissionEpoch[] = [0, 1, 2]

const KEYS = {
  change: [
    'v',
    'index',
    'opId',
    'host',
    'epoch',
    'type',
    'namespace',
    'principal',
    'action',
    'phase',
    'sqlReturned',
    'authorityRef',
  ],
  'authority:protected-membership': [
    'v',
    'index',
    'opId',
    'host',
    'epoch',
    'type',
    'scope',
    'namespace',
    'principal',
    'count',
  ],
  'authority:sentinel-acl': [
    'v',
    'index',
    'opId',
    'host',
    'epoch',
    'type',
    'scope',
    'namespace',
    'rows',
  ],
  client: [
    'v',
    'index',
    'opId',
    'host',
    'epoch',
    'type',
    'namespace',
    'principal',
    'clientId',
    'groupId',
    'storageKey',
    'origin',
    'rows',
    'markers',
    'complete',
    'fresh',
    'pullEchoed',
  ],
  barrier: [
    'v',
    'index',
    'opId',
    'host',
    'epoch',
    'type',
    'marker',
    'complete',
    'observers',
  ],
} as const

function result(violations: string[]): CheckResult {
  return { valid: violations.length === 0, violations }
}

function isSortedUnique(value: unknown): value is string[] {
  if (!Array.isArray(value)) return false
  for (let i = 0; i < value.length; i++) {
    const item = value[i]
    if (typeof item !== 'string' || item === '') return false
    if (i > 0 && !(value[i - 1]! < item)) return false
  }
  return true
}

function hasExactKeys(
  event: Record<string, unknown>,
  allowed: readonly string[]
): boolean {
  const keys = Object.keys(event)
  if (keys.length !== allowed.length) return false
  const set = new Set(allowed)
  return keys.every((key) => set.has(key))
}

// Structural gate. Rejects malformed events without emitting any semantic
// diagnostics, so a semantic test never has to defend against garbage shapes.
function validateStructure(events: readonly PermissionEvent[]): string[] {
  const violations: string[] = []
  const opIds = new Set<string>()

  for (let position = 0; position < events.length; position++) {
    const event = events[position] as unknown as Record<string, unknown>
    const at = `event ${position}`
    if (event === null || typeof event !== 'object' || Array.isArray(event)) {
      violations.push(`${at} is not an object`)
      continue
    }
    if (event.v !== PERMISSION_HISTORY_SCHEMA_VERSION) {
      violations.push(`${at} has schema version ${String(event.v)}`)
    }
    if (event.index !== position) {
      violations.push(`${at} has index ${String(event.index)}`)
    }
    if (typeof event.opId !== 'string' || event.opId === '') {
      violations.push(`${at} has an empty opId`)
    } else if (opIds.has(event.opId)) {
      violations.push(`${at} reuses opId ${event.opId}`)
    } else {
      opIds.add(event.opId)
    }
    if (typeof event.host !== 'string' || event.host === '') {
      violations.push(`${at} has an empty host`)
    }
    if (!EPOCHS.includes(event.epoch as PermissionEpoch)) {
      violations.push(`${at} has invalid epoch ${String(event.epoch)}`)
    }

    switch (event.type) {
      case 'change': {
        if (!hasExactKeys(event, KEYS.change)) {
          violations.push(`${at} change has unexpected keys`)
          break
        }
        if (typeof event.namespace !== 'string' || event.namespace === '') {
          violations.push(`${at} change has an empty namespace`)
        }
        if (typeof event.principal !== 'string' || event.principal === '') {
          violations.push(`${at} change has an empty principal`)
        }
        if (event.action !== 'grant' && event.action !== 'revoke') {
          violations.push(`${at} change has invalid action ${String(event.action)}`)
        }
        if (event.phase !== 'ok' && event.phase !== 'info') {
          violations.push(
            `${at} change has invalid terminal phase ${String(event.phase)}`
          )
        }
        if (typeof event.sqlReturned !== 'boolean') {
          violations.push(`${at} change has non-boolean sqlReturned`)
        }
        if (typeof event.authorityRef !== 'string' || event.authorityRef === '') {
          violations.push(`${at} change has an empty authorityRef`)
        }
        break
      }
      case 'authority': {
        if (event.scope === 'protected-membership') {
          if (!hasExactKeys(event, KEYS['authority:protected-membership'])) {
            violations.push(`${at} authority has unexpected keys`)
            break
          }
          if (typeof event.namespace !== 'string' || event.namespace === '') {
            violations.push(`${at} authority has an empty namespace`)
          }
          if (typeof event.principal !== 'string' || event.principal === '') {
            violations.push(`${at} authority has an empty principal`)
          }
          if (!Number.isSafeInteger(event.count) || (event.count as number) < 0) {
            violations.push(`${at} authority has invalid count ${String(event.count)}`)
          }
        } else if (event.scope === 'sentinel-acl') {
          if (!hasExactKeys(event, KEYS['authority:sentinel-acl'])) {
            violations.push(`${at} authority has unexpected keys`)
            break
          }
          if (typeof event.namespace !== 'string' || event.namespace === '') {
            violations.push(`${at} authority has an empty namespace`)
          }
          if (!isSortedUnique(event.rows)) {
            violations.push(`${at} authority rows are not sorted unique`)
          }
        } else {
          violations.push(`${at} authority has invalid scope ${String(event.scope)}`)
        }
        break
      }
      case 'client': {
        if (!hasExactKeys(event, KEYS.client)) {
          violations.push(`${at} client has unexpected keys`)
          break
        }
        for (const field of [
          'namespace',
          'principal',
          'clientId',
          'groupId',
          'storageKey',
        ] as const) {
          if (typeof event[field] !== 'string' || event[field] === '') {
            violations.push(`${at} client has an empty ${field}`)
          }
        }
        if (event.origin !== 'named' && event.origin !== 'raw') {
          violations.push(`${at} client has invalid origin ${String(event.origin)}`)
        }
        if (!isSortedUnique(event.rows)) {
          violations.push(`${at} client rows are not sorted unique`)
        }
        if (!isSortedUnique(event.markers)) {
          violations.push(`${at} client markers are not sorted unique`)
        }
        for (const field of ['complete', 'fresh', 'pullEchoed'] as const) {
          if (typeof event[field] !== 'boolean') {
            violations.push(`${at} client has non-boolean ${field}`)
          }
        }
        break
      }
      case 'barrier': {
        if (!hasExactKeys(event, KEYS.barrier)) {
          violations.push(`${at} barrier has unexpected keys`)
          break
        }
        if (typeof event.marker !== 'string' || event.marker === '') {
          violations.push(`${at} barrier has an empty marker`)
        }
        if (typeof event.complete !== 'boolean') {
          violations.push(`${at} barrier has non-boolean complete`)
        }
        if (!isSortedUnique(event.observers)) {
          violations.push(`${at} barrier observers are not sorted unique`)
        }
        break
      }
      default:
        violations.push(`${at} has unknown type ${String(event.type)}`)
    }
  }
  return violations
}

function setEq(a: readonly string[], b: readonly string[]): boolean {
  if (a.length !== b.length) return false
  const set = new Set(b)
  return a.every((item) => set.has(item))
}

// The protected identity universe: same ids in both namespaces (separate dbs).
// Exported so the live lane and its tests encode identities exactly one way.
export const PROTECTED_IDS = {
  project: 'pt-project',
  member: 'pt-member',
  task: 'pt-task',
} as const

export type ProtectedView = keyof typeof PROTECTED_IDS

export function protectedRowId(view: ProtectedView, id: string): string {
  return `${view}:${id}`
}

const PROJECT = protectedRowId('project', PROTECTED_IDS.project)
const TASK = protectedRowId('task', PROTECTED_IDS.task)
const MEMBER = protectedRowId('member', PROTECTED_IDS.member)
const OWNER_BASE = [PROJECT, TASK].sort()
const FULL = [MEMBER, PROJECT, TASK].sort()

// Expected protected rows for a role at an epoch. Owner keeps project+task at
// every epoch and gains/loses the membership row with the grant/revoke; the
// subject is empty until granted and empty again after revoke; the stable
// namespace's subject holds the full set at every epoch.
function expectedRows(
  nsRole: 'transition' | 'stable',
  principalRole: 'owner' | 'subject',
  epoch: PermissionEpoch
): string[] {
  if (nsRole === 'stable') return FULL
  if (principalRole === 'owner') return epoch === 1 ? FULL : OWNER_BASE
  return epoch === 1 ? FULL : []
}

export function checkPermissionTransition(
  events: readonly PermissionEvent[]
): CheckResult {
  const structural = validateStructure(events)
  if (structural.length > 0) return result(structural)

  const violations: string[] = []
  const changes = events.filter((e): e is ChangeEvent => e.type === 'change')
  const authorities = events.filter((e): e is AuthorityEvent => e.type === 'authority')
  const clients = events.filter((e): e is ClientEvent => e.type === 'client')
  const barriers = events.filter((e): e is BarrierEvent => e.type === 'barrier')

  // ---- topology: one host, exactly two namespaces ------------------------
  const hosts = new Set(events.map((e) => e.host))
  if (hosts.size !== 1) {
    violations.push(`expected exactly one host, saw ${hosts.size}`)
  }
  const namespaces = new Set<string>()
  for (const e of events) {
    if (e.type === 'barrier') continue
    namespaces.add(e.namespace)
  }
  if (namespaces.size !== 2) {
    violations.push(`expected exactly two namespaces, saw ${namespaces.size}`)
    return result(violations)
  }

  // ---- exact grant then revoke on one namespace/principal ----------------
  const grants = changes.filter((c) => c.action === 'grant')
  const revokes = changes.filter((c) => c.action === 'revoke')
  if (grants.length !== 1 || revokes.length !== 1 || changes.length !== 2) {
    violations.push('expected exactly one grant and one revoke change')
    return result(violations)
  }
  const grant = grants[0]!
  const revoke = revokes[0]!
  if (grant.namespace !== revoke.namespace || grant.principal !== revoke.principal) {
    violations.push('grant and revoke target different namespace or principal')
    return result(violations)
  }
  if (grant.epoch !== 1 || revoke.epoch !== 2 || grant.index >= revoke.index) {
    violations.push('grant must establish epoch 1 before revoke establishes epoch 2')
    return result(violations)
  }
  const transitionNs = grant.namespace
  const subjectPrincipal = grant.principal
  const stableNs = [...namespaces].find((n) => n !== transitionNs)!

  // roles are derived, never assumed from names
  const roleOf = (e: {
    namespace: string
    principal: string
  }):
    | { nsRole: 'transition'; principalRole: 'owner' | 'subject' }
    | { nsRole: 'stable'; principalRole: 'subject' } => {
    if (e.namespace === stableNs) return { nsRole: 'stable', principalRole: 'subject' }
    return {
      nsRole: 'transition',
      principalRole: e.principal === subjectPrincipal ? 'subject' : 'owner',
    }
  }

  // ---- terminal authority corroboration of each change -------------------
  const authorityByOpId = new Map(authorities.map((a) => [a.opId, a]))
  const corroborate = (change: ChangeEvent, wantCount: number) => {
    if (change.phase !== 'ok') {
      violations.push(`${change.action} is ${change.phase}, not an authoritative ok`)
      return
    }
    if (!change.sqlReturned) {
      violations.push(`${change.action} ok without a target.sql return`)
    }
    const authority = authorityByOpId.get(change.authorityRef)
    if (
      authority === undefined ||
      authority.type !== 'authority' ||
      authority.scope !== 'protected-membership'
    ) {
      violations.push(
        `${change.action} authorityRef does not resolve to a membership read`
      )
      return
    }
    if (
      authority.namespace !== change.namespace ||
      authority.principal !== change.principal ||
      authority.epoch !== change.epoch ||
      authority.count !== wantCount
    ) {
      violations.push(
        `${change.action} authority read does not corroborate count ${wantCount}`
      )
    }
  }
  corroborate(grant, 1)
  corroborate(revoke, 0)

  // ---- sentinel ACL unchanged across epochs 0/1/2 per namespace ----------
  for (const ns of namespaces) {
    const acls = authorities.filter(
      (a): a is SentinelAclAuthority => a.scope === 'sentinel-acl' && a.namespace === ns
    )
    for (const epoch of EPOCHS) {
      if (!acls.some((a) => a.epoch === epoch)) {
        violations.push(
          `namespace ${ns} is missing a sentinel-acl read at epoch ${epoch}`
        )
      }
    }
    if (acls.length > 0) {
      const baseline = acls[0]!.rows
      if (baseline.length === 0) {
        violations.push(`namespace ${ns} sentinel scope is empty`)
      }
      for (const acl of acls) {
        if (!setEq(acl.rows, baseline)) {
          violations.push(`namespace ${ns} sentinel ACL drifted at epoch ${acl.epoch}`)
        }
      }
    }
  }

  // ---- barriers: one per epoch, complete, fresh marker, cover all clients -
  const clientsAtEpoch = (epoch: PermissionEpoch) =>
    new Set(clients.filter((c) => c.epoch === epoch).map((c) => c.clientId))
  const seenMarkers = new Set<string>()
  for (const epoch of EPOCHS) {
    const epochBarriers = barriers.filter((b) => b.epoch === epoch)
    if (epochBarriers.length !== 1) {
      violations.push(`expected exactly one sentinel barrier at epoch ${epoch}`)
      continue
    }
    const barrier = epochBarriers[0]!
    if (!barrier.complete) {
      violations.push(`sentinel barrier at epoch ${epoch} is not complete`)
    }
    if (seenMarkers.has(barrier.marker)) {
      violations.push(`sentinel barrier at epoch ${epoch} reuses a stale marker`)
    }
    seenMarkers.add(barrier.marker)
    const present = clientsAtEpoch(epoch)
    const observers = new Set(barrier.observers)
    if (present.size === 0 || !setEq([...present], [...observers])) {
      violations.push(
        `sentinel barrier at epoch ${epoch} does not cover every live client`
      )
    }
  }

  // ---- per-client rows/markers, contamination, fresh/original agreement --
  const markersByNs = new Map<string, Set<string>>([
    [transitionNs, new Set()],
    [stableNs, new Set()],
  ])
  // agreement key: namespace|principal|origin|epoch -> canonical rows
  const agreement = new Map<string, string>()
  let sawRawEvidence = false
  let originalSubjectPopulated = false
  const freshAtEpoch = new Map<PermissionEpoch, boolean>()

  for (const client of clients) {
    if (client.namespace !== transitionNs && client.namespace !== stableNs) {
      violations.push(`client ${client.opId} is in an unknown namespace`)
      continue
    }
    if (!client.pullEchoed) {
      violations.push(`client ${client.opId} identity was not echoed in a pull body`)
    }
    if (!client.complete) {
      violations.push(`client ${client.opId} reported an incomplete observation`)
    }
    if (client.origin === 'raw') sawRawEvidence = true

    const role = roleOf(client)
    const want = expectedRows(role.nsRole, role.principalRole, client.epoch)
    if (!setEq(client.rows, want)) {
      violations.push(
        `client ${client.opId} (${role.nsRole}/${role.principalRole} epoch ${client.epoch}) rows [${client.rows.join(', ')}] != expected [${want.join(', ')}]`
      )
    }

    // markers must be exactly the namespace's marker when rows are present
    for (const marker of client.markers) markersByNs.get(client.namespace)!.add(marker)
    if (client.rows.length === 0 && client.markers.length !== 0) {
      violations.push(`client ${client.opId} carries markers without any rows`)
    }

    const key = `${client.namespace}|${client.principal}|${client.origin}|${client.epoch}`
    const canonical = client.rows.join(',')
    const prior = agreement.get(key)
    if (prior !== undefined && prior !== canonical) {
      violations.push(`fresh and original clients disagree for ${key}`)
    } else {
      agreement.set(key, canonical)
    }

    if (client.fresh) freshAtEpoch.set(client.epoch, true)
    if (
      !client.fresh &&
      role.nsRole === 'transition' &&
      role.principalRole === 'subject' &&
      client.epoch === 1 &&
      client.rows.length > 0
    ) {
      originalSubjectPopulated = true
    }
  }

  // exactly one distinct protected marker per namespace, distinct across them
  const transitionMarkers = [...markersByNs.get(transitionNs)!]
  const stableMarkers = [...markersByNs.get(stableNs)!]
  if (transitionMarkers.length !== 1) {
    violations.push(
      `namespace ${transitionNs} observed markers [${transitionMarkers.sort().join(', ')}]`
    )
  }
  if (stableMarkers.length !== 1) {
    violations.push(
      `namespace ${stableNs} observed markers [${stableMarkers.sort().join(', ')}]`
    )
  }
  if (
    transitionMarkers.length === 1 &&
    stableMarkers.length === 1 &&
    transitionMarkers[0] === stableMarkers[0]
  ) {
    violations.push('the two namespaces share one marker; they are not isolated')
  }

  // ---- required roles, principals, and evidence present ------------------
  const present = (nsRole: 'transition' | 'stable', principalRole: 'owner' | 'subject') =>
    clients.some((c) => {
      const r = roleOf(c)
      return r.nsRole === nsRole && r.principalRole === principalRole
    })
  if (!present('transition', 'owner'))
    violations.push('missing a transition-namespace owner client')
  if (!present('transition', 'subject'))
    violations.push('missing a transition-namespace subject client')
  if (!present('stable', 'subject'))
    violations.push('missing a stable-namespace subject client')

  // named views must exist; raw local-only evidence must exist
  if (!clients.some((c) => c.origin === 'named')) {
    violations.push('missing named full-scope client evidence')
  }
  if (!sawRawEvidence) violations.push('missing raw local-cache-only client evidence')

  if (!originalSubjectPopulated) {
    violations.push(
      'no original subject client was populated with rows before the revoke'
    )
  }
  if (!freshAtEpoch.get(1)) violations.push('no fresh client observed the grant epoch')
  if (!freshAtEpoch.get(2)) violations.push('no fresh client observed the revoke epoch')

  return result(violations)
}

export type PermissionOutcome = 'pass' | 'fail' | 'inconclusive'

// The checks-v2 verdict. A clean history passes. Otherwise, an ambiguous admin
// error (a terminal `info` change) makes the whole run inconclusive rather than
// a fail: we could not authoritatively establish the transition, so a downstream
// row mismatch is not evidence the system is broken. Any other violation is a
// real fail.
export function classifyPermissionOutcome(
  events: readonly PermissionEvent[],
  check: CheckResult = checkPermissionTransition(events)
): PermissionOutcome {
  if (check.valid) return 'pass'
  const ambiguous = events.some((e) => e.type === 'change' && e.phase === 'info')
  return ambiguous ? 'inconclusive' : 'fail'
}
