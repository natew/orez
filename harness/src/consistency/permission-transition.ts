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
  // the fresh sentinel marker this client observed at its epoch; ties the client
  // to the epoch's liveness barrier so an empty row set is a proven live revoke
  sentinelMarker: string
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
  // sorted unique opIds of the client observations that prove this barrier; must
  // be exactly the client events present at this epoch, each carrying `marker`
  observationRefs: string[]
  // opId of the admin change that established this epoch and the oracle read that
  // corroborated it; both null at the initial epoch 0 (no transition happened yet)
  changeRef: string | null
  authorityRef: string | null
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
    'sentinelMarker',
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
    'observationRefs',
    'changeRef',
    'authorityRef',
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
          'sentinelMarker',
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
        if (!isSortedUnique(event.observationRefs)) {
          violations.push(`${at} barrier observationRefs are not sorted unique`)
        }
        for (const field of ['changeRef', 'authorityRef'] as const) {
          const value = event[field]
          if (value !== null && (typeof value !== 'string' || value === '')) {
            violations.push(`${at} barrier ${field} must be null or a nonempty opId`)
          }
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
  // the sentinel scope grants exactly the participating principals of its
  // namespace (one membership row each), stays byte-identical across every
  // epoch, and never overlaps a protected identity. distinct participants are
  // derived from the client observations, so an extra or missing sentinel row
  // is caught against ground truth rather than a hand-declared count.
  const protectedIdentitySet = new Set([PROJECT, TASK, MEMBER])
  const participantsIn = (ns: string) =>
    new Set(clients.filter((c) => c.namespace === ns).map((c) => c.principal))
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
      const participants = participantsIn(ns).size
      if (participants > 0 && baseline.length !== participants) {
        violations.push(
          `namespace ${ns} sentinel scope grants ${baseline.length} identities, not its ${participants} participants`
        )
      }
      if (baseline.some((row) => protectedIdentitySet.has(row))) {
        violations.push(`namespace ${ns} sentinel scope overlaps a protected identity`)
      }
      for (const acl of acls) {
        if (!setEq(acl.rows, baseline)) {
          violations.push(`namespace ${ns} sentinel ACL drifted at epoch ${acl.epoch}`)
        }
      }
    }
  }

  // ---- barriers: one per epoch, complete, fresh marker, cover all clients -
  // each barrier references, by exact opId, every client observation present at
  // its epoch (proving liveness) and — for a transition epoch — the change and
  // the oracle read that established it. epoch 0 is the initial state and refers
  // to no change. the marker liveness of each referenced observation is checked
  // per-client below against `barrierMarkerByEpoch`.
  const clientByOpId = new Map(clients.map((c) => [c.opId, c]))
  const barrierMarkerByEpoch = new Map<PermissionEpoch, string>()
  const seenMarkers = new Set<string>()
  for (const epoch of EPOCHS) {
    const epochBarriers = barriers.filter((b) => b.epoch === epoch)
    if (epochBarriers.length !== 1) {
      violations.push(`expected exactly one sentinel barrier at epoch ${epoch}`)
      continue
    }
    const barrier = epochBarriers[0]!
    barrierMarkerByEpoch.set(epoch, barrier.marker)
    if (!barrier.complete) {
      violations.push(`sentinel barrier at epoch ${epoch} is not complete`)
    }
    if (seenMarkers.has(barrier.marker)) {
      violations.push(`sentinel barrier at epoch ${epoch} reuses a stale marker`)
    }
    seenMarkers.add(barrier.marker)

    const presentClients = clients.filter((c) => c.epoch === epoch)
    const present = new Set(presentClients.map((c) => c.clientId))
    if (present.size === 0 || !setEq([...present], barrier.observers)) {
      violations.push(
        `sentinel barrier at epoch ${epoch} does not cover every live client`
      )
    }

    // observationRefs must be exactly the client observation opIds at this epoch
    const presentOpIds = presentClients.map((c) => c.opId).sort()
    if (!setEq(barrier.observationRefs, presentOpIds)) {
      violations.push(
        `sentinel barrier at epoch ${epoch} observationRefs do not match the live client observations`
      )
    }
    for (const ref of barrier.observationRefs) {
      const observed = clientByOpId.get(ref)
      if (observed === undefined || observed.epoch !== epoch) {
        violations.push(
          `sentinel barrier at epoch ${epoch} observationRef ${ref} does not resolve to a live client observation`
        )
      }
    }

    // change/authority references: none at epoch 0, exact at a transition epoch
    if (epoch === 0) {
      if (barrier.changeRef !== null || barrier.authorityRef !== null) {
        violations.push(
          'sentinel barrier at epoch 0 must not reference a permission change'
        )
      }
    } else {
      const change = epoch === 1 ? grant : revoke
      if (barrier.changeRef !== change.opId) {
        violations.push(
          `sentinel barrier at epoch ${epoch} changeRef does not reference the ${change.action}`
        )
      }
      if (barrier.authorityRef !== change.authorityRef) {
        violations.push(
          `sentinel barrier at epoch ${epoch} authorityRef does not reference the corroborating oracle read`
        )
      }
    }
  }

  // ---- per-client rows/markers, sentinel liveness, 1:1 identity ----------
  const markersByNs = new Map<string, Set<string>>([
    [transitionNs, new Set()],
    [stableNs, new Set()],
  ])
  // agreement key: namespace|principal|origin|epoch -> canonical rows
  const agreement = new Map<string, string>()
  // every clientId maps to exactly one identity tuple; every groupId and every
  // storageKey belongs to exactly one clientId (globally unique, 1:1)
  type Identity = {
    groupId: string
    storageKey: string
    principal: string
    namespace: string
    fresh: boolean
  }
  const identityOf = new Map<string, Identity>()
  const clientOfGroup = new Map<string, string>()
  const clientOfStorage = new Map<string, string>()
  let sawRawEvidence = false
  let sawNamedEvidence = false

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
    if (client.origin === 'named') sawNamedEvidence = true

    // the client must have observed the epoch's fresh sentinel marker; that is
    // what makes an empty protected row set a proven live revoke, not a lag
    const barrierMarker = barrierMarkerByEpoch.get(client.epoch)
    if (barrierMarker !== undefined && client.sentinelMarker !== barrierMarker) {
      violations.push(
        `client ${client.opId} observed sentinel marker ${client.sentinelMarker}, not the epoch ${client.epoch} barrier marker`
      )
    }

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

    // 1:1 identity: a clientId keeps one tuple; a groupId/storageKey is not
    // shared. this rejects a fresh client masquerading as an original one.
    const priorIdentity = identityOf.get(client.clientId)
    if (priorIdentity === undefined) {
      identityOf.set(client.clientId, {
        groupId: client.groupId,
        storageKey: client.storageKey,
        principal: client.principal,
        namespace: client.namespace,
        fresh: client.fresh,
      })
    } else if (
      priorIdentity.groupId !== client.groupId ||
      priorIdentity.storageKey !== client.storageKey ||
      priorIdentity.principal !== client.principal ||
      priorIdentity.namespace !== client.namespace ||
      priorIdentity.fresh !== client.fresh
    ) {
      violations.push(
        `client ${client.clientId} reports an inconsistent identity across observations`
      )
    }
    const groupOwner = clientOfGroup.get(client.groupId)
    if (groupOwner !== undefined && groupOwner !== client.clientId) {
      violations.push(`group ${client.groupId} is shared by two clients`)
    } else clientOfGroup.set(client.groupId, client.clientId)
    const storageOwner = clientOfStorage.get(client.storageKey)
    if (storageOwner !== undefined && storageOwner !== client.clientId) {
      violations.push(`storage key ${client.storageKey} is shared by two clients`)
    } else clientOfStorage.set(client.storageKey, client.clientId)
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

  // ---- exact client roster: stable originals + a precise fresh set --------
  // named + raw evidence must both exist at all
  if (!sawNamedEvidence) violations.push('missing named full-scope client evidence')
  if (!sawRawEvidence) violations.push('missing raw local-cache-only client evidence')

  // every (epoch, origin) combination a clientId was observed in
  const combosOf = new Map<string, Set<string>>()
  for (const client of clients) {
    let combos = combosOf.get(client.clientId)
    if (!combos) combosOf.set(client.clientId, (combos = new Set()))
    combos.add(`${client.epoch}:${client.origin}`)
  }
  const roleKeyOf = (clientId: string): string => {
    const identity = identityOf.get(clientId)!
    const r = roleOf(identity)
    return `${r.nsRole}/${r.principalRole}`
  }
  const ALL_COMBOS = ['0:named', '0:raw', '1:named', '1:raw', '2:named', '2:raw']

  // each of the three original roles is one clientId present in named + raw at
  // every epoch; the same original subject therefore has rows at 1 and none at 2
  for (const wantRole of ['transition/owner', 'transition/subject', 'stable/subject']) {
    const ids = [...identityOf].filter(
      ([id, identity]) => !identity.fresh && roleKeyOf(id) === wantRole
    )
    if (ids.length !== 1) {
      violations.push(
        `expected exactly one stable original ${wantRole} client, saw ${ids.length}`
      )
      continue
    }
    const combos = combosOf.get(ids[0]![0])!
    if (combos.size !== ALL_COMBOS.length || ALL_COMBOS.some((c) => !combos.has(c))) {
      violations.push(
        `original ${wantRole} client is not present in named and raw at every epoch`
      )
    }
  }

  // fresh clients: exactly one transition-subject and one stable-subject after
  // the grant (epoch 1) and again after the revoke (epoch 2), each present in
  // named + raw at exactly its own epoch and nowhere else
  const freshSignatures = new Map<string, number>()
  for (const [id, identity] of identityOf) {
    if (!identity.fresh) continue
    const combos = [...(combosOf.get(id) ?? new Set<string>())]
    const epochs = new Set(combos.map((c) => c.split(':')[0]))
    if (epochs.size !== 1) {
      violations.push(`fresh client ${id} spans more than one epoch`)
      continue
    }
    const epoch = [...epochs][0]!
    if (
      combos.length !== 2 ||
      !combos.includes(`${epoch}:named`) ||
      !combos.includes(`${epoch}:raw`)
    ) {
      violations.push(`fresh client ${id} is not present in named and raw at its epoch`)
      continue
    }
    const signature = `${roleKeyOf(id)}@${epoch}`
    freshSignatures.set(signature, (freshSignatures.get(signature) ?? 0) + 1)
  }
  const WANT_FRESH = [
    'transition/subject@1',
    'stable/subject@1',
    'transition/subject@2',
    'stable/subject@2',
  ]
  for (const signature of WANT_FRESH) {
    if (freshSignatures.get(signature) !== 1) {
      const [role, epoch] = signature.split('@')
      violations.push(`expected exactly one fresh ${role} client at epoch ${epoch}`)
    }
  }
  for (const signature of freshSignatures.keys()) {
    if (!WANT_FRESH.includes(signature)) {
      violations.push(`unexpected fresh client in the roster: ${signature}`)
    }
  }

  return result([...new Set(violations)])
}

export type PermissionOutcome = 'pass' | 'fail' | 'inconclusive'

// The checks-v2 verdict, with fail precedence. A clean history passes. The run
// is `inconclusive` only when the SOLE reason it is not a pass is that an admin
// change came back ambiguous (a terminal `info`): we could not authoritatively
// establish that transition, so we make no claim. If any OTHER violation is
// present — a structural defect, a topology break, a sentinel/liveness failure,
// a retained-row safety violation — that is a genuine `fail` that an ambiguous
// change never excuses. The only violation an `info` change itself contributes
// is "<action> is info, not an authoritative ok"; anything beyond that is real.
export function classifyPermissionOutcome(
  events: readonly PermissionEvent[],
  check: CheckResult = checkPermissionTransition(events)
): PermissionOutcome {
  if (check.valid) return 'pass'
  const excusable = new Set(
    events
      .filter((e): e is ChangeEvent => e.type === 'change' && e.phase === 'info')
      .map((c) => `${c.action} is ${c.phase}, not an authoritative ok`)
  )
  if (excusable.size === 0) return 'fail'
  const residual = check.violations.filter((v) => !excusable.has(v))
  return residual.length === 0 ? 'inconclusive' : 'fail'
}
