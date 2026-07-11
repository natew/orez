import { describe, expect, test } from 'bun:test'

import {
  FULL,
  OWNER_BASE,
  patch,
  PROJECT,
  reindex,
  TASK,
  validPermissionHistory as valid,
} from './permission-transition.fixture.js'
import {
  checkPermissionTransition,
  classifyPermissionOutcome,
  PERMISSION_CHECKS_SCHEMA_VERSION,
  PERMISSION_HISTORY_SCHEMA_VERSION,
  PERMISSION_TRANSITION_PROFILE,
  PERMISSION_TRANSITION_PROFILE_VERSION,
  type PermissionEvent,
} from './permission-transition.js'

function violations(events: PermissionEvent[]): string[] {
  return checkPermissionTransition(events).violations
}

describe(`permission transition (${PERMISSION_TRANSITION_PROFILE.name}@${PERMISSION_TRANSITION_PROFILE.version})`, () => {
  test('pins the frozen v1 profile and the independent schema versions', () => {
    // the frozen profile is v1; the history/event schema is v1; the checks
    // envelope is v2. these are three distinct constants, never conflated.
    expect(PERMISSION_TRANSITION_PROFILE_VERSION).toBe(1)
    expect(PERMISSION_HISTORY_SCHEMA_VERSION).toBe(1)
    expect(PERMISSION_CHECKS_SCHEMA_VERSION).toBe(2)
    expect(PERMISSION_TRANSITION_PROFILE.version).toBe(1)
    expect(PERMISSION_TRANSITION_PROFILE.historySchemaVersion).toBe(1)
    expect(PERMISSION_TRANSITION_PROFILE.checksSchemaVersion).toBe(2)
    expect(PERMISSION_TRANSITION_PROFILE.adapterRequirements).toEqual({
      namespaces: 'exactly-two-child-namespaces-on-one-host',
      authority: 'terminal-admin-change-corroborated-by-oracle',
      clientView: 'complete-named-full-scope-plus-prearmed-raw-local-only',
      barrier: 'disjoint-sentinel-scope-permanently-granted-every-participant',
    })
  })

  test('accepts the canonical grant-then-revoke history', () => {
    expect(checkPermissionTransition(valid())).toEqual({ valid: true, violations: [] })
  })

  // ---- topology ---------------------------------------------------------
  test('rejects a single namespace', () => {
    const events = valid().map((e) =>
      'namespace' in e ? ({ ...e, namespace: 'nsA' } as PermissionEvent) : e
    )
    expect(violations(events)).toContain('expected exactly two namespaces, saw 1')
  })

  test('rejects a third namespace', () => {
    const extra = valid()
    extra.push({
      v: PERMISSION_HISTORY_SCHEMA_VERSION,
      index: extra.length,
      host: 'H',
      type: 'client',
      opId: 'stray-named-0',
      epoch: 0,
      namespace: 'nsC',
      principal: 'ghost',
      clientId: 'ghost',
      groupId: 'ghost-g',
      storageKey: 'ghost-sk',
      origin: 'named',
      rows: [],
      markers: [],
      sentinelMarker: 'sn-0',
      complete: true,
      fresh: false,
      pullEchoed: true,
    })
    expect(violations(extra)).toContain('expected exactly two namespaces, saw 3')
  })

  test('rejects more than one host', () => {
    expect(violations(patch(valid(), 'cAo-named-0', { host: 'H2' }))).toContain(
      'expected exactly one host, saw 2'
    )
  })

  // ---- exact grant then revoke -----------------------------------------
  test('rejects a missing revoke', () => {
    expect(violations(reindex(valid().filter((e) => e.opId !== 'revoke')))).toContain(
      'expected exactly one grant and one revoke change'
    )
  })

  test('rejects two grants', () => {
    expect(violations(patch(valid(), 'revoke', { action: 'grant' }))).toContain(
      'expected exactly one grant and one revoke change'
    )
  })

  test('rejects an extra change event', () => {
    const events = valid()
    events.push({
      v: PERMISSION_HISTORY_SCHEMA_VERSION,
      index: events.length,
      host: 'H',
      type: 'change',
      opId: 'grant-2',
      epoch: 1,
      namespace: 'nsA',
      principal: 'subjectA',
      action: 'grant',
      phase: 'ok',
      sqlReturned: true,
      authorityRef: 'auth-grant',
    })
    expect(violations(events)).toContain(
      'expected exactly one grant and one revoke change'
    )
  })

  test('rejects grant and revoke on different principals', () => {
    expect(violations(patch(valid(), 'revoke', { principal: 'ownerA' }))).toContain(
      'grant and revoke target different namespace or principal'
    )
  })

  test('rejects revoke ordered before grant', () => {
    let events = patch(valid(), 'grant', { epoch: 2 })
    events = patch(events, 'revoke', { epoch: 1 })
    expect(violations(events)).toContain(
      'grant must establish epoch 1 before revoke establishes epoch 2'
    )
  })

  // ---- authority corroboration -----------------------------------------
  test('treats an info grant as inconclusive, not authoritative', () => {
    expect(violations(patch(valid(), 'grant', { phase: 'info' }))).toContain(
      'grant is info, not an authoritative ok'
    )
  })

  test('rejects a non-terminal fail phase structurally', () => {
    const events = patch(valid(), 'grant', { phase: 'fail' })
    expect(
      violations(events).some((v) => v.includes('invalid terminal phase fail'))
    ).toBe(true)
  })

  test('rejects an ok change without a target.sql return', () => {
    expect(violations(patch(valid(), 'grant', { sqlReturned: false }))).toContain(
      'grant ok without a target.sql return'
    )
  })

  test('rejects authority that does not corroborate the count', () => {
    expect(violations(patch(valid(), 'auth-grant', { count: 0 }))).toContain(
      'grant authority read does not corroborate count 1'
    )
  })

  test('rejects a dangling authorityRef', () => {
    expect(violations(patch(valid(), 'grant', { authorityRef: 'nope' }))).toContain(
      'grant authorityRef does not resolve to a membership read'
    )
  })

  // ---- sentinel liveness barrier ---------------------------------------
  test('rejects sentinel ACL drift across epochs', () => {
    expect(
      violations(patch(valid(), 'acl-A-2', { rows: ['member:sn-ownerA'] }))
    ).toContain('namespace nsA sentinel ACL drifted at epoch 2')
  })

  test('rejects a missing sentinel ACL read', () => {
    expect(violations(reindex(valid().filter((e) => e.opId !== 'acl-A-1')))).toContain(
      'namespace nsA is missing a sentinel-acl read at epoch 1'
    )
  })

  test('rejects an empty sentinel scope', () => {
    let events = valid()
    for (const epoch of [0, 1, 2]) events = patch(events, `acl-A-${epoch}`, { rows: [] })
    expect(violations(events)).toContain('namespace nsA sentinel scope is empty')
  })

  test('rejects an incomplete sentinel barrier', () => {
    expect(violations(patch(valid(), 'bar-1', { complete: false }))).toContain(
      'sentinel barrier at epoch 1 is not complete'
    )
  })

  test('rejects a missing sentinel barrier', () => {
    expect(violations(reindex(valid().filter((e) => e.opId !== 'bar-2')))).toContain(
      'expected exactly one sentinel barrier at epoch 2'
    )
  })

  test('rejects a duplicated sentinel barrier at one epoch', () => {
    const events = valid()
    events.push({
      v: PERMISSION_HISTORY_SCHEMA_VERSION,
      index: events.length,
      host: 'H',
      type: 'barrier',
      opId: 'bar-1b',
      epoch: 1,
      marker: 'sn-1x',
      complete: true,
      observers: ['cAo', 'cAs', 'cBs', 'fAs1', 'fBs1'],
      observationRefs: [],
      changeRef: 'grant',
      authorityRef: 'auth-grant',
    })
    expect(violations(events)).toContain(
      'expected exactly one sentinel barrier at epoch 1'
    )
  })

  test('rejects a stale (reused) sentinel marker', () => {
    expect(violations(patch(valid(), 'bar-2', { marker: 'sn-1' }))).toContain(
      'sentinel barrier at epoch 2 reuses a stale marker'
    )
  })

  test('rejects a barrier that does not cover every live client', () => {
    expect(
      violations(patch(valid(), 'bar-2', { observers: ['cAo', 'cAs', 'cBs', 'fAs2'] }))
    ).toContain('sentinel barrier at epoch 2 does not cover every live client')
  })

  // ---- client observations ---------------------------------------------
  test('rejects an incomplete client callback', () => {
    expect(violations(patch(valid(), 'cAs-named-1', { complete: false }))).toContain(
      'client cAs-named-1 reported an incomplete observation'
    )
  })

  test('rejects a client identity not echoed in a pull body', () => {
    expect(violations(patch(valid(), 'cAo-named-0', { pullEchoed: false }))).toContain(
      'client cAo-named-0 identity was not echoed in a pull body'
    )
  })

  test('rejects retained rows after revoke', () => {
    const events = patch(valid(), 'cAs-named-2', { rows: FULL, markers: ['mk-A'] })
    expect(violations(events).some((v) => v.includes('!= expected []'))).toBe(true)
  })

  test('rejects lost stable-namespace rows', () => {
    const events = patch(valid(), 'cBs-named-1', { rows: OWNER_BASE })
    expect(
      violations(events).some(
        (v) => v.includes('cBs-named-1') && v.includes('!= expected')
      )
    ).toBe(true)
  })

  test('rejects lost owner rows', () => {
    const events = patch(valid(), 'cAo-named-0', { rows: [TASK] })
    expect(
      violations(events).some(
        (v) => v.includes('cAo-named-0') && v.includes('!= expected')
      )
    ).toBe(true)
  })

  test('rejects an extra protected identity', () => {
    const events = patch(valid(), 'cAo-named-0', {
      rows: [...OWNER_BASE, 'zzz:extra'].sort(),
    })
    expect(violations(events).some((v) => v.includes('zzz:extra'))).toBe(true)
  })

  test('rejects A-marker contamination in the stable namespace', () => {
    expect(violations(patch(valid(), 'cBs-named-0', { markers: ['mk-A'] }))).toContain(
      'namespace nsB observed markers [mk-A, mk-B]'
    )
  })

  test('rejects fresh/original disagreement', () => {
    const events = patch(valid(), 'fAs1-named-1', { rows: OWNER_BASE })
    expect(violations(events)).toContain(
      'fresh and original clients disagree for nsA|subjectA|named|1'
    )
  })

  test('rejects markers reported without rows', () => {
    const events = patch(valid(), 'cAs-named-0', { markers: ['mk-A'] })
    expect(violations(events)).toContain(
      'client cAs-named-0 carries markers without any rows'
    )
  })

  // ---- required roles + evidence ---------------------------------------
  test('rejects a missing transition owner client', () => {
    expect(violations(reindex(valid().filter((e) => e.clientId !== 'cAo')))).toContain(
      'expected exactly one stable original transition/owner client, saw 0'
    )
  })

  test('rejects a missing stable subject client', () => {
    const events = reindex(
      valid().filter(
        (e) => e.clientId !== 'cBs' && e.clientId !== 'fBs1' && e.clientId !== 'fBs2'
      )
    )
    expect(violations(events)).toContain(
      'expected exactly one stable original stable/subject client, saw 0'
    )
  })

  test('rejects missing raw local-only evidence', () => {
    expect(
      violations(
        reindex(valid().filter((e) => e.type !== 'client' || e.origin !== 'raw'))
      )
    ).toContain('missing raw local-cache-only client evidence')
  })

  test('rejects missing named full-scope evidence', () => {
    expect(
      violations(
        reindex(valid().filter((e) => e.type !== 'client' || e.origin !== 'named'))
      )
    ).toContain('missing named full-scope client evidence')
  })

  test('rejects a history with no fresh client at the grant epoch', () => {
    // demoting the epoch-1 fresh clients to non-fresh leaves the grant epoch
    // without its required fresh roster entry
    let events = patch(valid(), 'fAs1-named-1', { fresh: false })
    events = patch(events, 'fAs1-raw-1', { fresh: false })
    events = patch(events, 'fBs1-named-1', { fresh: false })
    events = patch(events, 'fBs1-raw-1', { fresh: false })
    expect(violations(events)).toContain(
      'expected exactly one fresh transition/subject client at epoch 1'
    )
  })

  test('rejects when the same original subject is not populated at the grant', () => {
    // the original subject client must hold the full protected set at epoch 1;
    // an empty snapshot there is a rows mismatch against the granted expectation
    let events = patch(valid(), 'cAs-named-1', { rows: [], markers: [] })
    events = patch(events, 'cAs-raw-1', { rows: [], markers: [] })
    expect(
      violations(events).some(
        (v) => v.includes('cAs-named-1') && v.includes('!= expected')
      )
    ).toBe(true)
  })

  // ---- sentinel marker liveness on each client -------------------------
  test('rejects a client that observed a stale sentinel marker', () => {
    expect(
      violations(patch(valid(), 'cAs-named-1', { sentinelMarker: 'sn-0' }))
    ).toContain(
      'client cAs-named-1 observed sentinel marker sn-0, not the epoch 1 barrier marker'
    )
  })

  // ---- barrier observation / change / authority references -------------
  test('rejects barrier observationRefs that miss a live client', () => {
    expect(
      violations(patch(valid(), 'bar-1', { observationRefs: ['cAo-named-1'] }))
    ).toContain(
      'sentinel barrier at epoch 1 observationRefs do not match the live client observations'
    )
  })

  test('rejects a barrier observationRef that resolves to the wrong epoch', () => {
    // swap one epoch-1 ref for an existing epoch-0 observation: a stale ref
    const events = patch(valid(), 'bar-1', {
      observationRefs: [
        'cAo-named-0',
        'cAo-raw-1',
        'cAs-named-1',
        'cAs-raw-1',
        'cBs-named-1',
        'cBs-raw-1',
        'fAs1-named-1',
        'fAs1-raw-1',
        'fBs1-named-1',
        'fBs1-raw-1',
      ].sort(),
    })
    expect(
      violations(events).some((v) =>
        v.includes(
          'observationRef cAo-named-0 does not resolve to a live client observation'
        )
      )
    ).toBe(true)
  })

  test('rejects a barrier changeRef that does not reference its change', () => {
    expect(violations(patch(valid(), 'bar-1', { changeRef: 'revoke' }))).toContain(
      'sentinel barrier at epoch 1 changeRef does not reference the grant'
    )
  })

  test('rejects a barrier authorityRef that does not corroborate', () => {
    expect(violations(patch(valid(), 'bar-2', { authorityRef: 'auth-grant' }))).toContain(
      'sentinel barrier at epoch 2 authorityRef does not reference the corroborating oracle read'
    )
  })

  test('rejects an epoch-0 barrier that references a permission change', () => {
    expect(violations(patch(valid(), 'bar-0', { changeRef: 'grant' }))).toContain(
      'sentinel barrier at epoch 0 must not reference a permission change'
    )
  })

  // ---- 1:1 client identity ---------------------------------------------
  test('rejects a groupId shared by two clients', () => {
    expect(
      violations(patch(valid(), 'cAo-named-0', { groupId: 'cAs-g' })).some((v) =>
        v.includes('is shared by two clients')
      )
    ).toBe(true)
  })

  test('rejects a storageKey shared by two clients', () => {
    expect(
      violations(patch(valid(), 'cAo-named-0', { storageKey: 'cAs-sk' })).some((v) =>
        v.includes('storage key cAs-sk is shared by two clients')
      )
    ).toBe(true)
  })

  test('rejects a fresh client masquerading under an original clientId', () => {
    // reuse original subject id `cAs` for a fresh observation: inconsistent
    expect(
      violations(patch(valid(), 'fAs1-named-1', { clientId: 'cAs' })).some((v) =>
        v.includes('reports an inconsistent identity')
      )
    ).toBe(true)
  })

  // ---- sentinel ACL cardinality + disjointness -------------------------
  test('rejects a sentinel scope with an extra identity', () => {
    let events = valid()
    for (const epoch of [0, 1, 2])
      events = patch(events, `acl-A-${epoch}`, {
        rows: ['member:sn-extra', 'member:sn-ownerA', 'member:sn-subjectA'],
      })
    expect(violations(events)).toContain(
      'namespace nsA sentinel scope grants 3 identities, not its 2 participants'
    )
  })

  test('rejects a sentinel scope that overlaps a protected identity', () => {
    let events = valid()
    for (const epoch of [0, 1, 2])
      events = patch(events, `acl-A-${epoch}`, {
        rows: ['member:pt-member', 'member:sn-ownerA'],
      })
    expect(violations(events)).toContain(
      'namespace nsA sentinel scope overlaps a protected identity'
    )
  })

  // ---- exact fresh roster ----------------------------------------------
  test('rejects an unexpected fresh client role in the roster', () => {
    const events = valid()
    for (const origin of ['named', 'raw'] as const) {
      events.push({
        v: PERMISSION_HISTORY_SCHEMA_VERSION,
        index: events.length,
        host: 'H',
        type: 'client',
        opId: `fAo1-${origin}-1`,
        epoch: 1,
        namespace: 'nsA',
        principal: 'ownerA',
        clientId: 'fAo1',
        groupId: 'fAo1-g',
        storageKey: 'fAo1-sk',
        origin,
        rows: FULL,
        markers: ['mk-A'],
        sentinelMarker: 'sn-1',
        complete: true,
        fresh: true,
        pullEchoed: true,
      })
    }
    expect(violations(events)).toContain(
      'unexpected fresh client in the roster: transition/owner@1'
    )
  })

  test('rejects an original client absent at one epoch', () => {
    const events = reindex(
      valid().filter((e) => e.opId !== 'cAo-named-1' && e.opId !== 'cAo-raw-1')
    )
    expect(violations(events)).toContain(
      'original transition/owner client is not present in named and raw at every epoch'
    )
  })

  // ---- structural / schema ---------------------------------------------
  test('rejects duplicate rows structurally', () => {
    const events = patch(valid(), 'cAo-named-0', { rows: [PROJECT, PROJECT] })
    expect(violations(events).some((v) => v.includes('rows are not sorted unique'))).toBe(
      true
    )
  })

  test('rejects unknown keys', () => {
    const events = patch(valid(), 'cAo-named-0', { extra: 1 })
    expect(violations(events).some((v) => v.includes('client has unexpected keys'))).toBe(
      true
    )
  })

  test('rejects an unknown event type', () => {
    const events = patch(valid(), 'cAo-named-0', { type: 'weird' })
    expect(violations(events).some((v) => v.includes('unknown type weird'))).toBe(true)
  })

  test('rejects an unknown client origin', () => {
    const events = patch(valid(), 'cAo-named-0', { origin: 'sideways' })
    expect(violations(events).some((v) => v.includes('invalid origin sideways'))).toBe(
      true
    )
  })

  test('rejects an unknown authority scope', () => {
    const events = patch(valid(), 'acl-A-0', { scope: 'mystery' })
    expect(violations(events).some((v) => v.includes('invalid scope mystery'))).toBe(true)
  })

  test('rejects a wrong history schema version', () => {
    const events = patch(valid(), 'grant', { v: 2 })
    expect(violations(events).some((v) => v.includes('schema version 2'))).toBe(true)
  })

  // ---- checks-v2 pass/fail/inconclusive verdict ------------------------
  test('classifies a clean history as pass', () => {
    expect(classifyPermissionOutcome(valid())).toBe('pass')
  })

  test('classifies a real property violation as fail', () => {
    // a retained row after revoke is a genuine failure, not ambiguity
    const events = patch(valid(), 'cAs-named-2', { rows: FULL, markers: ['mk-A'] })
    expect(classifyPermissionOutcome(events)).toBe('fail')
  })

  test('classifies an ambiguous admin change as inconclusive, not fail', () => {
    const events = patch(valid(), 'grant', { phase: 'info' })
    expect(checkPermissionTransition(events).valid).toBe(false)
    expect(classifyPermissionOutcome(events)).toBe('inconclusive')
  })

  test('an info change plus an independent safety violation is a fail', () => {
    // fail precedence: an ambiguous change only downgrades to inconclusive when
    // it is the ONLY reason for the miss. a retained row after revoke is a real
    // safety violation the ambiguity does not excuse.
    let events = patch(valid(), 'grant', { phase: 'info' })
    events = patch(events, 'cAs-named-2', { rows: FULL, markers: ['mk-A'] })
    expect(classifyPermissionOutcome(events)).toBe('fail')
  })

  test('an info change plus a structural defect is a fail', () => {
    let events = patch(valid(), 'revoke', { phase: 'info' })
    events = patch(events, 'cAo-named-0', { extra: 1 })
    expect(classifyPermissionOutcome(events)).toBe('fail')
  })
})
