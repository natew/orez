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

  test('rejects a non-terminal fail phase structurally', () => {
    const events = patch(valid(), 'grant', { phase: 'fail' })
    expect(
      violations(events).some((v) => v.includes('invalid terminal phase fail'))
    ).toBe(true)
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
