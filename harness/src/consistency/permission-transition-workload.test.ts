import { describe, expect, test } from 'bun:test'

import {
  derivePermissionScenario,
  permissionReplayCommand,
  projectProtectedObservation,
  sentinelAclRows,
  validatePermissionScenario,
  type ProtectedObservation,
} from './permission-transition-workload.js'
import { PERMISSION_TRANSITION_PROFILE } from './permission-transition.js'

const empty: ProtectedObservation = { project: [], member: [], task: [] }

describe('permission transition workload contract', () => {
  test('derives a deterministic, self-consistent, isolated scenario', () => {
    const a = derivePermissionScenario('example')
    const b = derivePermissionScenario('example')
    expect(a).toEqual(b)
    expect(a.namespaces.transition).not.toBe(a.namespaces.stable)
    expect(a.markers.transition).not.toBe(a.markers.stable)
    expect(new Set(a.sentinelMarkers).size).toBe(3)
    expect(() => validatePermissionScenario(a)).not.toThrow()
  })

  test('gives different seeds different markers and namespaces', () => {
    const a = derivePermissionScenario('one')
    const b = derivePermissionScenario('two')
    expect(a.namespaces.transition).not.toBe(b.namespaces.transition)
    expect(a.markers.transition).not.toBe(b.markers.transition)
  })

  test('rejects scenarios that break the frozen invariants', () => {
    const base = derivePermissionScenario('mutate')
    expect(() =>
      validatePermissionScenario({
        ...base,
        namespaces: { transition: 'same', stable: 'same' },
      })
    ).toThrow('must be distinct')
    expect(() =>
      validatePermissionScenario({
        ...base,
        markers: { transition: 'shared', stable: 'shared' },
      })
    ).toThrow('distinct markers')
    expect(() =>
      validatePermissionScenario({
        ...base,
        namespaces: { transition: 'bad id!', stable: base.namespaces.stable },
      })
    ).toThrow('valid mount database id')
    expect(() =>
      validatePermissionScenario({
        ...base,
        sentinel: { ...base.sentinel, project: base.protectedIds.project },
      })
    ).toThrow('not disjoint')
    expect(() =>
      validatePermissionScenario({
        ...base,
        sentinel: { ...base.sentinel, members: { transition: [], stable: ['x'] } },
      })
    ).toThrow('every participant')
    expect(() =>
      validatePermissionScenario({
        ...base,
        sentinelMarkers: ['dup', 'dup', 'dup'],
      })
    ).toThrow('distinct fresh sentinel marker')
  })

  test('projects observed views into sorted-unique rows and markers', () => {
    // members carry no marker column; project/task carry the namespace marker
    const observed: ProtectedObservation = {
      project: [{ id: 'pt-project', marker: 'mk-a' }],
      member: [{ id: 'pt-member' }],
      task: [{ id: 'pt-task', marker: 'mk-a' }],
    }
    expect(projectProtectedObservation(observed)).toEqual({
      rows: ['member:pt-member', 'project:pt-project', 'task:pt-task'],
      markers: ['mk-a'],
    })
  })

  test('projects an empty observation to empty rows and markers', () => {
    expect(projectProtectedObservation(empty)).toEqual({ rows: [], markers: [] })
  })

  test('encodes a contaminating foreign marker instead of hiding it', () => {
    const observed: ProtectedObservation = {
      project: [{ id: 'pt-project', marker: 'mk-b' }],
      member: [],
      task: [{ id: 'pt-task', marker: 'mk-a' }],
    }
    expect(projectProtectedObservation(observed).markers).toEqual(['mk-a', 'mk-b'])
  })

  test('renders sentinel ACL identities sorted per namespace', () => {
    const scenario = derivePermissionScenario('acl')
    expect(sentinelAclRows(scenario, 'transition')).toEqual([
      'member:sn-owner',
      'member:sn-subject-a',
    ])
    expect(sentinelAclRows(scenario, 'stable')).toEqual(['member:sn-subject-b'])
  })

  test('replay command preserves a leading-dash seed as one option value', () => {
    expect(permissionReplayCommand('orez-local', '-case')).toBe(
      'bun src/permission-transition-lane.ts --target orez-local --seed=-case --replay'
    )
  })

  test('re-exports the checker profile so the lane cannot drift', () => {
    expect(PERMISSION_TRANSITION_PROFILE.name).toBe(
      'populated-cache-permission-transition'
    )
    expect(PERMISSION_TRANSITION_PROFILE.version).toBe(1)
  })
})
