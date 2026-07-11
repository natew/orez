import { describe, expect, test } from 'bun:test'

import {
  FAULT_SCHEDULE_SCHEMA_VERSION,
  validateFaultSchedule,
  type FaultSchedule,
} from './fault-schedule.js'

function validSchedule(): FaultSchedule {
  return {
    schemaVersion: FAULT_SCHEDULE_SCHEMA_VERSION,
    faultsRequired: true,
    plans: [
      {
        id: 'drop-push-1',
        kind: 'drop-push-response',
        arm: { logicalStep: 2, hook: 'before-push' },
        fire: { logicalStep: 3, hook: 'after-commit-before-response' },
        heal: { logicalStep: 5, hook: 'before-replay' },
      },
      {
        id: 'duplicate-pull-1',
        kind: 'duplicate-request',
        arm: { logicalStep: 7, hook: 'before-pull' },
        fire: { logicalStep: 8, hook: 'send-pull' },
      },
    ],
    receipts: [
      { planId: 'drop-push-1', phase: 'arm', logicalStep: 2, hook: 'before-push' },
      {
        planId: 'drop-push-1',
        phase: 'fire',
        logicalStep: 3,
        hook: 'after-commit-before-response',
      },
      { planId: 'drop-push-1', phase: 'heal', logicalStep: 5, hook: 'before-replay' },
      { planId: 'duplicate-pull-1', phase: 'arm', logicalStep: 7, hook: 'before-pull' },
      { planId: 'duplicate-pull-1', phase: 'fire', logicalStep: 8, hook: 'send-pull' },
    ],
  }
}

function mutant(change: (schedule: FaultSchedule) => void): FaultSchedule {
  const schedule = structuredClone(validSchedule())
  change(schedule)
  return schedule
}

function expectOnly(schedule: FaultSchedule, violation: string): void {
  expect(validateFaultSchedule(schedule)).toEqual({
    valid: false,
    violations: [violation],
  })
}

describe('deterministic fault schedule', () => {
  test('accepts matched logical receipts with optional healing', () => {
    expect(validateFaultSchedule(validSchedule())).toEqual({
      valid: true,
      violations: [],
    })
  })

  test('allows an empty schedule only when faults are not required', () => {
    const schedule: FaultSchedule = {
      schemaVersion: FAULT_SCHEDULE_SCHEMA_VERSION,
      faultsRequired: false,
      plans: [],
      receipts: [],
    }
    expect(validateFaultSchedule(schedule)).toEqual({ valid: true, violations: [] })
    schedule.faultsRequired = true
    expectOnly(schedule, 'workload requires faults but schedule is empty')
  })

  test('rejects one-property plan identity and ordering mutants', () => {
    expectOnly(
      mutant((schedule) => {
        schedule.plans.push(structuredClone(schedule.plans[0]!))
      }),
      'plan id drop-push-1 is not unique'
    )
    expectOnly(
      mutant((schedule) => {
        schedule.plans[0]!.fire.logicalStep = 2
        schedule.receipts[1]!.logicalStep = 2
      }),
      'plan drop-push-1 does not satisfy arm < fire'
    )
    expectOnly(
      mutant((schedule) => {
        schedule.plans[0]!.heal!.logicalStep = 3
        schedule.receipts[2]!.logicalStep = 3
      }),
      'plan drop-push-1 does not satisfy fire < heal'
    )
  })

  test('rejects missing and duplicate receipt mutants', () => {
    expectOnly(
      mutant((schedule) => schedule.receipts.splice(0, 1)),
      'plan drop-push-1 expected exactly one arm receipt, got 0'
    )
    expectOnly(
      mutant((schedule) =>
        schedule.receipts.push(structuredClone(schedule.receipts[1]!))
      ),
      'plan drop-push-1 expected exactly one fire receipt, got 2'
    )
    expectOnly(
      mutant((schedule) => schedule.receipts.splice(2, 1)),
      'plan drop-push-1 expected exactly one heal receipt, got 0'
    )
  })

  test('rejects receipt identity, step, and hook mutants', () => {
    expectOnly(
      mutant((schedule) => {
        schedule.receipts.push({
          planId: 'unknown',
          phase: 'arm',
          logicalStep: 10,
          hook: 'before-unknown',
        })
      }),
      'receipt 5 references unknown plan unknown'
    )
    expectOnly(
      mutant((schedule) => {
        schedule.receipts[1]!.logicalStep = 4
      }),
      'plan drop-push-1 fire receipt step 4 does not match 3'
    )
    expectOnly(
      mutant((schedule) => {
        schedule.receipts[1]!.hook = 'wrong-hook'
      }),
      'plan drop-push-1 fire receipt hook wrong-hook does not match after-commit-before-response'
    )
  })

  test('rejects a heal receipt when healing was not planned', () => {
    expectOnly(
      mutant((schedule) => {
        schedule.receipts.push({
          planId: 'duplicate-pull-1',
          phase: 'heal',
          logicalStep: 9,
          hook: 'after-pull',
        })
      }),
      'plan duplicate-pull-1 has an unplanned heal receipt'
    )
  })
})
