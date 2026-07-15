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

  test('rejects schema version and empty plan metadata mutants', () => {
    expectOnly(
      mutant((schedule) => {
        schedule.schemaVersion = 2 as typeof FAULT_SCHEDULE_SCHEMA_VERSION
      }),
      'schedule has schema version 2'
    )
    expectOnly(
      mutant((schedule) => {
        schedule.plans[0]!.id = ''
        for (const receipt of schedule.receipts) {
          if (receipt.planId === 'drop-push-1') receipt.planId = ''
        }
      }),
      'plan 0 has an empty id'
    )
    expectOnly(
      mutant((schedule) => {
        schedule.plans[0]!.kind = ''
      }),
      'plan drop-push-1 has an empty kind'
    )
  })

  test('rejects negative and unsafe plan logical steps', () => {
    expect(
      validateFaultSchedule(
        mutant((schedule) => {
          schedule.plans[0]!.arm.logicalStep = -1
        })
      )
    ).toEqual({
      valid: false,
      violations: [
        'plan drop-push-1 arm has invalid logical step -1',
        'plan drop-push-1 arm receipt step 2 does not match -1',
      ],
    })
    const unsafe = Number.MAX_SAFE_INTEGER + 1
    expect(
      validateFaultSchedule(
        mutant((schedule) => {
          schedule.plans[0]!.heal!.logicalStep = unsafe
        })
      )
    ).toEqual({
      valid: false,
      violations: [
        `plan drop-push-1 heal has invalid logical step ${unsafe}`,
        `plan drop-push-1 heal receipt step 5 does not match ${unsafe}`,
      ],
    })
  })

  test('rejects negative and unsafe receipt logical steps', () => {
    expect(
      validateFaultSchedule(
        mutant((schedule) => (schedule.receipts[0]!.logicalStep = -1))
      )
    ).toEqual({
      valid: false,
      violations: [
        'receipt 0 has invalid logical step -1',
        'plan drop-push-1 arm receipt step -1 does not match 2',
      ],
    })
    const unsafe = Number.MAX_SAFE_INTEGER + 1
    expect(
      validateFaultSchedule(
        mutant((schedule) => (schedule.receipts[2]!.logicalStep = unsafe))
      )
    ).toEqual({
      valid: false,
      violations: [
        `receipt 2 has invalid logical step ${unsafe}`,
        `plan drop-push-1 heal receipt step ${unsafe} does not match 5`,
      ],
    })
  })

  test('rejects empty plan and receipt hooks', () => {
    expect(
      validateFaultSchedule(mutant((schedule) => (schedule.plans[0]!.fire.hook = '')))
    ).toEqual({
      valid: false,
      violations: [
        'plan drop-push-1 fire has an empty hook',
        'plan drop-push-1 fire receipt hook after-commit-before-response does not match ',
      ],
    })
    expect(
      validateFaultSchedule(mutant((schedule) => (schedule.receipts[1]!.hook = '')))
    ).toEqual({
      valid: false,
      violations: [
        'receipt 1 has an empty hook',
        'plan drop-push-1 fire receipt hook  does not match after-commit-before-response',
      ],
    })
  })

  test('rejects an unknown receipt phase', () => {
    const schedule = mutant((candidate) => {
      candidate.receipts[1]!.phase = 'explode' as never
    })
    expect(validateFaultSchedule(schedule)).toEqual({
      valid: false,
      violations: [
        'receipt 1 has unknown phase explode',
        'plan drop-push-1 expected exactly one fire receipt, got 0',
      ],
    })
  })
})
