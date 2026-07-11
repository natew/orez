import type { CheckResult } from './history.js'

export const FAULT_SCHEDULE_SCHEMA_VERSION = 1 as const

export type FaultPhase = 'arm' | 'fire' | 'heal'

export type FaultPoint = {
  logicalStep: number
  hook: string
}

export type FaultPlan = {
  id: string
  kind: string
  arm: FaultPoint
  fire: FaultPoint
  heal?: FaultPoint
}

export type FaultReceipt = FaultPoint & {
  planId: string
  phase: FaultPhase
}

export type FaultSchedule = {
  schemaVersion: typeof FAULT_SCHEDULE_SCHEMA_VERSION
  faultsRequired: boolean
  plans: FaultPlan[]
  receipts: FaultReceipt[]
}

function result(violations: string[]): CheckResult {
  return { valid: violations.length === 0, violations }
}

function validatePoint(label: string, point: FaultPoint, violations: string[]): void {
  if (!Number.isSafeInteger(point.logicalStep) || point.logicalStep < 0) {
    violations.push(`${label} has invalid logical step ${point.logicalStep}`)
  }
  if (point.hook.trim() === '') violations.push(`${label} has an empty hook`)
}

export function validateFaultSchedule(schedule: FaultSchedule): CheckResult {
  const violations: string[] = []
  if (schedule.schemaVersion !== FAULT_SCHEDULE_SCHEMA_VERSION) {
    violations.push(`schedule has schema version ${schedule.schemaVersion}`)
  }
  if (schedule.faultsRequired && schedule.plans.length === 0) {
    violations.push('workload requires faults but schedule is empty')
  }

  const plans = new Map<string, FaultPlan>()
  for (const [index, plan] of schedule.plans.entries()) {
    if (plan.id.trim() === '') violations.push(`plan ${index} has an empty id`)
    if (plan.kind.trim() === '') violations.push(`plan ${plan.id} has an empty kind`)
    if (plans.has(plan.id)) violations.push(`plan id ${plan.id} is not unique`)
    else plans.set(plan.id, plan)

    validatePoint(`plan ${plan.id} arm`, plan.arm, violations)
    validatePoint(`plan ${plan.id} fire`, plan.fire, violations)
    if (plan.heal !== undefined)
      validatePoint(`plan ${plan.id} heal`, plan.heal, violations)
    if (plan.arm.logicalStep >= plan.fire.logicalStep) {
      violations.push(`plan ${plan.id} does not satisfy arm < fire`)
    }
    if (plan.heal !== undefined && plan.fire.logicalStep >= plan.heal.logicalStep) {
      violations.push(`plan ${plan.id} does not satisfy fire < heal`)
    }
  }

  const receipts = new Map<string, FaultReceipt[]>()
  for (const [index, receipt] of schedule.receipts.entries()) {
    validatePoint(`receipt ${index}`, receipt, violations)
    const plan = plans.get(receipt.planId)
    if (plan === undefined) {
      violations.push(`receipt ${index} references unknown plan ${receipt.planId}`)
      continue
    }
    const phase = receipt.phase
    if (phase !== 'arm' && phase !== 'fire' && phase !== 'heal') {
      violations.push(`receipt ${index} has unknown phase ${String(phase)}`)
      continue
    }
    const key = `${receipt.planId}\u0000${phase}`
    const matching = receipts.get(key) ?? []
    matching.push(receipt)
    receipts.set(key, matching)

    const planned = plan[phase]
    if (planned === undefined) {
      violations.push(`plan ${plan.id} has an unplanned ${phase} receipt`)
    } else {
      if (receipt.logicalStep !== planned.logicalStep) {
        violations.push(
          `plan ${plan.id} ${phase} receipt step ${receipt.logicalStep} does not match ${planned.logicalStep}`
        )
      }
      if (receipt.hook !== planned.hook) {
        violations.push(
          `plan ${plan.id} ${phase} receipt hook ${receipt.hook} does not match ${planned.hook}`
        )
      }
    }
  }

  for (const plan of schedule.plans) {
    const phases: FaultPhase[] =
      plan.heal === undefined ? ['arm', 'fire'] : ['arm', 'fire', 'heal']
    for (const phase of phases) {
      const count = receipts.get(`${plan.id}\u0000${phase}`)?.length ?? 0
      if (count !== 1) {
        violations.push(
          `plan ${plan.id} expected exactly one ${phase} receipt, got ${count}`
        )
      }
    }
  }

  return result(violations)
}
