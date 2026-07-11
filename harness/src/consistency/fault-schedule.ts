import type { CheckResult } from './history.js'
import type { ExactlyOnceIdentity } from './history.js'

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
  operationId?: string
  identity?: ExactlyOnceIdentity
}

export type FaultReceipt = FaultPoint & {
  planId: string
  phase: FaultPhase
  operationId?: string
  identity?: ExactlyOnceIdentity
  anchor?: { historyIndex: number; historyOpId: string }
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

function validatePoint(
  label: string,
  point: unknown,
  violations: string[]
): point is FaultPoint {
  if (typeof point !== 'object' || point === null) {
    violations.push(`${label} is not a fault point`)
    return false
  }
  const value = point as Record<string, unknown>
  if (!Number.isSafeInteger(value.logicalStep) || Number(value.logicalStep) < 0) {
    violations.push(`${label} has invalid logical step ${String(value.logicalStep)}`)
  }
  if (typeof value.hook !== 'string' || value.hook.trim() === '')
    violations.push(`${label} has an empty hook`)
  return Number.isSafeInteger(value.logicalStep) && typeof value.hook === 'string'
}

function identityMatches(
  left: ExactlyOnceIdentity | undefined,
  right: ExactlyOnceIdentity | undefined
): boolean {
  if (left === undefined || right === undefined) return left === right
  if (
    typeof left !== 'object' ||
    left === null ||
    typeof right !== 'object' ||
    right === null
  )
    return false
  return (
    left.clientGroupId === right.clientGroupId &&
    left.clientId === right.clientId &&
    left.mutationId === right.mutationId
  )
}

export function validateFaultSchedule(schedule: FaultSchedule): CheckResult {
  const violations: string[] = []
  if (typeof schedule !== 'object' || schedule === null) {
    return result(['schedule is not an object'])
  }
  if (schedule.schemaVersion !== FAULT_SCHEDULE_SCHEMA_VERSION) {
    violations.push(`schedule has schema version ${schedule.schemaVersion}`)
  }
  if (typeof schedule.faultsRequired !== 'boolean') {
    violations.push('schedule has invalid faultsRequired')
  }
  if (!Array.isArray(schedule.plans) || !Array.isArray(schedule.receipts)) {
    violations.push('schedule plans and receipts must be arrays')
    return result(violations)
  }
  if (schedule.faultsRequired && schedule.plans.length === 0) {
    violations.push('workload requires faults but schedule is empty')
  }

  const plans = new Map<string, FaultPlan>()
  for (const [index, plan] of schedule.plans.entries()) {
    if (typeof plan !== 'object' || plan === null) {
      violations.push(`plan ${index} is not an object`)
      continue
    }
    if (typeof plan.id !== 'string' || plan.id.trim() === '') {
      violations.push(`plan ${index} has an empty id`)
      if (typeof plan.id !== 'string') continue
    }
    if (typeof plan.kind !== 'string' || plan.kind.trim() === '')
      violations.push(`plan ${plan.id} has an empty kind`)
    if (
      plan.operationId !== undefined &&
      (typeof plan.operationId !== 'string' || plan.operationId.trim() === '')
    ) {
      violations.push(`plan ${plan.id} has an empty operation id`)
    }
    if (plans.has(plan.id)) violations.push(`plan id ${plan.id} is not unique`)
    else plans.set(plan.id, plan)

    const armValid = validatePoint(`plan ${plan.id} arm`, plan.arm, violations)
    const fireValid = validatePoint(`plan ${plan.id} fire`, plan.fire, violations)
    if (plan.heal !== undefined)
      validatePoint(`plan ${plan.id} heal`, plan.heal, violations)
    if (armValid && fireValid && plan.arm.logicalStep >= plan.fire.logicalStep) {
      violations.push(`plan ${plan.id} does not satisfy arm < fire`)
    }
    if (
      plan.heal !== undefined &&
      fireValid &&
      validatePoint(`plan ${plan.id} heal`, plan.heal, []) &&
      plan.fire.logicalStep >= plan.heal.logicalStep
    ) {
      violations.push(`plan ${plan.id} does not satisfy fire < heal`)
    }
  }

  const receipts = new Map<string, FaultReceipt[]>()
  for (const [index, receipt] of schedule.receipts.entries()) {
    if (typeof receipt !== 'object' || receipt === null) {
      violations.push(`receipt ${index} is not an object`)
      continue
    }
    validatePoint(`receipt ${index}`, receipt, violations)
    if (typeof receipt.planId !== 'string') {
      violations.push(`receipt ${index} has invalid plan id`)
      continue
    }
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
      if (receipt.operationId !== plan.operationId) {
        violations.push(`plan ${plan.id} ${phase} receipt operation does not match`)
      }
      if (!identityMatches(receipt.identity, plan.identity)) {
        violations.push(`plan ${plan.id} ${phase} receipt identity does not match`)
      }
      if (
        receipt.anchor !== undefined &&
        (typeof receipt.anchor !== 'object' ||
          receipt.anchor === null ||
          !Number.isSafeInteger(receipt.anchor.historyIndex) ||
          receipt.anchor.historyIndex < 0 ||
          typeof receipt.anchor.historyOpId !== 'string' ||
          receipt.anchor.historyOpId.trim() === '')
      ) {
        violations.push(`plan ${plan.id} ${phase} receipt has an invalid history anchor`)
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
