import { createHash } from 'node:crypto'

import type { ExactlyOnceIdentity } from './history.js'

export const EXACTLY_ONCE_MUTATOR = 'exactlyOnce.incrementProbe' as const

export type IncrementProbeArgs = { id: string }

export type ExpectedExactlyOncePush = {
  identity: ExactlyOnceIdentity
  args: IncrementProbeArgs
}

export type ParsedExactlyOncePush = ExpectedExactlyOncePush & {
  operationDigest: string
  mutationTimestamp: number
}

export function validateIncrementProbeArgs(value: unknown): IncrementProbeArgs {
  if (
    typeof value !== 'object' ||
    value === null ||
    !('id' in value) ||
    typeof value.id !== 'string' ||
    value.id.trim() === '' ||
    Object.keys(value).length !== 1
  ) {
    throw new Error('increment probe requires a nonempty id')
  }
  return { id: value.id }
}

export function parseExactlyOncePush(body: unknown): ParsedExactlyOncePush {
  if (typeof body !== 'object' || body === null) {
    throw new Error('push body must be an object')
  }
  const value = body as Record<string, unknown>
  const rootKeys = Object.keys(value).sort()
  const allowedRoot = [
    'clientGroupID',
    'mutations',
    'pushVersion',
    'requestID',
    'timestamp',
  ]
  if (
    rootKeys.length !== allowedRoot.length ||
    rootKeys.some((key) => !allowedRoot.includes(key))
  ) {
    throw new Error('push body has unknown fields')
  }
  if (typeof value.clientGroupID !== 'string' || value.clientGroupID.trim() === '') {
    throw new Error('push has invalid clientGroupID')
  }
  if (!Array.isArray(value.mutations) || value.mutations.length !== 1) {
    throw new Error('push must contain exactly one mutation')
  }
  const mutation = value.mutations[0]
  if (typeof mutation !== 'object' || mutation === null) {
    throw new Error('push mutation must be an object')
  }
  const raw = mutation as Record<string, unknown>
  const mutationKeys = Object.keys(raw).sort()
  const allowedMutation = ['args', 'clientID', 'id', 'name', 'timestamp', 'type']
  if (
    mutationKeys.length !== allowedMutation.length ||
    mutationKeys.some((key, index) => key !== allowedMutation[index]) ||
    raw.type !== 'custom' ||
    raw.name !== EXACTLY_ONCE_MUTATOR ||
    typeof raw.clientID !== 'string' ||
    raw.clientID.trim() === '' ||
    !Number.isSafeInteger(raw.id) ||
    Number(raw.id) <= 0 ||
    !Number.isSafeInteger(raw.timestamp) ||
    Number(raw.timestamp) <= 0 ||
    !Array.isArray(raw.args) ||
    raw.args.length !== 1 ||
    value.pushVersion !== 1 ||
    typeof value.requestID !== 'string' ||
    value.requestID.trim() === '' ||
    !Number.isSafeInteger(value.timestamp) ||
    Number(value.timestamp) <= 0
  ) {
    throw new Error('push mutation does not match the increment probe contract')
  }
  const args = validateIncrementProbeArgs(raw.args[0])
  const canonical = {
    clientGroupId: value.clientGroupID,
    clientId: raw.clientID,
    mutationId: Number(raw.id),
    type: 'custom',
    name: EXACTLY_ONCE_MUTATOR,
    pushVersion: 1,
    args,
  }
  return {
    identity: {
      clientGroupId: canonical.clientGroupId,
      clientId: canonical.clientId,
      mutationId: canonical.mutationId,
    },
    args,
    operationDigest: createHash('sha256').update(JSON.stringify(canonical)).digest('hex'),
    mutationTimestamp: Number(raw.timestamp),
  }
}

export function assertExpectedExactlyOncePush(
  parsed: ParsedExactlyOncePush,
  expected: ExpectedExactlyOncePush
): void {
  if (
    parsed.identity.clientGroupId !== expected.identity.clientGroupId ||
    parsed.identity.clientId !== expected.identity.clientId ||
    parsed.identity.mutationId !== expected.identity.mutationId ||
    parsed.args.id !== expected.args.id
  ) {
    throw new Error('push does not match the armed exactly-once identity')
  }
}
