import { isJsonValue } from './crud.js'
import {
  createEffectAttempt,
  runCommittedEffects,
  type EffectAttempt,
} from './effects.js'
import { MutationApplicationError, SyncExecutorRequestError } from './errors.js'
import { createServerTransaction } from './transaction.js'

import type {
  ApplicationDatabase,
  ApplicationTransaction,
  CreateSyncExecutorOptions,
  JsonValue,
  MutatorRegistry,
  NormalizedClaims,
  PushResult,
  ServerTransaction,
  SyncExecutor,
} from './types.js'
import type { Schema } from '@rocicorp/zero'

const CLEANUP_RESULTS_MUTATION_NAME = '_zero_cleanupResults'

type PushMutation = {
  readonly type: 'custom'
  readonly id: number
  readonly clientID: string
  readonly name: string
  readonly args: readonly JsonValue[]
}

type PushBody = {
  readonly clientGroupID: string
  readonly mutations: readonly PushMutation[]
  readonly pushVersion: number
}

type Preflight =
  | { readonly kind: 'applied' }
  | { readonly kind: 'replay'; readonly expected: number }

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isMutationID(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0
}

function isMutationApplicationError(
  error: unknown
): error is Error & { readonly details: JsonValue } {
  return (
    isRecord(error) &&
    error.name === 'MutationApplicationError' &&
    typeof error.message === 'string' &&
    isJsonValue(error.details)
  )
}

function validatePushBody(value: unknown): PushBody {
  if (
    !isRecord(value) ||
    typeof value.clientGroupID !== 'string' ||
    value.clientGroupID.length === 0 ||
    !Array.isArray(value.mutations) ||
    typeof value.pushVersion !== 'number' ||
    !Number.isFinite(value.pushVersion)
  ) {
    throw new SyncExecutorRequestError(400, 'invalid push body')
  }

  const mutations: PushMutation[] = []
  for (const [index, mutation] of value.mutations.entries()) {
    if (
      !isRecord(mutation) ||
      mutation.type !== 'custom' ||
      (!isMutationID(mutation.id) &&
        !(mutation.name === CLEANUP_RESULTS_MUTATION_NAME && mutation.id === 0)) ||
      typeof mutation.clientID !== 'string' ||
      mutation.clientID.length === 0 ||
      typeof mutation.name !== 'string' ||
      mutation.name.length === 0 ||
      !Array.isArray(mutation.args) ||
      !mutation.args.every(isJsonValue)
    ) {
      throw new SyncExecutorRequestError(400, `invalid mutation at index ${index}`)
    }
    mutations.push(mutation as PushMutation)
  }
  return {
    clientGroupID: value.clientGroupID,
    mutations,
    pushVersion: value.pushVersion,
  }
}

// a mutator that throws is an application failure, not a transport failure:
// upstream zero's process-mutations wraps any handler error the same way, so
// the write rolls back, the client gets an app result, and the ledger still
// advances. rethrowing instead would make the client retry that id forever and
// block every later mutation in its group. ledger and database failures raised
// outside the mutator stay fatal.
function toApplicationError(error: unknown): unknown {
  if (isMutationApplicationError(error)) return error
  if (error instanceof SyncExecutorRequestError) return error
  return new MutationApplicationError(
    error instanceof Error ? error.message : String(error)
  )
}

function validateClaims(claims: NormalizedClaims): void {
  if (!claims || typeof claims.userID !== 'string' || claims.userID.length === 0) {
    throw new SyncExecutorRequestError(403, 'authenticated claims require a userID')
  }
}

function quoteIdentifier(value: string): string {
  if (!value) throw new TypeError('SQL identifier must not be empty')
  return `"${value.replaceAll('"', '""')}"`
}

function internalTable(database: ApplicationDatabase, table: string): string {
  const quoted = quoteIdentifier(table)
  return database.internalSchema
    ? `${quoteIdentifier(database.internalSchema)}.${quoted}`
    : quoted
}

function placeholders(database: ApplicationDatabase, count: number): string[] {
  return Array.from({ length: count }, (_, index) =>
    database.dialect === 'sqlite' ? '?' : `$${index + 1}`
  )
}

function counter(value: unknown, name: string): number {
  const parsed = typeof value === 'bigint' ? Number(value) : Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`unsafe ${name}: ${String(value)}`)
  }
  return parsed
}

async function initializeLedger(database: ApplicationDatabase): Promise<void> {
  await database.transaction(async (tx) => {
    if (database.dialect === 'postgresql' && database.internalSchema) {
      await tx.exec(
        `CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(database.internalSchema)}`
      )
    }
    const clients = internalTable(database, '_zsync_clients')
    const changes = internalTable(database, '_zsync_changes')
    const integer = database.dialect === 'sqlite' ? 'INTEGER' : 'BIGINT'
    await tx.exec(`CREATE TABLE IF NOT EXISTS ${clients} (
      "clientGroupID" TEXT NOT NULL,
      "clientID" TEXT NOT NULL,
      "lastMutationID" ${integer} NOT NULL,
      "userID" TEXT,
      PRIMARY KEY ("clientGroupID", "clientID")
    )`)
    if (database.dialect === 'sqlite') {
      await tx.exec(`CREATE TABLE IF NOT EXISTS ${changes} (
        "watermark" INTEGER PRIMARY KEY AUTOINCREMENT,
        "tableName" TEXT NOT NULL,
        "op" TEXT NOT NULL CHECK ("op" IN ('row', 'lmid', 'marker')),
        "pk" TEXT
      )`)
    } else {
      await tx.exec(`CREATE TABLE IF NOT EXISTS ${changes} (
        "watermark" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        "tableName" TEXT NOT NULL,
        "op" TEXT NOT NULL CHECK ("op" IN ('row', 'lmid', 'marker')),
        "pk" TEXT
      )`)
    }
  })
}

async function preflight(
  database: ApplicationDatabase,
  tx: ApplicationTransaction,
  clientGroupID: string,
  clientID: string,
  mutationID: number,
  userID: string
): Promise<Preflight> {
  const clients = internalTable(database, '_zsync_clients')
  if (database.dialect === 'postgresql') {
    const [groupLock] = placeholders(database, 1)
    await tx.query(`SELECT pg_advisory_xact_lock(hashtextextended(${groupLock}, 0))`, [
      clientGroupID,
    ])
  }
  const params = placeholders(database, 5)
  await tx.exec(
    `INSERT INTO ${clients} ("clientGroupID", "clientID", "lastMutationID", "userID")
     SELECT ${params[0]}, ${params[1]}, 0, ${params[2]}
     WHERE NOT EXISTS (
       SELECT 1 FROM ${clients}
       WHERE "clientGroupID" = ${params[3]} AND "userID" IS NOT NULL AND "userID" <> ${params[4]}
     )
     ON CONFLICT ("clientGroupID", "clientID")
     DO UPDATE SET "userID" = excluded."userID" WHERE "userID" IS NULL`,
    [clientGroupID, clientID, userID, clientGroupID, userID]
  )

  const [groupParam] = placeholders(database, 1)
  const owners = await tx.query<{ userID: string }>(
    `SELECT DISTINCT "userID" AS "userID" FROM ${clients}
     WHERE "clientGroupID" = ${groupParam} AND "userID" IS NOT NULL`,
    [clientGroupID]
  )
  if (owners.some((row) => row.userID !== userID)) {
    throw new SyncExecutorRequestError(403, 'client group belongs to a different user')
  }

  const [group, client] = placeholders(database, 2)
  const lock = database.dialect === 'postgresql' ? ' FOR UPDATE' : ''
  const rows = await tx.query<{ lastMutationID: number | bigint | string }>(
    `SELECT "lastMutationID" AS "lastMutationID" FROM ${clients}
     WHERE "clientGroupID" = ${group} AND "clientID" = ${client}${lock}`,
    [clientGroupID, clientID]
  )
  if (!rows[0]) throw new Error('failed to claim sync client')
  const lmid = counter(rows[0].lastMutationID, 'lastMutationID')
  if (mutationID <= lmid) return { kind: 'replay', expected: lmid + 1 }
  if (mutationID > lmid + 1) {
    throw new SyncExecutorRequestError(
      400,
      `mutation id ${mutationID} skips lmid ${lmid} (out of order)`
    )
  }
  return { kind: 'applied' }
}

async function advanceLMID(
  database: ApplicationDatabase,
  tx: ApplicationTransaction,
  clientGroupID: string,
  clientID: string,
  mutationID: number
): Promise<void> {
  const clients = internalTable(database, '_zsync_clients')
  const changes = internalTable(database, '_zsync_changes')
  const [mutation, group, client] = placeholders(database, 3)
  await tx.exec(
    `UPDATE ${clients} SET "lastMutationID" = ${mutation}
     WHERE "clientGroupID" = ${group} AND "clientID" = ${client}`,
    [mutationID, clientGroupID, clientID]
  )
  const [tableName, op, pk] = placeholders(database, 3)
  await tx.exec(
    `INSERT INTO ${changes} ("tableName", "op", "pk") VALUES (${tableName}, ${op}, ${pk})`,
    [
      '_zsync_clients',
      'lmid',
      JSON.stringify({ clientGroupID, clientID, lmid: mutationID }),
    ]
  )
}

export function registerMutators<
  S extends Schema,
  const Registry extends Record<string, MutatorRegistry<S>[string]>,
>(registry: Registry): Readonly<Registry> {
  return Object.freeze({ ...registry })
}

export function createSyncExecutor<S extends Schema>(
  options: CreateSyncExecutorOptions<S>
): SyncExecutor<S> {
  const { database, effects, mutators, schema } = options
  let ledgerInitialization: Promise<void> | undefined
  const ensureLedger = () => (ledgerInitialization ??= initializeLedger(database))

  async function runMutation(
    mutator: MutatorRegistry<S>[string],
    args: JsonValue,
    claims: NormalizedClaims,
    identity: { clientGroupID: string; clientID: string; mutationID: number } | undefined
  ): Promise<readonly ReturnType<EffectAttempt['entries']>[number][]> {
    let committedEffects: ReturnType<EffectAttempt['entries']> = []
    await database.transaction(async (applicationTx) => {
      const attempt = createEffectAttempt()
      const tx = createServerTransaction(
        schema,
        applicationTx,
        database.dialect,
        identity?.clientID,
        identity?.mutationID
      )
      const ctx = identity
        ? {
            source: 'zero-push' as const,
            claims,
            ...identity,
            defer: attempt.defer,
          }
        : {
            source: 'direct' as const,
            claims,
            defer: attempt.defer,
          }
      try {
        await mutator({ tx, args, ctx })
        attempt.close()
        committedEffects = attempt.entries()
      } catch (error) {
        attempt.close()
        throw error
      }
    })
    return committedEffects
  }

  return {
    schema,

    async push(body: unknown, claims: NormalizedClaims): Promise<PushResult> {
      validateClaims(claims)
      const push = validatePushBody(body)
      if (push.pushVersion !== 1) {
        return {
          pushResponse: {
            error: 'unsupportedPushVersion',
            mutationIDs: push.mutations.map(({ clientID, id }) => ({ clientID, id })),
          },
        }
      }
      await ensureLedger()

      const results: Array<{
        id: { clientID: string; id: number }
        result:
          | Record<string, never>
          | { error: 'alreadyProcessed'; details: string }
          | { error: 'app'; message: string; details: JsonValue }
      }> = []

      for (const mutation of push.mutations) {
        if (mutation.name === CLEANUP_RESULTS_MUTATION_NAME) continue
        const id = { clientID: mutation.clientID, id: mutation.id }
        let committedEffects: ReturnType<EffectAttempt['entries']> = []
        try {
          const decision = await database.transaction(async (applicationTx) => {
            const current = await preflight(
              database,
              applicationTx,
              push.clientGroupID,
              mutation.clientID,
              mutation.id,
              claims.userID
            )
            if (current.kind === 'replay') return current

            const attempt = createEffectAttempt()
            const tx = createServerTransaction(
              schema,
              applicationTx,
              database.dialect,
              mutation.clientID,
              mutation.id
            )
            try {
              try {
                // resolved in here so an unknown name is an application error
                // too: a stale client naming a removed mutator would otherwise
                // retry it forever and block its later mutation ids
                if (!Object.hasOwn(mutators, mutation.name)) {
                  throw new Error(`unknown mutator: ${mutation.name}`)
                }
                await mutators[mutation.name]!({
                  tx,
                  args: mutation.args[0] ?? null,
                  ctx: {
                    source: 'zero-push',
                    claims,
                    clientGroupID: push.clientGroupID,
                    clientID: mutation.clientID,
                    mutationID: mutation.id,
                    defer: attempt.defer,
                  },
                })
              } catch (error) {
                throw toApplicationError(error)
              }
              await advanceLMID(
                database,
                applicationTx,
                push.clientGroupID,
                mutation.clientID,
                mutation.id
              )
              attempt.close()
              committedEffects = attempt.entries()
              return current
            } catch (error) {
              attempt.close()
              throw error
            }
          })

          if (decision.kind === 'replay') {
            results.push({
              id,
              result: {
                error: 'alreadyProcessed',
                details: `Ignoring mutation from ${mutation.clientID} with ID ${mutation.id} as it was already processed. Expected: ${decision.expected}`,
              },
            })
            continue
          }
          results.push({ id, result: {} })
          await runCommittedEffects(committedEffects, effects)
        } catch (error) {
          if (!isMutationApplicationError(error)) throw error
          await database.transaction(async (applicationTx) => {
            const decision = await preflight(
              database,
              applicationTx,
              push.clientGroupID,
              mutation.clientID,
              mutation.id,
              claims.userID
            )
            if (decision.kind === 'applied') {
              await advanceLMID(
                database,
                applicationTx,
                push.clientGroupID,
                mutation.clientID,
                mutation.id
              )
            }
          })
          results.push({
            id,
            result: { error: 'app', message: error.message, details: error.details },
          })
        }
      }
      return { pushResponse: { mutations: results } }
    },

    async execute(name, args, claims): Promise<void> {
      validateClaims(claims)
      if (!Object.hasOwn(mutators, name)) throw new Error(`unknown mutator: ${name}`)
      const committed = await runMutation(mutators[name]!, args, claims, undefined)
      await runCommittedEffects(committed, effects)
    },

    async transaction<Value>(
      claims: NormalizedClaims,
      work: (tx: ServerTransaction<S>) => Value | Promise<Value>
    ): Promise<Value> {
      validateClaims(claims)
      return database.transaction((applicationTx) =>
        work(createServerTransaction(schema, applicationTx, database.dialect))
      )
    },

    async query<Result>(
      claims: NormalizedClaims,
      work: (tx: ServerTransaction<S>) => Result | Promise<Result>
    ): Promise<Result> {
      validateClaims(claims)
      return database.transaction((applicationTx) =>
        work(createServerTransaction(schema, applicationTx, database.dialect))
      )
    },
  }
}

export async function handleSyncExecutorPushRequest<S extends Schema>(options: {
  readonly executor: SyncExecutor<S>
  readonly request: Request
  readonly claims: NormalizedClaims
}): Promise<Response> {
  try {
    const result = await options.executor.push(
      await options.request.json(),
      options.claims
    )
    // the body IS zero's mutate response; wrapping it in another object fails
    // zero's mutateResponseSchema and the push comes back as PushFailed
    return Response.json(result.pushResponse)
  } catch (error) {
    const status =
      error instanceof SyncExecutorRequestError
        ? error.status
        : error instanceof SyntaxError
          ? 400
          : 500
    return Response.json(
      { error: error instanceof Error ? error.message : String(error) },
      { status }
    )
  }
}
