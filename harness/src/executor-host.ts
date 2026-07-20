import {
  boolean,
  createSchema,
  json,
  number,
  string,
  table,
  type Schema,
} from '@rocicorp/zero'

import {
  createZeroHttpApplicationDatabase,
  createZeroHttpSyncServer,
  type ZeroHttpSyncDb,
  type ZeroHttpVisibility,
} from '../../src/zero-http/mount.js'
import { TABLES, executeMutator } from './fixture-data.js'

import type { MutatorRegistry } from 'orez-sync-executor'

const valueTypes = { boolean, json, number, string }
const schema = createSchema({
  tables: Object.entries(TABLES).map(([name, spec]) =>
    table(name)
      .columns(
        Object.fromEntries(
          Object.entries(spec.columns).map(([column, type]) => [
            column,
            valueTypes[type === 'null' ? 'string' : type](),
          ])
        )
      )
      .primaryKey(...(spec.primaryKey as [string, ...string[]]))
  ),
}) as Schema

const mutationNames = [
  'exactlyOnce.incrementProbe',
  'exactlyOnce.incrementThenReject',
  'atomicVisibility.appendGroup',
  'project.create',
  'project.rename',
  'project.delete',
  'member.add',
  'member.remove',
  'task.create',
  'task.toggle',
  'task.setRank',
] as const

export function createHarnessSyncServer(
  db: ZeroHttpSyncDb,
  options?: {
    readonly retainChanges?: number
    readonly visible?: ZeroHttpVisibility
    readonly transaction?: <Value>(work: () => Value | Promise<Value>) => Promise<Value>
  }
) {
  const mutators = Object.fromEntries(
    mutationNames.map((name) => [
      name,
      ({ args, ctx }: Parameters<MutatorRegistry<typeof schema>[string]>[0]) =>
        executeMutator(db, name, args, { userID: ctx.claims.userID }),
    ])
  ) as MutatorRegistry<typeof schema>

  return createZeroHttpSyncServer({
    applicationDatabase: createZeroHttpApplicationDatabase(db, options?.transaction),
    db,
    mutators,
    retainChanges: options?.retainChanges,
    schema,
    tables: TABLES,
    visible: options?.visible,
  })
}

export type HarnessSyncServer = ReturnType<typeof createHarnessSyncServer>
