import { setMutationsPermissions } from './modelRegistry.js'
import { getSchema, getZQL } from './state.js'

import type { TableBuilderWithColumns } from '../zero/zero.js'
import type {
  MutatorContext,
  Schema,
  TableInsertRow,
  TableName,
  TableUpdateRow,
  Where,
} from './types.js'

// derive a TableBuilderWithColumns from the global schema by table name
type SchemaTableBuilder<TName extends TableName> = TableBuilderWithColumns<
  Schema['tables'][TName]
>

// HMR registry - stores mutation implementations and proxies by table name
// allows hot-swapping implementations without changing object references
// stored on globalThis to persist across HMR module reloads
function mutationRegistry(): Map<string, Record<string, Function>> {
  const global = globalThis as typeof globalThis & {
    __onZeroMutationRegistry__?: Map<string, Record<string, Function>>
  }
  return (global.__onZeroMutationRegistry__ ||= new Map())
}

function proxyRegistry(): Map<string, any> {
  const global = globalThis as typeof globalThis & {
    __onZeroProxyRegistry__?: Map<string, any>
  }
  return (global.__onZeroProxyRegistry__ ||= new Map())
}

// get or create a proxy that delegates to the registry
// returns the SAME proxy object on subsequent calls so HMR works
function getOrCreateMutationProxy<T extends Record<string, Function>>(
  tableName: string,
  implementations: T,
  crud = true
): T {
  // merge with any prior registration for this table: multiple modules may
  // register mutators on the same table (a seed.ts alongside the table's own
  // mutations file) and module import order is arbitrary, so replacing
  // wholesale drops whichever module registered first. per-key replacement
  // still supports HMR updates of edited handlers.
  const prior = mutationRegistry().get(tableName)
  if (prior && !crud) {
    for (const action of ['insert', 'update', 'delete', 'upsert']) {
      if (!(action in implementations)) delete prior[action]
    }
  }
  mutationRegistry().set(
    tableName,
    prior ? { ...prior, ...implementations } : implementations
  )

  // return existing proxy if we have one (HMR case)
  const existing = proxyRegistry().get(tableName)
  if (existing) {
    return existing as T
  }

  // first time - create the proxy
  const proxy = new Proxy({} as T, {
    get(_, key: string) {
      return mutationRegistry().get(tableName)?.[key]
    },
    ownKeys() {
      const current = mutationRegistry().get(tableName)
      return current ? Object.keys(current) : []
    },
    getOwnPropertyDescriptor(_, key: string) {
      const current = mutationRegistry().get(tableName)
      if (current && key in current) {
        return { enumerable: true, configurable: true, value: current[key] }
      }
    },
    has(_, key: string) {
      const current = mutationRegistry().get(tableName)
      return current ? key in current : false
    },
  })

  proxyRegistry().set(tableName, proxy)
  return proxy
}

type MutationBuilder<Obj = any> = (ctx: MutatorContext, obj?: Obj) => Promise<void>
type MutationBuilders = Record<string, MutationBuilder>

type GenericTable = TableBuilderWithColumns<any>

type CRUDMutations<Table extends GenericTable> = {
  insert: MutationBuilder<TableInsertRow<Table>>
  upsert: MutationBuilder<TableInsertRow<Table>>
  update: MutationBuilder<TableUpdateRow<Table>>
  delete: MutationBuilder<TableUpdateRow<Table>>
}

type CRUDNames = 'insert' | 'upsert' | 'update' | 'delete'

type MutationOptions<CRUD extends boolean = boolean> = { crud?: CRUD }

type MutationsWithCRUD<Table extends GenericTable, Mutations extends MutationBuilders> = {
  [Key in CRUDNames | keyof Mutations]: Key extends keyof Mutations
    ? Mutations[Key]
    : Key extends keyof CRUDMutations<any>
      ? CRUDMutations<Table>[Key]
      : never
}

export function mutations<Mutations extends MutationBuilders>(
  mutations: Mutations
): Mutations
export function mutations<Table extends GenericTable, Permissions extends Where>(
  table: Table,
  permissions: Permissions
): MutationsWithCRUD<Table, {}>
export function mutations<
  Table extends GenericTable,
  Permissions extends Where,
  Mutations extends MutationBuilders,
  CRUD extends boolean = true,
>(
  table: Table,
  permissions: Permissions,
  mutations: Mutations,
  options?: MutationOptions<CRUD>
): CRUD extends false ? Mutations : MutationsWithCRUD<Table, Mutations>
// string-based overloads (for drizzle-zero derived schemas where table builders aren't available)
export function mutations<TName extends TableName, Permissions extends Where>(
  tableName: TName,
  permissions: Permissions
): MutationsWithCRUD<SchemaTableBuilder<TName>, {}>
export function mutations<
  TName extends TableName,
  Permissions extends Where,
  Mutations extends MutationBuilders,
  CRUD extends boolean = true,
>(
  tableName: TName,
  permissions: Permissions,
  mutations: Mutations,
  options?: MutationOptions<CRUD>
): CRUD extends false
  ? Mutations
  : MutationsWithCRUD<SchemaTableBuilder<TName>, Mutations>
export function mutations<
  Table extends GenericTable,
  Mutations extends Record<string, MutationBuilder>,
>(
  table: Table | string | Mutations,
  permissions?: Where,
  mutations?: Mutations,
  options?: MutationOptions
): Mutations {
  if (permissions) {
    const tableName =
      typeof table === 'string'
        ? (table as TableName)
        : ((table as Table).schema.name as TableName)

    setMutationsPermissions(tableName, permissions)
    if (options?.crud === false) {
      return getOrCreateMutationProxy(tableName, mutations ?? {}, false) as Mutations
    }

    const createCRUDMutation = (action: CRUDNames) => {
      const customMutation = mutations?.[action]
      if (customMutation) return customMutation

      return async (ctx: MutatorContext, obj: any) => {
        // bound can checks authoritative writes, including browser-hosted servers.
        // optimistic clients skip the existence read because their cache is partial.
        if (action === 'upsert' && ctx.environment === 'server') {
          let existing = getZQL()[tableName]
          for (const key of getSchema().tables[tableName].primaryKey) {
            existing = existing.where(key, obj[key])
          }
          if (await ctx.tx.run(existing.one())) {
            await ctx.can(permissions, obj)
          }
        } else if (action === 'update' || action === 'delete') {
          await ctx.can(permissions, obj)
        }

        type TableName = keyof typeof ctx.tx.mutate // weird type foo because we declare this module and then type check
        await ctx.tx.mutate[tableName as TableName]![action](obj)

        if (action !== 'delete') {
          await ctx.can(permissions, obj)
        }
      }
    }

    const crudMutations: CRUDMutations<any> = {
      insert: createCRUDMutation('insert'),
      update: createCRUDMutation('update'),
      delete: createCRUDMutation('delete'),
      upsert: createCRUDMutation('upsert'),
    }

    const finalMutations = {
      ...mutations,
      // generated CRUD fills missing operations; custom CRUD runs as-is
      ...crudMutations,
    } as any as Mutations

    // return proxy for HMR support - allows swapping implementations at runtime
    return getOrCreateMutationProxy(tableName, finalMutations)
  }

  // no schema/permissions don't add CRUD
  return table as any
}
