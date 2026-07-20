import { combineZeroClients } from './combineZeroClients'
import { createUseQueryDirect } from './createUseQueryDirect'
import {
  createZeroClientInternal,
  type DirectQueryAdapter,
  type CreateZeroClientOptions,
} from './createZeroClient'
import { releaseClientInstances } from './instanceRegistry'

import type { GenericModels } from './types'
import type { Schema as ZeroSchema } from '@rocicorp/zero'

export * from './combineZeroClients'

export function createZeroClientWithDirectQueries<
  Schema extends ZeroSchema,
  Models extends GenericModels,
>(
  options: CreateZeroClientOptions<Schema, Models>
): ReturnType<typeof createZeroClientInternal<Schema, Models>> {
  const createDirectUseQuery: DirectQueryAdapter<Schema> = createUseQueryDirect
  return createZeroClientInternal<Schema, Models>({
    ...options,
    createDirectUseQuery,
  })
}

export type ZeroInstanceManifestEntry<
  Schema extends ZeroSchema = ZeroSchema,
  Models extends GenericModels = GenericModels,
> = {
  schema: Schema
  queries: CreateZeroClientOptions<Schema, Models>['groupedQueries']
  models: Models
  tables: readonly string[]
  syncTables: readonly string[]
  scope: string | null
  defaultVisibility: ((value: string) => { column: string; value: string }) | null
}

type ClientFor<Entry> =
  Entry extends ZeroInstanceManifestEntry<infer Schema, infer Models>
    ? ReturnType<typeof createZeroClientInternal<Schema, Models>>
    : never

type ClientMap<Instances extends Record<string, ZeroInstanceManifestEntry>> = {
  [Name in keyof Instances]: ClientFor<Instances[Name]>
}

export function createZeroClients<
  const Instances extends Record<string, ZeroInstanceManifestEntry>,
>(instances: Instances) {
  const names = Object.keys(instances)
  if (names.length === 0) {
    throw new Error('[on-zero] createZeroClients requires at least one instance')
  }
  if (names.includes('default')) {
    names.splice(names.indexOf('default'), 1)
    names.unshift('default')
  }

  const innerName = names[names.length - 1]!
  const clients: Record<string, ReturnType<typeof createZeroClientInternal>> = {}

  // release the complete old partition atomically before any new instance
  // claims namespaces that may have moved during hot reload.
  releaseClientInstances(names)
  for (const name of [...names].reverse()) {
    const instance = instances[name]!
    const options = {
      schema: instance.schema,
      models: instance.models,
      groupedQueries: instance.queries,
      instanceName: name,
    }
    clients[name] =
      name === innerName
        ? createZeroClientInternal(options)
        : createZeroClientWithDirectQueries(options)
  }

  const orderedClients = names.map((name) => clients[name]!) as [
    ReturnType<typeof createZeroClientInternal>,
    ...ReturnType<typeof createZeroClientInternal>[],
  ]
  const typedClients = clients as ClientMap<Instances>
  const providers = Object.fromEntries(
    names.map((name) => [name, clients[name]!.ProvideZero])
  ) as { [Name in keyof Instances]: ClientMap<Instances>[Name]['ProvideZero'] }

  return {
    clients: typedClients,
    providers,
    combined: combineZeroClients(...orderedClients),
  }
}
