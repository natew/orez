import { combineDataClients } from './combineZeroClients.js'
import { createUseQueryDirect } from './createUseQueryDirect.js'
import {
  createZeroClientInternal,
  type DirectQueryAdapter,
  type CreateZeroClientOptions,
} from './createZeroClient.js'
import { releaseClientInstances } from './instanceRegistry.js'

import type { AggregateSet } from '../aggregate.js'
import type {
  EmptyLocalSchema,
  LocalSchemaMeta,
  Schema as ZeroSchema,
} from '../zero/zero.js'
import type { GenericModels } from './types.js'

export * from './combineZeroClients.js'

export function createDataClientWithDirectQueries<
  Schema extends ZeroSchema,
  Models extends GenericModels,
  LocalSchema extends ZeroSchema = EmptyLocalSchema,
>(
  options: CreateZeroClientOptions<Schema, Models, LocalSchema>
): ReturnType<typeof createZeroClientInternal<Schema, Models, LocalSchema>> {
  const createDirectUseQuery: DirectQueryAdapter<Schema> = createUseQueryDirect
  return createZeroClientInternal<Schema, Models, LocalSchema>({
    ...options,
    createDirectUseQuery,
  })
}

export type DataInstanceManifestEntry<
  Schema extends ZeroSchema = ZeroSchema,
  Models extends GenericModels = GenericModels,
> = {
  schema: Schema
  queries: CreateZeroClientOptions<Schema, Models>['groupedQueries']
  models: Models
  aggregates?: AggregateSet<Schema>
  tables: readonly string[]
  syncTables: readonly string[]
  supportTables: readonly string[]
  scope: string | null
  defaultVisibility: ((value: string) => { column: string; value: string }) | null
}

type ClientFor<
  Entry,
  LocalSchema extends ZeroSchema = EmptyLocalSchema,
> = Entry extends DataInstanceManifestEntry<infer Schema, infer Models>
  ? ReturnType<typeof createZeroClientInternal<Schema, Models, LocalSchema>>
  : never

type ClientMap<
  Instances extends Record<string, DataInstanceManifestEntry>,
  Inner extends keyof Instances & string,
  LocalSchema extends ZeroSchema = EmptyLocalSchema,
> = {
  [Name in keyof Instances]: ClientFor<
    Instances[Name],
    Name extends Inner ? LocalSchema : EmptyLocalSchema
  >
}

type ClientUnion<
  Instances extends Record<string, DataInstanceManifestEntry>,
  Inner extends keyof Instances & string,
  LocalSchema extends ZeroSchema = EmptyLocalSchema,
> = ClientMap<Instances, Inner, LocalSchema>[keyof Instances]

export function createDataClients<
  const Instances extends Record<string, DataInstanceManifestEntry>,
  const Inner extends keyof Instances & string,
  LocalSchema extends ZeroSchema = EmptyLocalSchema,
>(
  instances: Instances,
  options: {
    inner: Inner
    localSchema?: LocalSchemaMeta<LocalSchema>
  }
) {
  const names = Object.keys(instances)
  if (names.length === 0) {
    throw new Error('[limesync] createDataClients requires at least one instance')
  }
  if (names.includes('default')) {
    names.splice(names.indexOf('default'), 1)
    names.unshift('default')
  }

  const innerName = options.inner
  if (!names.includes(innerName)) {
    throw new Error(
      `[limesync] createDataClients inner instance '${innerName}' is not in the manifest`
    )
  }
  const clients: Record<string, any> = {}

  // release the complete old partition atomically before any new instance
  // claims namespaces that may have moved during hot reload.
  releaseClientInstances(names)
  for (const name of [...names].reverse()) {
    const instance = instances[name]!
    const clientOptions = {
      schema: instance.schema,
      localSchema: name === innerName ? options.localSchema : undefined,
      models: instance.models,
      groupedQueries: instance.queries,
      instanceName: name,
      aggregates: instance.aggregates,
    }
    clients[name] =
      name === innerName
        ? createZeroClientInternal(clientOptions)
        : createDataClientWithDirectQueries(clientOptions)
  }

  const typedClients = clients as ClientMap<Instances, Inner, LocalSchema>
  // names is checked non-empty above. retain the union of the concrete client
  // types so combineDataClients can intersect every instance surface.
  const orderedClients = names.map(
    (name) => typedClients[name as keyof Instances]
  ) as unknown as [
    ClientUnion<Instances, Inner, LocalSchema>,
    ...ClientUnion<Instances, Inner, LocalSchema>[],
  ]
  const providers = Object.fromEntries(
    names.map((name) => [name, clients[name]!.ProvideData])
  ) as {
    [Name in keyof Instances]: ClientMap<Instances, Inner, LocalSchema>[Name]['ProvideData']
  }

  return {
    clients: typedClients,
    providers,
    combined: combineDataClients<
      [
        ClientUnion<Instances, Inner, LocalSchema>,
        ...ClientUnion<Instances, Inner, LocalSchema>[],
      ]
    >(...orderedClients, { inner: innerName }),
  }
}
