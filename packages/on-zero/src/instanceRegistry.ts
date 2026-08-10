// registry mapping query/mutator namespaces to the zero client instance that
// owns them. enables multiple createZeroClient instances on one page (e.g. a
// control-plane instance + a per-project instance): the ambient run() and the
// combineZeroClients facade dispatch each query/mutation to the owning
// instance by its registered namespace instead of whichever mounted last.
//
// stored via globalValue so dual-loaded module copies (cjs/esm) share one map,
// matching the package's other cross-module registries.

import { globalValue } from './helpers/globalValue'
import { getQueryName } from './queryRegistry'

import type { ZeroRunner } from './zeroRunner'
import type { AnyQueryRegistry } from '@rocicorp/zero'

export type ZeroClientRuntime = {
  zero: unknown | null
  readyWaiters: Set<(zero: unknown) => void>
  runner: ZeroRunner | null
}

export type ZeroClientInstance = {
  name: string
  customQueries: AnyQueryRegistry
  runtime: ZeroClientRuntime
  // set when the instance's provider mounts (client); stays null on the
  // server, where the ambient transaction runner serves every instance
  runner: ZeroRunner | null
}

const getRuntimesByName = () =>
  globalValue<Map<string, ZeroClientRuntime>>(
    'on-zero:instance-runtimes-by-name',
    () => new Map()
  )

const getInstancesByNamespace = () =>
  globalValue<Map<string, ZeroClientInstance>>(
    'on-zero:instances-by-namespace',
    () => new Map()
  )

const getInstancesByQueryName = () =>
  globalValue<Map<string, ZeroClientInstance>>(
    'on-zero:instances-by-query-name',
    () => new Map()
  )

export function releaseClientInstances(names: readonly string[]): void {
  const releasing = new Set(names)
  const instancesByNamespace = getInstancesByNamespace()
  const instancesByQueryName = getInstancesByQueryName()
  for (const [namespace, owner] of instancesByNamespace) {
    if (releasing.has(owner.name)) instancesByNamespace.delete(namespace)
  }
  for (const [queryName, owner] of instancesByQueryName) {
    if (releasing.has(owner.name)) instancesByQueryName.delete(queryName)
  }
}

export function registerClientInstance({
  name,
  namespaces,
  customQueries,
  queryNames = [],
}: {
  name: string
  namespaces: string[]
  customQueries: AnyQueryRegistry
  queryNames?: string[]
}): ZeroClientInstance {
  const instancesByNamespace = getInstancesByNamespace()
  const instancesByQueryName = getInstancesByQueryName()

  // re-creating an instance under the same name (hmr) replaces its claims
  for (const [namespace, owner] of instancesByNamespace) {
    if (owner.name === name) {
      instancesByNamespace.delete(namespace)
    }
  }
  for (const [queryName, owner] of instancesByQueryName) {
    if (owner.name === name) {
      instancesByQueryName.delete(queryName)
    }
  }

  // the provider and its project module do not necessarily refresh together.
  // keep their live zero behind the stable instance name so a new module facade
  // resolves the provider that is already mounted.
  const runtimesByName = getRuntimesByName()
  let runtime = runtimesByName.get(name)
  if (!runtime) {
    runtime = { zero: null, readyWaiters: new Set(), runner: null }
    runtimesByName.set(name, runtime)
  }
  const instance: ZeroClientInstance = {
    name,
    customQueries,
    runtime,
    get runner() {
      return runtime.runner
    },
    set runner(next) {
      runtime.runner = next
    },
  }

  for (const namespace of namespaces) {
    const existing = instancesByNamespace.get(namespace)
    if (existing) {
      throw new Error(
        `[on-zero] namespace '${namespace}' is already claimed by zero client instance '${existing.name}' ` +
          `(while creating instance '${name}'). Each query/mutator namespace must belong to exactly one createZeroClient instance.`
      )
    }
    instancesByNamespace.set(namespace, instance)
  }

  for (const queryName of queryNames) {
    const existing = instancesByQueryName.get(queryName)
    if (existing) {
      throw new Error(
        `[on-zero] query '${queryName}' is already claimed by zero client instance '${existing.name}' ` +
          `(while creating instance '${name}'). Each query function must belong to exactly one createZeroClient instance.`
      )
    }
    instancesByQueryName.set(queryName, instance)
  }

  return instance
}

export function getInstanceForNamespace(
  namespace: string
): ZeroClientInstance | undefined {
  return getInstancesByNamespace().get(namespace)
}

export function getInstanceForQueryFn(fn: Function): ZeroClientInstance | undefined {
  const queryName = getQueryName(fn)
  if (!queryName) return undefined
  const queryOwner = getInstancesByQueryName().get(queryName)
  if (queryOwner) return queryOwner
  return getInstanceForNamespace(queryName.split('.', 1)[0])
}
