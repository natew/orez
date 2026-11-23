import type { ClientState, DesiredQuery } from './types.js'

export const pullQueryPatch = Symbol('native Zero desired query patch')

type QueryPatch = {
  baseVersion: number
  version: number
  patch: (
    | { op: 'clear' }
    | { op: 'del'; hash: string }
    | ({ op: 'put' } & DesiredQuery)
  )[]
}

export type PullState = ClientState & { [pullQueryPatch]?: QueryPatch | null }

export function diffPullQueries(
  state: Pick<ClientState, 'queries' | 'queryVersion'>,
  previous?: Pick<ClientState, 'queries' | 'queryVersion'>
): QueryPatch | null {
  const patch: QueryPatch['patch'] = []
  if (!previous) patch.push({ op: 'clear' })
  const retained = new Map(previous?.queries.map((query) => [query.hash, query]))
  for (const query of state.queries) {
    const old = retained.get(query.hash)
    retained.delete(query.hash)
    if (!old || JSON.stringify(old) !== JSON.stringify(query))
      patch.push({ op: 'put', ...query })
  }
  for (const hash of retained.keys()) patch.push({ op: 'del', hash })
  return patch.length
    ? { baseVersion: previous?.queryVersion ?? 0, version: state.queryVersion, patch }
    : null
}
