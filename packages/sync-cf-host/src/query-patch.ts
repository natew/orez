import type { JsonValue } from 'orez-sync-executor'

export type QueryResolutionRequest = {
  readonly name: string
  readonly args: readonly JsonValue[]
}

/**
 * One resolved query, positionally matched to the request at the same index.
 *
 * An `error` fails only its own query rather than the whole patch, so a client
 * that registers one unknown query alongside nine good ones is told which one
 * is unknown.
 */
export type QueryResolution = { readonly ast: JsonValue } | { readonly error: string }

/**
 * Turn a client's desired-query patch into the resolved patch sync-core takes.
 *
 * Both hosts share this because both must agree on exactly one thing: how many
 * times a consumer's resolver is called for one patch. Resolving per query made
 * a pull cost one application round trip per registered query, each one
 * re-authenticating, so a screen mounting eleven views paid eleven of them in
 * series. Everything else here exists to make the single call safe: the patch
 * keeps its order, duplicate (name, args) pairs collapse to one request and fan
 * back out, and a resolver error names the query it came from.
 */
export async function resolveQueryPatch(
  patch: readonly unknown[],
  resolve: (
    requests: readonly QueryResolutionRequest[]
  ) => readonly QueryResolution[] | Promise<readonly QueryResolution[]>,
  transformVersion: number,
  fail: (message: string) => Error
): Promise<unknown[]> {
  const puts: { index: number; key: string; name: string; hash: unknown }[] = []
  const requests: QueryResolutionRequest[] = []
  const requestByKey = new Map<string, number>()

  patch.forEach((operation, index) => {
    if (!operation || typeof operation !== 'object') return
    const op = operation as Record<string, unknown>
    if (op.op !== 'put') return
    if (typeof op.name !== 'string') {
      throw fail('query put requires a server-resolved named query')
    }
    if (!Array.isArray(op.args)) throw fail('named query args must be an array')
    const key = JSON.stringify([op.name, op.args])
    if (!requestByKey.has(key)) {
      requestByKey.set(key, requests.length)
      requests.push({ name: op.name, args: op.args as JsonValue[] })
    }
    puts.push({ index, key, name: op.name, hash: op.hash })
  })

  if (puts.length === 0) return [...patch]
  const resolved = await resolve(requests)
  if (resolved.length !== requests.length) {
    throw new Error(
      `query resolver returned ${resolved.length} results for ${requests.length} queries`
    )
  }

  const result = [...patch]
  for (const put of puts) {
    const resolution = resolved[requestByKey.get(put.key)!]
    if (!resolution || 'error' in resolution) {
      throw fail(`unknown or unsupported named query: ${put.name}`)
    }
    result[put.index] = {
      op: 'put',
      hash: put.hash,
      ast: resolution.ast,
      transformVersion,
    }
  }
  return result
}
