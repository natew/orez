import type { AnyQueryRegistry } from '@rocicorp/zero'
import type { JsonValue } from 'orez-sync-executor'

// Zero separates registry path segments with '.' or '|' (its getValueAtPath).
const NAME_SEPARATOR = /[.|]/

/**
 * Turn a client's desired-query patch into the resolved patch sync-core takes.
 *
 * Every named query resolves in-process against the app's ordinary Zero query
 * registry — the same object the client was built with. `CustomQuery.fn`
 * validates the arguments and applies the query context, so this is exactly
 * the transform zero-cache delegates to an app endpoint, minus the network.
 * The context is the host's authenticated identity (claims on the CF host,
 * authData on the browser host); a query definition scopes its rows from it.
 *
 * Resolution is structural on purpose: the registry is walked as a plain
 * object and the built query's `ast` is read directly, never through Zero's
 * `asQueryInternals`, whose instance-tag check fails whenever the app's
 * registry and this host resolve different copies of the zero module.
 *
 * A query the registry does not contain at all is version skew, not a bug: a
 * shipped native client is always older than the server it talks to, so it can
 * ask for a query that has since been renamed or removed. That put is dropped
 * and the rest of the patch resolves, leaving the client without rows for that
 * one query. Refusing the whole pull instead would take an app that is one
 * feature out of date and stop it syncing anything at all, which is what
 * happened to Contrast mobile on 2026-08-20.
 *
 * A query that IS registered but fails to build is a real fault in the app's
 * own code, so it still fails the whole patch and names the query it came from.
 */
export function resolveQueryPatch(
  patch: readonly unknown[],
  queries: AnyQueryRegistry,
  context: unknown,
  transformVersion: number,
  fail: (message: string) => Error
): unknown[] {
  const result: unknown[] = []
  for (const operation of patch) {
    if (!operation || typeof operation !== 'object') {
      result.push(operation)
      continue
    }
    const op = operation as Record<string, unknown>
    if (op.op !== 'put') {
      result.push(operation)
      continue
    }
    if (typeof op.name !== 'string') {
      throw fail('query put requires a server-resolved named query')
    }
    if (!Array.isArray(op.args)) throw fail('named query args must be an array')
    let node: unknown = queries
    for (const segment of op.name.split(NAME_SEPARATOR)) {
      node =
        node && typeof node === 'object'
          ? (node as Record<string, unknown>)[segment]
          : undefined
    }
    const custom = node as
      | { fn?: (options: { args: unknown; ctx: unknown }) => unknown }
      | undefined
    if (!custom || typeof custom.fn !== 'function') {
      // version skew: drop this query and keep syncing the rest.
      console.warn(`orez: dropping unknown named query from pull: ${op.name}`)
      continue
    }
    let ast: JsonValue
    try {
      const built = custom.fn({
        args: (op.args as JsonValue[])[0],
        ctx: context,
      }) as { ast?: unknown } | null
      if (!built || typeof built !== 'object' || built.ast === undefined) {
        throw new Error('query did not build to an AST-bearing Zero query')
      }
      ast = built.ast as JsonValue
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error)
      throw fail(`named query failed to build: ${op.name}: ${detail}`)
    }
    result.push({
      op: 'put',
      hash: op.hash,
      ast,
      transformVersion,
    })
  }
  return result
}
