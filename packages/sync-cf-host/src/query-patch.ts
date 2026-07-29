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
 * A resolution error fails the whole patch (the pull is refused as one unit),
 * but the refusal names the query it came from, so a client that registers
 * one unknown query alongside nine good ones is told which one is unknown.
 */
export function resolveQueryPatch(
  patch: readonly unknown[],
  queries: AnyQueryRegistry,
  context: unknown,
  transformVersion: number,
  fail: (message: string) => Error
): unknown[] {
  const result = [...patch]
  patch.forEach((operation, index) => {
    if (!operation || typeof operation !== 'object') return
    const op = operation as Record<string, unknown>
    if (op.op !== 'put') return
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
      throw fail(`unknown or unsupported named query: ${op.name}`)
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
      throw fail(`unknown or unsupported named query: ${op.name}: ${detail}`)
    }
    result[index] = {
      op: 'put',
      hash: op.hash,
      ast,
      transformVersion,
    }
  })
  return result
}
