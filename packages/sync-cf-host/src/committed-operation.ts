/**
 * One unit of committed application data work, handed to a host's metering hook
 * after the work is durable.
 *
 * This module deliberately imports nothing. Both hosts that emit receipts (the
 * Orez Lite data worker's Durable Object and this package's sync Durable
 * Object) share the one contract, and orez-lite already depends on this package
 * at runtime, so the type lives here rather than growing a second definition or
 * a dependency cycle.
 */

/**
 * Who asked for the work.
 *
 * Only `application` work is metered. Schema migrations, backup exports,
 * restores and the platform's own snapshot/changefeed reads all run through the
 * same SQLite session machinery as an application statement, so they are
 * separated by declaring an origin at the call site rather than by pattern
 * matching on SQL, table names or route. A caller that declares nothing is an
 * application caller, which is the only safe default: a new platform path that
 * forgets to declare itself over-reports usage, where a new application path
 * that forgets would silently stop being metered.
 */
export type CommittedOperationOrigin = 'application' | 'platform'

/**
 * Reads and writes are priced apart and are never summed into one number, so a
 * session that both reads and writes emits one of each rather than a single
 * combined receipt.
 */
export type CommittedOperationKind = 'read' | 'write'

export type CommittedOperationSource = 'application-sql' | 'sync-pull'

export interface CommittedDataOperation {
  /**
   * The namespace the work committed in, as the emitting host knows it. The
   * Orez Lite data worker emits its canonical namespace name; the sync host
   * emits the namespace hash it forwards to its Durable Object, because the raw
   * namespace deliberately never reaches that object.
   */
  namespace: string
  kind: CommittedOperationKind
  source: CommittedOperationSource
  /**
   * Logical application rows. A read counts the rows its statements
   * materialized; a write counts SQLite's `changes()`, which excludes the rows
   * triggers and referential actions wrote on its behalf.
   */
  rows: number
  /**
   * Stable identity for this operation. Retrying an operation whose response
   * was lost repeats its receipt, so a sink that has already recorded it can
   * drop the duplicate instead of double-billing.
   */
  receipt: string
}

export type CommittedOperationHandler<Env> = (
  operation: CommittedDataOperation,
  env: Env
) => void | Promise<void>

/**
 * The receipt a committed pull reports.
 *
 * A pull is the one operation a client is expected to repeat verbatim: if the
 * response never arrives the client re-sends the same request from the same
 * cookie, the engine recomputes the same patch from the same committed state,
 * and the namespace is asked to do the work twice for one unit of usage. So the
 * receipt is derived only from what a retry reproduces exactly (who asked, and
 * which cookie it moved from and to) and never from anything per-attempt.
 *
 * Only `put` and `del` rows count. A patch also carries the client's own
 * bookkeeping, and metering it would report usage that scales with how a client
 * chose to poll rather than with the data it read.
 *
 * This lives next to the contract, rather than inline in the pull handler, so
 * the retry property can be tested against the same inputs twice without a
 * durable object in the way.
 */
export function pullCommittedOperation(
  namespace: string,
  request: { clientGroupID?: unknown; clientID?: unknown; cookie?: unknown },
  response: { cookie?: unknown; rowsPatch?: unknown }
): CommittedDataOperation {
  const patch = Array.isArray(response.rowsPatch) ? response.rowsPatch : []
  const rows = patch.filter((entry) => {
    const op = (entry as { op?: unknown } | null)?.op
    return op === 'put' || op === 'del'
  }).length
  const from = JSON.stringify(request.cookie ?? null)
  const to = JSON.stringify(response.cookie ?? null)
  return {
    namespace,
    kind: 'read',
    source: 'sync-pull',
    rows,
    receipt: `pull:${namespace}:${String(request.clientGroupID ?? '')}:${String(
      request.clientID ?? ''
    )}:${from}:${to}`,
  }
}
