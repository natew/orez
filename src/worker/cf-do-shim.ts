/**
 * namespace routing primitives for the Cloudflare Durable Object deploy.
 *
 * a multi-tenant CF/orez deploy shards data into one DO instance per tenant
 * namespace (`ns:<scope>-<id>`) plus a control-plane `singleton`. the worker
 * tiers carry the chosen namespace in a header (and re-stamp it so an inbound
 * value is never trusted) and must validate its shape before routing — an
 * unvalidated namespace would let a client mint unbounded DO instances.
 *
 * the deployed worker entry classes are bundled strings in the consumer's
 * deploy integration (awkward to unit-test), so the routing decision and the
 * security-relevant shape validation live here as pure functions, tested in
 * cf-do-shim.test.ts. consumers import these into their worker shims instead of
 * copy-pasting the validation regex at every routing site.
 */

/** default namespace scope prefixes (`proj-<id>`, `test-<id>`). */
const DEFAULT_SCOPES = ['proj', 'test'] as const

export interface NamespaceRoutingOptions {
  /** allowed namespace scope prefixes. default `['proj', 'test']`. */
  scopes?: readonly string[]
  /**
   * namespace values that resolve to the control-plane `singleton` instance,
   * in addition to the empty string (which is always the singleton).
   */
  controlPlaneNamespaces?: readonly string[]
  /** request header the worker tiers carry the namespace in. default `x-orez-ns`. */
  nsHeader?: string
}

function escapeForRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function namespacePattern(scopes: readonly string[]): RegExp {
  const group = scopes.map(escapeForRegExp).join('|')
  return new RegExp(`^(?:${group})-[A-Za-z0-9_-]{1,64}$`)
}

/**
 * true when `ns` is a structurally valid tenant namespace: a configured scope
 * prefix followed by a 1-64 char `[A-Za-z0-9_-]` id. this is the gate that
 * keeps a stray header from minting an unbounded DO instance, so every routing
 * site that forwards a namespace should run it.
 */
export function isValidNamespace(
  ns: string,
  opts: NamespaceRoutingOptions = {}
): boolean {
  return namespacePattern(opts.scopes ?? DEFAULT_SCOPES).test(ns)
}

/**
 * resolve a raw namespace string to a DO instance name:
 *   - `''` or a control-plane alias -> `'singleton'`
 *   - a valid tenant namespace      -> `'ns:<ns>'`
 *   - anything else                 -> `null` (caller should reject the request)
 */
export function doInstanceName(
  ns: string,
  opts: NamespaceRoutingOptions = {}
): string | null {
  if (!ns) return 'singleton'
  if ((opts.controlPlaneNamespaces ?? []).includes(ns)) return 'singleton'
  if (!isValidNamespace(ns, opts)) return null
  return 'ns:' + ns
}

interface HeaderReader {
  get(name: string): string | null
}

/**
 * read the namespace from a request (the configured header, falling back to the
 * `?ns=` query param) and resolve it to a DO instance name. returns `null` for a
 * structurally invalid namespace so the worker can reply 400 instead of routing.
 */
export function doInstanceNameForRequest(
  request: { headers: HeaderReader },
  url: { searchParams: HeaderReader },
  opts: NamespaceRoutingOptions = {}
): string | null {
  const ns =
    request.headers.get(opts.nsHeader ?? 'x-orez-ns') || url.searchParams.get('ns') || ''
  return doInstanceName(ns, opts)
}
