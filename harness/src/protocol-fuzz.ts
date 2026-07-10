// Deterministic malformed-protocol lane for the Rust native and CF hosts.
// Seeded structural mutations over valid pull/push skeletons, constrained to
// shape-INVALID cases (checked against a predicate mirroring the engine's wire
// validation), so every case must return 4xx: a 2xx means the host accepted
// garbage, a 5xx means it crashed on it. App-layer classification of a
// shape-valid push (e.g. unknown mutator name) is the consumer's choice in the
// reference and is deliberately out of scope here. It never sends fixture row
// contents or logs request bodies.
import { parseArgs } from 'node:util'

import { startRustCf } from './targets/rust-cf.js'
import { startRustLocal } from './targets/rust-local.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'rust-local' },
    cases: { type: 'string', default: '10000' },
    seed: { type: 'string', default: '1' },
    concurrency: { type: 'string', default: '20' },
  },
})

const total = Number(args.cases)
const seed = Number(args.seed)
const concurrency = Number(args.concurrency)
if (!Number.isSafeInteger(total) || total < 1) throw new Error('cases must be positive')
if (!Number.isSafeInteger(seed)) throw new Error('seed must be an integer')
if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 100) {
  throw new Error('concurrency must be 1..100')
}

const target =
  args.target === 'rust-cf'
    ? await startRustCf({ queryAware: false, pullIntervalMs: 0 })
    : args.target === 'rust-local'
      ? await startRustLocal({ queryAware: false, pullIntervalMs: 0 })
      : (() => {
          throw new Error('target must be rust-local or rust-cf')
        })()

const origin =
  'origin' in target ? target.origin : `${target.baseUrl}/${target.namespace}`

function mulberry32(initial: number) {
  let value = initial
  return () => {
    value |= 0
    value = (value + 0x6d2b79f5) | 0
    let mixed = Math.imul(value ^ (value >>> 15), 1 | value)
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed
    return (mixed ^ (mixed >>> 14)) >>> 0
  }
}

const random = mulberry32(seed)

// hand-written anchors kept from the original corpus (regressions with known
// histories, e.g. the non-object bodies that once bypassed requestObject)
const anchors = [
  '',
  '{',
  '[',
  'null',
  '[]',
  '{}',
  '{"clientID":1}',
  '{"clientID":"x","clientGroupID":null,"cookie":null}',
  '{"clientID":"x","clientGroupID":"g","cookie":-1}',
  '{"clientID":"x","clientGroupID":"g","cookie":"not-a-counter"}',
  '{"pushVersion":1,"clientGroupID":"g","mutations":null}',
  '{"pushVersion":1,"clientGroupID":"g","mutations":[{}]}',
  '{"pushVersion":1,"clientGroupID":"g","mutations":[{"type":"custom","clientID":"c","id":0,"name":1,"args":[]}]}',
] as const

type Route = 'pull' | 'push'
type Tree = Record<string, unknown>

function skeleton(route: Route): Tree {
  return route === 'pull'
    ? { clientID: 'fuzz-client', clientGroupID: 'fuzz-group', cookie: null }
    : {
        pushVersion: 1,
        clientGroupID: 'fuzz-group',
        mutations: [
          {
            type: 'custom',
            clientID: 'fuzz-client',
            id: 1,
            name: 'project.create',
            args: [{ id: 'fuzz-row', ownerId: 'fuzz-user', name: 'fuzz' }],
          },
        ],
      }
}

function randomScalar(): unknown {
  switch (random() % 14) {
    case 0:
      return null
    case 1:
      return -1
    case 2:
      return 0
    case 3:
      return 2 ** 60
    case 4:
      return 1.5
    case 5:
      return ''
    case 6:
      return 'x'.repeat(1 + (random() % 2048))
    case 7:
      return true
    case 8:
      return false
    case 9:
      return {}
    case 10:
      return []
    case 11:
      return '\u0000\uffff\u{1f600}'
    case 12:
      return [[[[[[random() % 10]]]]]]
    default:
      return { unexpected: String(random()) }
  }
}

// every (parent, key) location in the tree, so mutations can hit nested
// mutation fields as easily as top-level ones
function locations(
  value: unknown
): Array<{ parent: Tree | unknown[]; key: string | number }> {
  const found: Array<{ parent: Tree | unknown[]; key: string | number }> = []
  const walk = (node: unknown) => {
    if (Array.isArray(node)) {
      node.forEach((child, index) => {
        found.push({ parent: node, key: index })
        walk(child)
      })
    } else if (node && typeof node === 'object') {
      for (const key of Object.keys(node)) {
        found.push({ parent: node as Tree, key })
        walk((node as Tree)[key])
      }
    }
  }
  walk(value)
  return found
}

// wire.rs non_negative_safe_int: number, integer-valued, 0..=2^53
function safeCounterNumber(value: unknown): boolean {
  return (
    typeof value === 'number' &&
    Number.isFinite(value) &&
    value % 1 === 0 &&
    value >= 0 &&
    value <= Number.MAX_SAFE_INTEGER
  )
}

// wire.rs parse_cookie: null | safe non-negative number | canonical base-10
// string (no sign, no leading zeros); over-accepting at the i64 boundary is
// fine — a predicate-valid case is skipped, never falsely asserted
function validCookie(value: unknown): boolean {
  if (value === null) return true
  if (safeCounterNumber(value)) return true
  return typeof value === 'string' && /^(0|[1-9]\d{0,18})$/.test(value)
}

// mirrors crates/sync-core pull.rs / push.rs wire validation: anything this
// predicate accepts could reach the engine (or a consumer mutator), so the
// generator must never send it. NOTE pushVersion only has to be a finite
// number for the shape to be accepted — a value != 1 gets the stock 200
// unsupportedPushVersion response, same as the reference.
function shapeValid(route: Route, body: unknown): boolean {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return false
  const value = body as Tree
  if (route === 'pull') {
    return (
      typeof value.clientID === 'string' &&
      typeof value.clientGroupID === 'string' &&
      'cookie' in value &&
      validCookie(value.cookie)
    )
  }
  if (typeof value.clientGroupID !== 'string') return false
  if (typeof value.pushVersion !== 'number' || !Number.isFinite(value.pushVersion))
    return false
  if (!Array.isArray(value.mutations)) return false
  return value.mutations.every((m) => {
    if (!m || typeof m !== 'object' || Array.isArray(m)) return false
    const mutation = m as Tree
    return (
      mutation.type === 'custom' &&
      typeof mutation.clientID === 'string' &&
      typeof mutation.name === 'string' &&
      Array.isArray(mutation.args) &&
      safeCounterNumber(mutation.id) &&
      mutation.id !== 0
    )
  })
}

function generateCase(): { route: Route; body: string } {
  const route: Route = random() % 2 === 0 ? 'pull' : 'push'
  // cross-route sends a valid body of the OTHER protocol shape
  if (random() % 10 === 0) {
    const other: Route = route === 'pull' ? 'push' : 'pull'
    return { route, body: JSON.stringify(skeleton(other)) }
  }
  const tree = structuredClone(skeleton(route))
  const edits = 1 + (random() % 3)
  for (let edit = 0; edit < edits; edit++) {
    const spots = locations(tree)
    const spot = spots[random() % spots.length]!
    switch (random() % 4) {
      case 0:
        // type-flip / value corruption
        ;(spot.parent as Tree)[spot.key as string] = randomScalar()
        break
      case 1:
        // drop a field entirely (array elements are nulled, not spliced)
        if (Array.isArray(spot.parent)) spot.parent[spot.key as number] = null
        else delete spot.parent[spot.key as string]
        break
      case 2:
        // inject an unexpected sibling key
        if (!Array.isArray(spot.parent)) {
          spot.parent[`fuzz_${random() % 1000}`] = randomScalar()
        }
        break
      default:
        // corrupt nested structure wholesale
        ;(spot.parent as Tree)[spot.key as string] =
          random() % 2 === 0 ? [skeleton(route)] : String(random())
        break
    }
  }
  if (random() % 8 === 0) {
    // serialization-level corruption: any proper prefix of minified JSON
    // starting with an object is unparseable
    const text = JSON.stringify(tree)
    return { route, body: text.slice(0, 1 + (random() % (text.length - 1))) }
  }
  if (shapeValid(route, tree)) {
    // the edits happened to keep the shape intact; force-break it so the
    // strict 4xx assertion holds for every sent case
    delete tree.clientGroupID
  }
  return { route, body: JSON.stringify(tree) }
}

const corpus: Array<{ route: Route; body: string }> = []
for (const body of anchors) {
  // route each anchor where it is malformed: a template that parses as a
  // valid pull (e.g. the bare {clientID, clientGroupID, cookie} shape) must
  // hit /push, or the strict 4xx assertion would trip on a legitimate 200
  let parsed: unknown
  let parseable = true
  try {
    parsed = JSON.parse(body)
  } catch {
    parseable = false
  }
  const route: Route =
    parseable && shapeValid('pull', parsed)
      ? 'push'
      : parseable && shapeValid('push', parsed)
        ? 'pull'
        : corpus.length % 2 === 0
          ? 'pull'
          : 'push'
  corpus.push({ route, body })
}
while (corpus.length < total) corpus.push(generateCase())
corpus.length = total

let next = 0
let completed = 0
const statuses = new Map<number, number>()
const started = performance.now()

async function worker() {
  while (true) {
    const index = next++
    if (index >= total) return
    const { route, body } = corpus[index]!
    const requestStarted = performance.now()
    const response = await fetch(`${origin}/${route}`, {
      method: 'POST',
      headers: {
        authorization: 'Bearer token-fuzz-user',
        'content-type': 'application/json',
      },
      body,
      signal: AbortSignal.timeout(2_000),
    }).catch((error: unknown) => {
      throw new Error(
        `case ${index} route=${route} failed after ${Math.round(performance.now() - requestStarted)}ms: ${error}`
      )
    })
    await response.arrayBuffer()
    const elapsed = performance.now() - requestStarted
    if (elapsed > 2_000) throw new Error(`case ${index} exceeded 2s (${elapsed}ms)`)
    if (response.status < 400 || response.status > 499) {
      throw new Error(`case ${index} route=${route} expected 4xx, got ${response.status}`)
    }
    statuses.set(response.status, (statuses.get(response.status) ?? 0) + 1)
    completed++
  }
}

try {
  await Promise.all(Array.from({ length: concurrency }, () => worker()))

  const validPull = await fetch(`${origin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-fuzz-user',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'fuzz-valid',
      clientGroupID: 'fuzz-valid-group',
      cookie: null,
    }),
  })
  await validPull.arrayBuffer()
  if (validPull.status !== 200) {
    throw new Error(`valid post-fuzz pull failed ${validPull.status}`)
  }

  console.log(
    JSON.stringify({
      lane: 'protocol-fuzz',
      result: 'PASS',
      target: args.target,
      seed,
      cases: completed,
      concurrency,
      elapsedMs: Math.round(performance.now() - started),
      statuses: Object.fromEntries([...statuses].sort(([left], [right]) => left - right)),
    })
  )
} finally {
  await target.close()
}
