// red-proof for the intermittent query-diff `allProjects` completion stall.
//
// this vendored transport is a snapshot of ~/orez/src/zero-http/transport.ts.
// when it drifted behind the canonical fix (1efd3e5) the query-diff --against
// rust-cf lane went intermittently red: a got ack rides an early poke, then an
// immediately following snapshot-reset pull (leading rowsPatch op:'clear')
// wipes the client's entire replicache space — rows AND got-query marks — and
// the transport never re-asserts an ack it believes was delivered, so the
// materialized view never reaches 'complete'. this pins that ordering
// deterministically: red before the re-assert fix, green after.
import { expect, test } from 'bun:test'

import { Zero } from '@rocicorp/zero'

import { mutators, queries, schema } from '../fixture.js'
import { installHttpPullTransport } from './httpPullTransport.js'

const ORIGIN = 'https://zero-http-stall.local'

function jsonResponse(body: unknown) {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

// query-aware server mock. every /pull answers with a full snapshot reset
// (leading rowsPatch op:'clear'). the FIRST pull carries the desired-query got
// ack (delayed so it rides the first poke); every later poll is a clear-bearing
// poke with NO got patch — the exact query-diff --against rust-cf shape where a
// desired-query ack is a one-time delta and a following snapshot reset wipes it.
function snapshotResetFetch(): typeof fetch {
  let pulls = 0
  return (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(
      typeof input === 'string' || input instanceof URL ? input : input.url
    )
    if (!url.pathname.endsWith('/pull')) return jsonResponse({})
    const n = ++pulls
    const body = init?.body
      ? (JSON.parse(String(init.body)) as {
          queries?: { version: number; patch: Array<{ op: string; hash: string }> }
        })
      : {}
    if (n === 1) await new Promise((r) => setTimeout(r, 150))
    // ack the desired queries exactly once, on the pull that carries them
    const ackHashes =
      body.queries?.patch.filter((op) => op.op === 'put').map((op) => op.hash) ?? []
    return jsonResponse({
      cookie: n,
      lastMutationIDChanges: {},
      rowsPatch: [
        { op: 'clear' },
        { op: 'put', tableName: 'user', value: { id: 'u1', name: 'ada' } },
        {
          op: 'put',
          tableName: 'project',
          value: { id: 'p1', ownerId: 'u1', name: 'control' },
        },
      ],
      ...(ackHashes.length > 0
        ? {
            gotQueries: {
              version: body.queries!.version,
              patch: ackHashes.map((hash) => ({ op: 'put', hash })),
            },
          }
        : {}),
    })
  }) as typeof fetch
}

test('got-query completion survives a snapshot-reset poke after the ack', async () => {
  const transport = installHttpPullTransport({
    origin: ORIGIN,
    fetch: snapshotResetFetch(),
    pullIntervalMs: 50,
    queryForward: true,
  })
  const zero = new Zero({
    server: ORIGIN,
    userID: 'u1',
    auth: 'token-u1',
    schema,
    mutators,
    kvStore: 'mem',
  })

  try {
    const view = zero.materialize(
      (queries as unknown as Record<string, () => unknown>).allProjects!() as never
    )
    let complete = false
    view.addListener((_data: unknown, resultType: string) => {
      if (resultType === 'complete') complete = true
    })
    // give the delayed first pull, its ack poke, and several follow-up
    // snapshot-reset polls time to land, then require the view to have reached
    // (and, with the fix, stayed at) complete. without the re-assert the got
    // mark is wiped by the second clear-bearing poke and this never fires.
    const deadline = Date.now() + 4_000
    while (!complete && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 25))
    }
    // let a few more clear-bearing polls run; the mark must not regress
    await new Promise((r) => setTimeout(r, 300))
    view.destroy()
    expect(complete).toBe(true)
  } finally {
    await zero.close()
    transport.uninstall()
  }
})
