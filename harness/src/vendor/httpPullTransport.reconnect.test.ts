// red-proof for the CI pull-fault-recovery stall (seed 38129815894).
//
// after several consecutive transient failures (an injected pull 500, an
// engine kill, then ECONNREFUSEDs across the restart window) the transport
// delayed each new socket's open by an exponential backoff (up to 30s). the
// open delivers the `connected` frame Zero's connect attempt waits for as
// "the server acknowledgement", and Zero abandons that attempt after 10s and
// closes the socket — which clears the pending open timer, so no request ever
// succeeds to reset the backoff. every retry reads the same elevated count
// and dies the same way: a permanent stall ("client has not recovered from
// injected pull fault").
//
// this pins the invariant directly: prior transient failures must never delay
// a new socket's open past Zero's connect deadline.
import { expect, test } from 'bun:test'

import { installHttpPullTransport } from './httpPullTransport.js'

const ORIGIN = 'https://orez-http-reconnect.local/ns'

type TestSocket = {
  addEventListener(type: string, listener: () => void): void
  close(): void
}

function failingFetch(): typeof fetch {
  return (async () => new Response('boom', { status: 500 })) as typeof fetch
}

function connectURL(n: number) {
  return `${ORIGIN}/sync/v51/connect?clientID=reconnect-client&clientGroupID=reconnect-group&wsid=reconnect-${n}`
}

function waitFor(
  ws: TestSocket,
  type: string,
  timeoutMs: number,
  label: string
): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`timed out waiting for socket ${label}`)),
      timeoutMs
    )
    ws.addEventListener(type, () => {
      clearTimeout(timer)
      resolve()
    })
  })
}

test('reconnecting sockets open promptly after consecutive transient failures', async () => {
  const transport = installHttpPullTransport({
    origin: ORIGIN,
    fetch: failingFetch(),
  })
  try {
    // stack three consecutive transient failures: each socket opens and its
    // opening pull fails with a 500, closing the socket.
    for (let n = 0; n < 3; n++) {
      const ws = new (globalThis.WebSocket as unknown as new (url: string) => TestSocket)(
        connectURL(n)
      )
      await waitFor(ws, 'open', 15_000, `open ${n}`)
      await waitFor(ws, 'close', 15_000, `close ${n}`)
    }
    // the next socket must still open promptly: the open carries the
    // connection acknowledgement, and any failure-count delay past Zero's 10s
    // connect deadline turns one outage into a self-sustaining stall.
    const start = Date.now()
    const ws = new (globalThis.WebSocket as unknown as new (url: string) => TestSocket)(
      connectURL(3)
    )
    await waitFor(ws, 'open', 15_000, 'open after failures')
    expect(Date.now() - start).toBeLessThan(1_000)
    ws.close()
  } finally {
    transport.uninstall()
  }
}, 30_000)
