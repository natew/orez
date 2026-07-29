// Streaming fields against a real sync-cf-host Durable Object.
//
// Everything below the socket is already covered by unit tests. What only a
// real DO can answer is whether the three things that meet there actually agree:
// the wake socket carries an authenticated identity, the engine's own durable
// membership authorizes a subscription, and a producer in a different isolate
// reaches a subscriber in this one.
//
// That last part is the whole reason this exists. A process-global rendezvous
// works on a single node and cannot work on Cloudflare, where the producer (an
// AI generation worker) and the consumer (a watching browser) are never in the
// same isolate.
//
//   bun src/streaming-fields.ts --worker http://127.0.0.1:8998 --admin-key <key>

import assert from 'node:assert/strict'

import { createSocketProducer, encodeFrame } from 'orez-sync-executor/realtime'

import { harnessStreaming } from '../../packages/sync-cf-host/src/harness-schema.js'
import { mintHarnessWakeToken } from '../../packages/sync-cf-host/src/harness-wake-token.js'
import { queries } from './fixture.js'

import type { RealtimeTopic } from 'orez-sync-executor/realtime'

const args = process.argv.slice(2)
const flag = (name: string): string | undefined => {
  const index = args.indexOf(`--${name}`)
  return index >= 0 ? args[index + 1] : undefined
}
const worker = flag('worker') ?? process.env.ZHARNESS_RUST_CF_WORKER
const adminKey = flag('admin-key') ?? process.env.ZHARNESS_CF_ADMIN_KEY
if (worker) process.env.ZHARNESS_RUST_CF_WORKER = worker
if (adminKey) process.env.ZHARNESS_CF_ADMIN_KEY = adminKey
assert(adminKey, 'an admin key is required (--admin-key or ZHARNESS_CF_ADMIN_KEY)')

// Imported after the flags are in the environment, because the target reads its
// worker URL at module scope. A static import here silently aimed the whole
// lane at the deployed production worker.
const { startRustCf } = await import('./targets/rust-cf.js')

// A socket that collects decoded frames and can be waited on for one.
function collector(url: string) {
  const socket = new WebSocket(url)
  const frames: [string, Record<string, unknown>][] = []
  const waiters: { match: (kind: string) => boolean; resolve: () => void }[] = []
  socket.addEventListener('message', (event) => {
    const raw = typeof event.data === 'string' ? event.data : ''
    if (!raw || raw === 'pong') return
    const frame = JSON.parse(raw) as [string, Record<string, unknown>]
    frames.push(frame)
    for (const waiter of waiters.splice(0)) {
      if (waiter.match(frame[0])) waiter.resolve()
      else waiters.push(waiter)
    }
  })
  return {
    socket,
    frames,
    open: () =>
      new Promise<void>((resolve, reject) => {
        if (socket.readyState === socket.OPEN) return resolve()
        const timer = setTimeout(
          () => reject(new Error(`open timed out: ${url}`)),
          10_000
        )
        socket.addEventListener('open', () => {
          clearTimeout(timer)
          resolve()
        })
        socket.addEventListener('error', () => {
          clearTimeout(timer)
          reject(new Error(`socket failed: ${url}`))
        })
      }),
    // Waiting for a KIND rather than a fixed delay: a sleep long enough to be
    // reliable is long enough to hide an ordering bug.
    waitFor: (kind: string, ms = 5_000) =>
      new Promise<void>((resolve, reject) => {
        if (frames.some(([seen]) => seen === kind)) return resolve()
        const timer = setTimeout(
          () => reject(new Error(`timed out waiting for a '${kind}' frame`)),
          ms
        )
        waiters.push({
          match: (seen) => seen === kind,
          resolve: () => {
            clearTimeout(timer)
            resolve()
          },
        })
      }),
    updates: () =>
      frames
        .filter(([kind]) => kind === 'field')
        .flatMap(
          ([, body]) =>
            (body.updates ?? []) as {
              value?: unknown
              text?: unknown
              op?: string
            }[]
        ),
    close: () => socket.close(),
  }
}

const target = await startRustCf()
const namespace = new URL(target.origin).pathname.slice(1)
const wsOrigin = target.origin.replace(/^http/, 'ws')
const sockets: { close(): void }[] = []

try {
  // A real client, so the subscription is authorized against the same durable
  // membership its query pull produced. Nothing here is faked into the engine.
  const zero = target.createClient('user-1')
  const view = zero.materialize(queries.tasksDone())
  let rows: { id: string }[] = []
  view.addListener((data) => {
    rows = JSON.parse(JSON.stringify(data)) as { id: string }[]
  })
  const started = Date.now()
  while (rows.length === 0 && Date.now() - started < 20_000) {
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  assert(rows.length > 0, 'client synced no task rows, so nothing can be subscribed')
  const taskID = String(rows[0].id)

  const groups = await target.sql(
    `SELECT clientGroupID, userID FROM _zsync_clients WHERE userID = 'user-1'`
  )
  const clientGroupID = String(groups[0]?.clientGroupID ?? '')
  assert(clientGroupID, 'no client group recorded for user-1')

  const topic: RealtimeTopic = { table: 'task', key: { id: taskID }, field: 'title' }
  const subscribeFrame = encodeFrame(['subscribe', { topic }])

  const openSubscriber = async (userID: string, clientID: string, group: string) => {
    const { token } = await mintHarnessWakeToken(namespace, userID, adminKey)
    const url =
      `${wsOrigin}/wake?clientID=${encodeURIComponent(clientID)}` +
      `&clientGroupID=${encodeURIComponent(group)}&wakeToken=${encodeURIComponent(token)}`
    const client = collector(url)
    sockets.push(client)
    await client.open()
    return client
  }

  const subscriber = await openSubscriber('user-1', 'stream-client-1', clientGroupID)
  subscriber.socket.send(subscribeFrame)
  await subscriber.waitFor('subscribed')

  // The negative control, and it has to be a control: a second user asserting
  // the FIRST user's client group. If the group id alone were enough, this
  // would receive every token below.
  const intruder = await openSubscriber('user-2', 'stream-client-2', clientGroupID)
  intruder.socket.send(subscribeFrame)
  await intruder.waitFor('subscribe-error')

  // The producer, in a different isolate from every subscriber above.
  const producer = collector(
    `${wsOrigin}/realtime/produce?producerID=probe&adminKey=${encodeURIComponent(adminKey)}`
  )
  sockets.push(producer)
  await producer.open()

  // Driven through the SHIPPED producer client, not hand-rolled frames. That
  // client waits for the host's begin ack before it will publish, so a host
  // that never acked would hang here rather than quietly working in a test that
  // encoded its own frames.
  const errors: string[] = []
  const writer = createSocketProducer(producer.socket, {
    manifest: harnessStreaming.manifest,
    onError: (message) => errors.push(message),
  })
  producer.socket.addEventListener('message', (event) => {
    if (typeof event.data === 'string') writer.handleMessage(event.data)
  })

  const field = harnessStreaming.task.title({ id: taskID })
  const tokens = ['Streaming ', 'across ', 'isolates']
  let text = ''
  for (const token of tokens) {
    text += token
    writer.fields.set(field, text)
  }
  await writer.fields.flush(field)
  assert.deepEqual(errors, [], 'the producer client reported errors')

  await subscriber.waitFor('field')
  // the batching window coalesces, so give the tail a moment to arrive
  await new Promise((resolve) => setTimeout(resolve, 500))

  const received = subscriber
    .updates()
    .reduce(
      (acc, update) =>
        update.op === 'snapshot' ? String(update.value) : acc + String(update.text),
      ''
    )
  assert.equal(received, text, `subscriber reconstructed ${JSON.stringify(received)}`)
  assert.equal(
    intruder.updates().length,
    0,
    'a user who does not own the client group received streamed values'
  )

  // A late subscriber gets the accumulated value as one snapshot, which is why
  // no client needs a replay buffer.
  const late = await openSubscriber('user-1', 'stream-client-3', clientGroupID)
  late.socket.send(subscribeFrame)
  await late.waitFor('field')
  const caughtUp = late
    .updates()
    .map((update) => String(update.value))
    .join('')
  assert.equal(caughtUp, text, `late subscriber caught up to ${JSON.stringify(caughtUp)}`)

  // Hibernation: the object holding the hub is evicted while every socket
  // above stays open. Nothing reconnects, and the next producer frame has to
  // still reach the subscriber that subscribed before the eviction.
  // The client polls every 500ms, and every poll is a request that resets the
  // idle clock. It has already done its job (the membership it produced is
  // durable), so it goes away before the object is left alone.
  view.destroy()
  await zero.close()

  const beforeEviction = await target.hibernationStatus()
  await new Promise((resolve) => setTimeout(resolve, 6_000))
  const afterEviction = await target.hibernationStatus()
  assert(
    afterEviction.hibernations > beforeEviction.hibernations,
    'the durable object never evicted, so rehydration was not exercised'
  )

  const beforeCount = subscriber.updates().length
  // A fresh generation on the same socket, through the same client. Its begin
  // has to be acked by a hub that did not exist when the socket was opened.
  await writer.fields.abort(field)
  writer.fields.set(field, 'survived the eviction')
  await writer.fields.flush(field)
  await new Promise((resolve) => setTimeout(resolve, 1_000))
  const latest = subscriber.updates().at(-1)
  assert(
    subscriber.updates().length > beforeCount,
    'the subscriber received nothing after the durable object was evicted'
  )
  assert.equal(String(latest?.value), 'survived the eviction')

  console.info('streaming fields: producer -> DO -> subscriber verified')
  console.info(`  value          ${JSON.stringify(text)}`)
  console.info(`  denied user    ${intruder.updates().length} updates`)
  console.info(`  late subscriber caught up in one snapshot`)
  console.info(
    `  survived ${afterEviction.hibernations - beforeEviction.hibernations} eviction(s) with the socket open`
  )
} finally {
  for (const socket of sockets) socket.close()
  await target.close()
}
