// MessagePort realtime adapter: the browser-worker host's transport.
//
// The hub state machine, publisher, and protocol are shared with every other
// host (orez-lite/realtime). This file is only the browser's socket lifecycle:
// a MessagePort instead of a WebSocket, and an authorizer that reads the same
// durable query membership the pull path maintains.
//
// The worker and its producer share one process lifecycle, so subscriptions and
// stream generations are memory-only here. Nothing survives a worker restart,
// which is correct: the durable value is whatever the client's next Zero pull
// produces.

import { applyClientFrame, applyHostFrame } from './host.js'
import { RealtimeHub } from './hub.js'
import { createProducer, inProcessTransport } from './producer.js'
import { decodeFrame, encodeFrame } from './protocol.js'
import { RealtimeStore } from './store.js'

import type {
  HubConnection,
  HubProducer,
  RealtimeIdentity,
  SubscribeAuthorization,
} from './hub.js'
import type { StreamingManifest } from './manifest.js'
import type { RealtimeProducer } from './producer.js'
import type { HostFrame, RealtimeTopic } from './protocol.js'
import type { RealtimePublisher } from './publisher.js'
import type { FieldWriter } from './writer.js'

// Reads the row-membership fact from the engine. Supplied by the host so this
// module never touches the database or the wasm boundary directly.
export type MembershipReader = (
  identity: RealtimeIdentity,
  table: string,
  key: Readonly<Record<string, boolean | null | number | string>>
) => Promise<{ readonly ownsGroup: boolean; readonly authorized: boolean }>

export type BrowserRealtimeOptions = {
  readonly manifest: StreamingManifest
  readonly readMembership: MembershipReader
  readonly onError?: (message: string) => void
}

export class BrowserRealtime {
  readonly #hub: RealtimeHub
  readonly #producer: RealtimeProducer
  #producerCounter = 0
  #connectionCounter = 0

  constructor(options: BrowserRealtimeOptions) {
    this.#hub = new RealtimeHub({
      manifest: options.manifest,
      authorizeSubscribe: async (identity, topic): Promise<SubscribeAuthorization> => {
        const { ownsGroup, authorized } = await options.readMembership(
          identity,
          topic.table,
          topic.key
        )
        // A client group id is not a bearer token: not owning it is a denial,
        // and the reason deliberately does not say whether the row exists.
        if (!ownsGroup) {
          return { status: 'denied', reason: 'client group does not belong to this user' }
        }
        // Owning the group without membership is the optimistic-row race: the
        // client holds a row from its own mutation that the server has not
        // recorded yet. It retries after its next pull.
        return authorized ? { status: 'active' } : { status: 'pending' }
      },
    })

    // There is no producer socket in the browser: the code running the model
    // loop and the hub share one JavaScript context, so begin/publish are
    // direct calls. The producer ROLE is still the shared one, so a loop
    // written here runs unchanged against a socket host.
    const producerHandle: HubProducer = {
      id: `browser-producer-${++this.#producerCounter}`,
      send: () => {},
    }
    this.#producer = createProducer(
      inProcessTransport(this.#hub, producerHandle),
      options
    )
  }

  // Imperative writing: set a row's field to its current value, generations
  // managed for you. Trusted by construction, this is the worker's own code.
  get fields(): FieldWriter {
    return this.#producer.fields
  }

  // The explicit generation API underneath, for a producer that controls begin,
  // commit, and end itself.
  get publisher(): RealtimePublisher {
    return this.#producer.publisher
  }

  // Attach a port. The identity comes from the host's authenticated pull path,
  // never from the port itself, so a page cannot claim another user's client
  // group by asserting one over the channel.
  connect(port: MessagePort, identity: RealtimeIdentity): () => void {
    const connection: HubConnection = {
      id: `browser-connection-${++this.#connectionCounter}`,
      identity,
      send: (frame) => {
        port.postMessage({ event: 'realtime', frame: encodeFrame(frame as never) })
      },
    }

    const onMessage = (event: MessageEvent<unknown>) => {
      const message = event.data
      if (
        !message ||
        typeof message !== 'object' ||
        (message as { event?: unknown }).event !== 'realtime'
      ) {
        return
      }
      const raw = (message as { frame?: unknown }).frame
      if (typeof raw !== 'string') return
      const frame = decodeFrame(raw)
      if (!frame) return
      // A subscriber channel is never a publish channel. Only subscribe and
      // unsubscribe are accepted here, whatever else a port sends.
      if (frame[0] !== 'subscribe' && frame[0] !== 'unsubscribe') return
      void applyClientFrame(this.#hub, connection, frame)
    }

    port.addEventListener('message', onMessage)
    return () => {
      port.removeEventListener('message', onMessage)
      this.#hub.dropConnection(connection.id)
    }
  }

  // A pull removed rows from a client group's membership, so its field
  // subscriptions for those rows go with them.
  revokeMembership(clientGroupID: string, removed: readonly RealtimeTopic[]): void {
    this.#hub.revokeMembership(clientGroupID, removed)
  }

  flush(): void {
    this.#hub.flush()
  }

  get hub(): RealtimeHub {
    return this.#hub
  }
}

// The page-side half of the port transport. Owns a RealtimeStore and pumps
// frames both ways, so `useStreamingField` sees the same store it would against
// a WebSocket host.
export function connectRealtimePort(
  port: MessagePort,
  options: {
    readonly staleAfterMs?: number
    readonly onError?: (message: string) => void
  } = {}
): { readonly store: RealtimeStore; readonly close: () => void } {
  const store = new RealtimeStore({
    send: (frame) => {
      port.postMessage({ event: 'realtime', frame: encodeFrame(frame as never) })
    },
    staleAfterMs: options.staleAfterMs,
    onError: options.onError,
  })

  const onMessage = (event: MessageEvent<unknown>) => {
    const message = event.data
    if (
      !message ||
      typeof message !== 'object' ||
      (message as { event?: unknown }).event !== 'realtime'
    ) {
      return
    }
    const raw = (message as { frame?: unknown }).frame
    if (typeof raw !== 'string') return
    const frame = decodeFrame(raw)
    if (frame) applyHostFrame(store, frame as HostFrame)
  }

  port.addEventListener('message', onMessage)
  port.start()
  return {
    store,
    close: () => {
      port.removeEventListener('message', onMessage)
      store.handleDisconnect()
    },
  }
}
