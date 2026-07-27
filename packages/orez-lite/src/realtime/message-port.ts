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

import { RealtimeHub } from './hub.js'
import { decodeFrame, encodeFrame } from './protocol.js'
import { RealtimePublisher } from './publisher.js'
import { RealtimeStore } from './store.js'

import type {
  HubConnection,
  HubProducer,
  RealtimeIdentity,
  SubscribeAuthorization,
} from './hub.js'
import type { StreamingManifest } from './manifest.js'
import type { FieldUpdate, RealtimeTopic } from './protocol.js'
import type { PublisherTransport } from './publisher.js'

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
}

export class BrowserRealtime {
  readonly #hub: RealtimeHub
  readonly #publisher: RealtimePublisher
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

    // In-process producer transport. There is no producer socket in the
    // browser: the application code that runs the model loop and the hub share
    // one JavaScript context, so `begin`/`publish` are direct calls.
    const producer: HubProducer = {
      id: `browser-producer-${++this.#producerCounter}`,
      send: () => {},
    }
    const transport: PublisherTransport = {
      begin: (topic: RealtimeTopic, streamID: string) => {
        const result = this.#hub.beginGeneration(producer, topic, streamID)
        if (!result.ok) throw new Error(result.reason)
      },
      publish: (update: FieldUpdate) => {
        const result = this.#hub.publish(producer, update)
        if (!result.ok) throw new Error(result.reason)
      },
      end: () => {},
    }
    this.#publisher = new RealtimePublisher(transport, options.manifest)
  }

  // The application's handle for producing values. Trusted by construction:
  // this is the worker's own code, not a remote publisher.
  get publisher(): RealtimePublisher {
    return this.#publisher
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
      const [kind, body] = frame
      // A subscriber channel is never a publish channel. Only subscribe and
      // unsubscribe are accepted here, whatever else a port sends.
      if (kind === 'subscribe') {
        void this.#hub.subscribe(connection, (body as { topic: RealtimeTopic }).topic)
      } else if (kind === 'unsubscribe') {
        void this.#hub.unsubscribe(connection, (body as { topic: RealtimeTopic }).topic)
      }
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
    if (!frame) return
    const [kind, body] = frame
    if (kind === 'field') {
      store.applyUpdates((body as { updates: readonly FieldUpdate[] }).updates)
    } else if (kind === 'subscribed') {
      const { topic, status } = body as { topic: string; status: 'active' | 'pending' }
      store.handleSubscribed(topic, status)
    } else if (kind === 'subscribe-error') {
      const { topic, reason } = body as { topic: string; reason: string }
      store.handleSubscribeError(topic, reason)
    }
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
