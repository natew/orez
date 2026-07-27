// The host role over sockets, for any runtime that has them.
//
// A Cloudflare Durable Object, a Node WebSocket server, and a test harness
// differ in how they accept a socket and in nothing else. So this owns the
// parts that are the same everywhere (which frames a role may send, when a
// connection is dropped, what survives a cold start) and leaves each runtime
// only its own socket lifecycle.
//
// The role split is enforced here rather than trusted. A subscriber socket is
// accepted as a subscriber and can never publish, whatever it sends: it has no
// producer handle, and the hub checks the handle rather than the frame. That is
// why a leaked streamID grants nothing.

import { applyClientFrame, applyProducerFrame } from './host.js'
import { RealtimeHub } from './hub.js'
import { decodeFrame, encodeFrame } from './protocol.js'

import type { HubConnection, HubProducer, HubOptions, RealtimeIdentity } from './hub.js'
import type { RealtimeTopic } from './protocol.js'

export type HostSocket = {
  send(data: string): void
}

// One attached socket. `topics()` is exactly what a runtime persists if its
// sockets can outlive the process holding the hub, and exactly what it hands
// back to `rehydrate`. The two are the same type on purpose: a runtime that
// stores one and replays the other is the shape of a bug nobody notices until a
// hibernation, which is the hardest case to reproduce.
export type HostConnection = {
  handleMessage(raw: string): void
  close(): void
  topics(): readonly RealtimeTopic[]
}

export type RealtimeSocketHost = {
  readonly hub: RealtimeHub
  // Accept a subscriber. Identity must come from the runtime's authenticated
  // path: a connection asserting its own userID is a connection claiming any
  // user's data.
  acceptSubscriber(
    socket: HostSocket,
    identity: RealtimeIdentity,
    connectionID: string
  ): HostConnection
  // Accept a producer. Trusted by the runtime's own authentication, exactly as
  // a subscriber is; a producer that should not hold a topic is refused by the
  // hub when it tries to begin one.
  acceptProducer(socket: HostSocket, producerID: string): HostConnection
  // Re-establish subscriptions for sockets that outlived the hub.
  //
  // A hibernatable socket is the case this exists for: the runtime evicts the
  // object holding the hub while the socket stays open, so on the next message
  // the hub is empty and a client that subscribed before eviction would receive
  // nothing forever. Replaying `subscribe` is the right repair rather than
  // restoring internal state, because it re-authorizes as a side effect: a
  // membership revoked during the gap is honoured instead of resurrected.
  //
  // Generations deliberately do NOT survive. They are ephemeral by definition,
  // and the durable row is still the truth, so a producer whose generation
  // vanished re-opens one and the client converges either way.
  //
  // Returns one connection per entry, in order, so a runtime that keeps sockets
  // in a map can put them straight back. Without that a rehydrated socket would
  // have no handle: its later frames would go nowhere and its topics would be
  // lost at the NEXT eviction, which looks like the first one half-worked.
  rehydrate(
    entries: readonly {
      socket: HostSocket
      identity: RealtimeIdentity
      connectionID: string
      topics: readonly RealtimeTopic[]
    }[]
  ): Promise<readonly HostConnection[]>
  flush(): void
}

export function createSocketHost(options: HubOptions): RealtimeSocketHost {
  const hub = new RealtimeHub(options)

  // One subscriber, whether it is being accepted for the first time or rebuilt
  // after an eviction. Both paths need the same tracking, so neither gets its
  // own copy of it.
  const subscriber = (
    socket: HostSocket,
    identity: RealtimeIdentity,
    connectionID: string
  ): {
    connection: HubConnection
    owned: Map<string, RealtimeTopic>
    host: HostConnection
  } => {
    // keyed for dedup, valued with the structured topic so `topics()` can hand
    // back something `rehydrate` accepts without a second parser
    const owned = new Map<string, RealtimeTopic>()
    const connection: HubConnection = {
      id: connectionID,
      identity,
      send: (frame) => {
        socket.send(encodeFrame(frame as never))
      },
    }
    const host: HostConnection = {
      handleMessage(raw: string): void {
        const frame = decodeFrame(raw)
        if (!frame) return
        // A subscriber channel is never a publish channel, whatever arrives on
        // it. Producer frames are not merely ignored by the hub here; they are
        // refused before reaching it.
        if (frame[0] !== 'subscribe' && frame[0] !== 'unsubscribe') return
        if (frame[0] === 'subscribe') owned.set(topicKey(frame[1].topic), frame[1].topic)
        else owned.delete(topicKey(frame[1].topic))
        void applyClientFrame(hub, connection, frame)
      },
      close(): void {
        hub.dropConnection(connectionID)
      },
      topics: () => [...owned.values()],
    }
    return { connection, owned, host }
  }

  return {
    hub,

    acceptSubscriber: (socket, identity, connectionID) =>
      subscriber(socket, identity, connectionID).host,

    acceptProducer(socket, producerID) {
      const producer: HubProducer = {
        id: producerID,
        send: (frame) => {
          socket.send(encodeFrame(frame as never))
        },
      }
      return {
        handleMessage(raw: string): void {
          const frame = decodeFrame(raw)
          if (!frame) return
          if (frame[0] !== 'begin' && frame[0] !== 'publish') return
          applyProducerFrame(hub, producer, frame)
        },
        close(): void {
          hub.dropProducer(producerID)
        },
        topics: () => [],
      }
    },

    async rehydrate(entries): Promise<readonly HostConnection[]> {
      const restored: HostConnection[] = []
      for (const entry of entries) {
        const { connection, owned, host } = subscriber(
          entry.socket,
          entry.identity,
          entry.connectionID
        )
        for (const topic of entry.topics) {
          // recorded before the result, exactly as a live subscribe frame does:
          // a topic denied during the gap stays the client's asserted interest,
          // so a membership that comes back is picked up at the next eviction
          // instead of being forgotten here.
          owned.set(topicKey(topic), topic)
          await hub.subscribe(connection, topic)
        }
        restored.push(host)
      }
      return restored
    },

    flush: () => hub.flush(),
  }
}

// Dedup key only. Never persisted and never parsed back: `topics()` hands out
// the structured topic, so there is no second encoding to keep in agreement
// with canonicalTopic.
function topicKey(topic: RealtimeTopic): string {
  return JSON.stringify(topic)
}
