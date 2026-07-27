// Frame routing. Every realtime surface is built from these three functions.
//
// A stream has exactly three directions, and there is one applier for each:
//
//   applyClientFrame    subscriber -> hub    subscribe, unsubscribe
//   applyProducerFrame  producer   -> hub    begin, publish
//   applyHostFrame      hub        -> store  subscribed, field, subscribe-error
//
// Everything else in a surface is channel plumbing: a MessagePort, a
// WebSocket, or a direct call. Keeping the routing here is what lets a new
// transport be written without re-deriving which frame maps to which call, and
// it is why adding one cannot quietly disagree with the others.
//
// Subscribers and producers are deliberately separate roles with separate
// frames. A subscriber socket can never publish, so a leaked streamID buys an
// attacker nothing: the hub checks the producer HANDLE, and a subscriber does
// not have one.

import type { RealtimeHub, HubConnection, HubProducer } from './hub.js'
import type { ClientFrame, FieldUpdate, HostFrame, ProducerFrame } from './protocol.js'
import type { RealtimeStore } from './store.js'

// Apply one subscriber frame. The returned promise resolves when the frame has
// been applied, which an adapter awaits before processing the next frame from
// the same socket so a subscribe/unsubscribe pair cannot land out of order.
export function applyClientFrame(
  hub: RealtimeHub,
  connection: HubConnection,
  frame: ClientFrame
): Promise<void> {
  return frame[0] === 'subscribe'
    ? hub.subscribe(connection, frame[1].topic)
    : hub.unsubscribe(connection, frame[1].topic)
}

// Apply one producer frame.
//
// `begin` is answered either way: the producer cannot start streaming until it
// knows the host accepted the generation. Updates are answered only when
// rejected, so a token stream costs one frame per token rather than a round
// trip per token.
export function applyProducerFrame(
  hub: RealtimeHub,
  producer: HubProducer,
  frame: ProducerFrame
): void {
  if (frame[0] === 'begin') {
    const { topic, streamID } = frame[1]
    const result = hub.beginGeneration(producer, topic, streamID)
    producer.send([
      'begin-result',
      {
        streamID,
        topic: result.ok ? result.topicID : null,
        reason: result.ok ? null : result.reason,
      },
    ])
    return
  }

  const { update } = frame[1]
  const result = hub.publish(producer, update)
  if (!result.ok) {
    producer.send([
      'publish-rejected',
      { topic: update.topic, streamID: update.streamID, reason: result.reason },
    ])
  }
}

// Apply one frame the host sent back to a subscriber. Every client transport
// ends in this call, so a MessagePort client and a WebSocket client cannot drift
// apart in how they interpret the same frame.
//
// A `wake` frame is deliberately ignored here: it belongs to the Zero pull path,
// not to the field overlay, and a client that shares one socket for both routes
// it before reaching this.
export function applyHostFrame(store: RealtimeStore, frame: HostFrame): void {
  switch (frame[0]) {
    case 'field':
      store.applyUpdates(frame[1].updates as readonly FieldUpdate[])
      return
    case 'subscribed':
      store.handleSubscribed(frame[1].topic, frame[1].status)
      return
    case 'subscribe-error':
      store.handleSubscribeError(frame[1].topic, frame[1].reason)
      return
    case 'wake':
      return
  }
}
