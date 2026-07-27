// Frame routing between a socket and the hub.
//
// The hub is a pure state machine with no I/O, and these two functions are the
// whole translation between it and the wire. A host adapter is then only socket
// lifecycle and identity: the Cloudflare Durable Object, a Node WebSocket
// server, and the test harness all share the routing here rather than each
// re-deriving which frame maps to which hub call.
//
// Subscribers and producers are deliberately separate roles with separate
// frames. A subscriber socket can never publish, so a leaked streamID buys an
// attacker nothing: the hub checks the producer HANDLE, and a subscriber does
// not have one.

import type { RealtimeHub, HubConnection, HubProducer } from './hub.js'
import type { ClientFrame, ProducerFrame } from './protocol.js'

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
