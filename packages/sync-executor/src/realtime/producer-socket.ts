// A PublisherTransport backed by a socket, for a producer that is not in the
// same process as the hub.
//
// This is the shape soot does NOT need and the shape agentbus and chat do: an
// application server generates the values, and the subscribers are browsers
// somewhere else, so the updates have to cross a network to reach the hub that
// fans them out.
//
// One socket carries every generation the producer has open. A server writing
// twenty agent sessions holds one connection, and the hub releases all twenty
// leases when it drops.
//
// Connecting, authenticating, and reconnecting are the caller's: it knows its
// own deployment. This owns only the frame bookkeeping on top of a socket that
// is already open.

import { createProducer } from './producer.js'
import { decodeFrame, encodeFrame } from './protocol.js'

import type { ProducerOptions, RealtimeProducer } from './producer.js'
import type { FieldUpdate, RealtimeTopic } from './protocol.js'
import type { PublisherTransport } from './publisher.js'

export type ProducerSocket = {
  send(data: string): void
}

export type SocketProducer = RealtimeProducer & {
  // feed every inbound socket message here
  handleMessage(raw: string): void
  // the socket closed or errored. Every generation waiting on a begin ack is
  // rejected and every open one is failed, so a producer writing into a dead
  // socket finds out on its next call instead of streaming into nothing.
  fail(reason: string): void
}

export function createSocketProducer(
  socket: ProducerSocket,
  options: ProducerOptions
): SocketProducer {
  const pendingBegins = new Map<
    string,
    { resolve: () => void; reject: (error: Error) => void }
  >()
  // streamID -> why it is unusable. Set by a host rejection or supersession, and
  // raised on the producer's next publish for that stream.
  const failures = new Map<string, string>()
  // The socket itself died. This is deliberately not per-stream: once the
  // connection is gone every generation on it is gone, including ones that
  // began successfully and are therefore absent from `failures`.
  let socketFailure: string | undefined

  const transport: PublisherTransport = {
    begin(topic: RealtimeTopic, streamID: string): Promise<void> {
      if (socketFailure) return Promise.reject(new Error(socketFailure))
      return new Promise<void>((resolve, reject) => {
        pendingBegins.set(streamID, { resolve, reject })
        socket.send(encodeFrame(['begin', { topic, streamID }]))
      })
    },

    publish(update: FieldUpdate): void {
      if (socketFailure) throw new Error(socketFailure)
      const failure = failures.get(update.streamID)
      if (failure) throw new Error(failure)
      socket.send(encodeFrame(['publish', { update }]))
    },

    end(streamID: string): void {
      failures.delete(streamID)
      pendingBegins.delete(streamID)
    },
  }

  return {
    ...createProducer(transport, options),

    handleMessage(raw: string): void {
      const frame = decodeFrame(raw)
      if (!frame) return
      switch (frame[0]) {
        case 'begin-result': {
          const { streamID, reason } = frame[1]
          const pending = pendingBegins.get(streamID)
          if (!pending) return
          pendingBegins.delete(streamID)
          if (reason === null) pending.resolve()
          else pending.reject(new Error(reason))
          return
        }
        case 'publish-rejected': {
          const { streamID, reason } = frame[1]
          failures.set(streamID, reason)
          return
        }
        case 'superseded': {
          const { topic, streamID } = frame[1]
          failures.set(
            streamID,
            `generation for '${topic}' was superseded by a newer one`
          )
          return
        }
        default:
          return
      }
    },

    fail(reason: string): void {
      socketFailure = reason
      for (const pending of pendingBegins.values()) pending.reject(new Error(reason))
      pendingBegins.clear()
    },
  }
}
