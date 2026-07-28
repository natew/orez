// Streaming fields: an ephemeral, typed field overlay beside Zero.
//
// The ordinary Zero row stays the durable truth. During a stream orez
// broadcasts field values without writing the application row or advancing the
// Zero cookie, and the application commits the final value through its existing
// write path. See plans/streaming-fields.md for the design and its rejected
// alternatives.

export { defineStreamingFields, resolveTopic } from './manifest.js'
export type {
  FieldMode,
  StreamingFieldDeclaration,
  StreamingFieldOptions,
  StreamingFieldHandle,
  StreamingFieldRef,
  StreamingFieldSpec,
  StreamingManifest,
  ZeroSchemaShape,
} from './manifest.js'

export {
  canonicalPrimaryKey,
  canonicalTopic,
  decodeFrame,
  encodeFrame,
  isLegacyWake,
  LEGACY_WAKE_FRAME,
  TOPIC_SEPARATOR,
} from './protocol.js'
export type {
  AnyFrame,
  ClientFrame,
  FieldUpdate,
  HostFrame,
  ProducerFrame,
  ProducerHostFrame,
  RealtimeKeyValue,
  RealtimeTopic,
} from './protocol.js'

export { canonicalEncode, RealtimeStore } from './store.js'
export type {
  RealtimeStoreOptions,
  StreamingFieldState,
  StreamingPhase,
} from './store.js'

export { RealtimePublisher } from './publisher.js'
export type {
  BeginOptions,
  PublisherOptions,
  PublisherTransport,
  StreamSession,
} from './publisher.js'

export { DEFAULT_HUB_LIMITS, RealtimeHub } from './hub.js'
export type {
  HubConnection,
  HubLimits,
  HubOptions,
  HubProducer,
  RealtimeIdentity,
  SubscribeAuthorization,
} from './hub.js'

export { FieldWriter } from './writer.js'
export type { FieldWriterOptions } from './writer.js'

export { createLocalRealtime } from './local.js'
export type { LocalRealtime, LocalRealtimeOptions } from './local.js'

export { BrowserRealtime, connectRealtimePort } from './message-port.js'
export type { BrowserRealtimeOptions, MembershipReader } from './message-port.js'

// Frame routing. One applier per direction; every surface is built from these.
export { applyClientFrame, applyHostFrame, applyProducerFrame } from './host.js'

// The producer role, identical on every surface
export { createProducer, inProcessTransport } from './producer.js'
export type { ProducerOptions, RealtimeProducer } from './producer.js'

// The host role over sockets, for any runtime that has them
export { createSocketHost } from './socket-host.js'
export type { HostConnection, HostSocket, RealtimeSocketHost } from './socket-host.js'

// A producer that reaches the hub over a socket: an application server
// generating values while the subscribers are browsers elsewhere
export { createSocketProducer } from './producer-socket.js'
export type { ProducerSocket, SocketProducer } from './producer-socket.js'

// The React binding lives in on-zero, which already owns the React peer
// dependency. orez-lite stays framework-free: see on-zero's useStreamingField.
