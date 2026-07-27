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
  ClientFrame,
  FieldUpdate,
  HostFrame,
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
export type { BeginOptions, PublisherTransport, StreamSession } from './publisher.js'

export { DEFAULT_HUB_LIMITS, RealtimeHub } from './hub.js'
export type {
  HubConnection,
  HubLimits,
  HubOptions,
  HubProducer,
  RealtimeIdentity,
  SubscribeAuthorization,
} from './hub.js'

export { createLocalRealtime } from './local.js'
export type { LocalRealtime, LocalRealtimeOptions } from './local.js'

export { BrowserRealtime, connectRealtimePort } from './message-port.js'
export type { BrowserRealtimeOptions, MembershipReader } from './message-port.js'

// The React binding lives in on-zero, which already owns the React peer
// dependency. orez-lite stays framework-free: see on-zero's useStreamingField.
