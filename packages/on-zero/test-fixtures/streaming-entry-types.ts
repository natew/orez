import {
  createUseStreamingField,
  createUseStreamingFields,
  type StreamingFieldHandle,
} from 'on-zero/streaming'

import type { RealtimeStore } from 'orez-lite/realtime'

declare const store: RealtimeStore
declare const handle: StreamingFieldHandle

const useStreamingField = createUseStreamingField(() => store)
const useStreamingFields = createUseStreamingFields(() => store)

useStreamingField(handle, 'durable')
useStreamingFields([{ key: 'message', handle, base: 'durable' }])
