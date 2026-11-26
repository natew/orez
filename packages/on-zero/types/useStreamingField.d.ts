import type { RealtimeStore, StreamingFieldHandle, StreamingFieldState } from 'orez-lite/realtime';
export type { StreamingFieldHandle };
export type UseStreamingField = <Value>(handle: StreamingFieldHandle | null | undefined, base: Value) => StreamingFieldState<Value>;
export type StreamingFieldRequest<Value = unknown> = {
    readonly key: string;
    readonly handle: StreamingFieldHandle;
    readonly base: Value;
};
export type UseStreamingFields = <Value>(requests: readonly StreamingFieldRequest<Value>[]) => Readonly<Record<string, StreamingFieldState<Value>>>;
export declare function createUseStreamingField(getStore: () => RealtimeStore | undefined): UseStreamingField;
export declare function createUseStreamingFields(getStore: () => RealtimeStore | undefined): UseStreamingFields;
//# sourceMappingURL=useStreamingField.d.ts.map