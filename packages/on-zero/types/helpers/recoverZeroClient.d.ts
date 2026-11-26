import type { ZeroEventsEmitter, ZeroRecoveryReasonKey } from '../types';
import type { LogLevel, LogSink } from '@rocicorp/logger';
import type { UpdateNeededReason } from '@rocicorp/zero';
export type ZeroRecoveryLogClassification = {
    reasonKey: Exclude<ZeroRecoveryReasonKey, 'NewClientGroup' | 'VersionNotSupported' | 'SchemaVersionNotSupported' | 'client-state-not-found'>;
    message: string;
    dropLocalState: boolean;
};
export type ZeroLogPattern = string | RegExp;
export type RecoveryGuardStorage = {
    getItem: (key: string) => string | null;
    setItem: (key: string, value: string) => void;
};
export type ScheduleReloadContext = {
    reason: string;
    reasonKey: ZeroRecoveryReasonKey;
    dropLocalState: boolean;
    performReload: () => Promise<void>;
};
export type ZeroRecoveryDeps = {
    deleteLocalState: () => Promise<unknown>;
    zeroEvents: ZeroEventsEmitter;
    beforeReload?: () => Promise<void>;
    scheduleReload?: (ctx: ScheduleReloadContext) => void;
    guardStorage?: RecoveryGuardStorage;
    benignLogPatterns?: readonly ZeroLogPattern[];
    onRecovery?: (reasonKey: ZeroRecoveryReasonKey) => void;
    recoverInPlace?: () => Promise<boolean>;
    reload?: () => void | boolean;
};
export declare function makeZeroRecovery(deps: ZeroRecoveryDeps): {
    onUpdateNeeded(reason: UpdateNeededReason): void;
    onClientStateNotFound(): void;
};
export declare function classifyZeroRecoveryLog(level: LogLevel | string, args: readonly unknown[], nowMs?: number): ZeroRecoveryLogClassification | undefined;
export declare function composeRecoveryLogSink(deps: ZeroRecoveryDeps, consumerLogSink?: LogSink): LogSink;
export declare function isRecoverableZeroStalePokeMessage(message: string): boolean;
export declare function resetRecoveryStateForTests(): void;
//# sourceMappingURL=recoverZeroClient.d.ts.map