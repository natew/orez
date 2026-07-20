import type { Emitter } from './emitter';
import type { ZeroEvent } from '../types';
import type { UpdateNeededReason } from '@rocicorp/zero';
import type { LogLevel, LogSink } from '@rocicorp/logger';
export type ZeroRecoveryLogReason = 'indexeddb-not-found' | 'sqlite-statement-finalized' | 'store-closed-repeat' | 'mutation-desync' | 'connection-cookie-invalid' | 'client-not-found' | 'connection-userid-mismatch';
export type ZeroRecoveryLogClassification = {
    reason: ZeroRecoveryLogReason;
    reasonKey: string;
    message: string;
    dropLocalState: boolean;
};
export type RecoveryGuardStorage = {
    getItem: (key: string) => string | null;
    setItem: (key: string, value: string) => void;
};
export type ScheduleReloadContext = {
    reason: string;
    reasonKey: string;
    dropLocalState: boolean;
    performReload: () => Promise<void>;
};
export type ZeroRecoveryDeps = {
    deleteLocalState: () => Promise<unknown>;
    zeroEvents: Emitter<ZeroEvent | null>;
    beforeReload?: () => Promise<void>;
    scheduleReload?: (ctx: ScheduleReloadContext) => void;
    guardStorage?: RecoveryGuardStorage;
    benignLogFilter?: (message: string) => boolean;
    reload?: () => void;
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