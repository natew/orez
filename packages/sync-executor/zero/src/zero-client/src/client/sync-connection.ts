import type {DeleteClientsBody} from '../../../zero-protocol/src/delete-clients.ts';
import type {UpQueriesPatchOp} from '../../../zero-protocol/src/queries-patch.ts';
import type {Upstream} from '../../../zero-protocol/src/up.ts';

export type HttpConnectOptions = {
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly connectionID: string;
  readonly baseCookie: string | null;
  readonly attemptStartedAt?: number | undefined;
  readonly authToken?: string | undefined;
  readonly init?: {
    readonly desiredQueriesPatch: readonly UpQueriesPatchOp[];
    readonly deleted?: DeleteClientsBody | undefined;
    readonly activeClients?: readonly string[] | undefined;
  } | undefined;
};

export type SyncConnectionListener = {
  open(): void;
  message(data: unknown): void;
  close(event: Pick<CloseEvent, 'code' | 'reason' | 'wasClean'>): void;
};

export type SyncConnection = {
  readonly id: string;
  listen(listener: SyncConnectionListener): () => void;
  send(message: Upstream): void;
  close(code?: number, reason?: string): void;
};
