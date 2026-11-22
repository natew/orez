import type {InitConnectionBody} from '../../../zero-protocol/src/connect.ts';
import type {Upstream} from '../../../zero-protocol/src/up.ts';

export type HttpConnectOptions = {
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly connectionID: string;
  readonly baseCookie: string | null;
  readonly attemptStartedAt?: number | undefined;
  readonly authToken?: string | undefined;
  readonly init?: InitConnectionBody | undefined;
};

export type SyncConnectionListener = {
  open(): void;
  message(data: unknown): void;
  close(event: Pick<CloseEvent, 'code' | 'reason' | 'wasClean'>): void;
};

export type SyncConnection = {
  readonly id: string;
  readonly webSocket?: WebSocket;
  listen(listener: SyncConnectionListener): () => void;
  send(message: Upstream, serialized?: string): void;
  close(code?: number, reason?: string): void;
};

type ConnectHttp = (options: HttpConnectOptions) => SyncConnection;

const transports = new Map<string, Set<ConnectHttp>>();

function originKey(origin: string) {
  const url = new URL(origin);
  return url.origin + url.pathname.replace(/\/$/, '');
}

export function registerHttpConnection(origin: string, connect: ConnectHttp) {
  const key = originKey(origin);
  let connections = transports.get(key);
  if (!connections) {
    connections = new Set();
    transports.set(key, connections);
  }
  connections.add(connect);
  return () => {
    if (connections.delete(connect) && !connections.size) {
      transports.delete(key);
    }
  };
}

export function httpConnectionFor(origin: string): ConnectHttp | undefined {
  const connections = transports.get(originKey(origin));
  if (connections) return [...connections].at(-1);
}
