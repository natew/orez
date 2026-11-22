import type {LogContext} from '@rocicorp/logger';
import {initBgIntervalProcess} from '../bg-interval.ts';
import type {Store} from '../dag/store.ts';
import {
  addDeletedClients,
  type WritableDeletedClients,
} from '../deleted-clients.ts';
import type {ClientID} from '../sync/ids.ts';
import {withWrite} from '../with-transactions.ts';
import type {Client, OnClientsDeleted} from './clients.ts';
import {type ClientMap, getClients, setClients} from './clients.ts';

/**
 * The maximum time a client can be inactive before it is garbage collected.
 * This means that this is the maximum time a tab can be in the background
 * (frozen) and still be able to sync when it comes back to the foreground.
 */
export const CLIENT_MAX_INACTIVE_TIME = 24 * 60 * 60 * 1000; // 24 hours

/**
 * How frequently to try to garbage collect clients.
 */
export const GC_INTERVAL = 5 * 60 * 1000; // 5 minutes

let latestGCUpdate: Promise<ClientMap> | undefined;
export function getLatestGCUpdate(): Promise<ClientMap> | undefined {
  return latestGCUpdate;
}

export function initClientGC(
  clientID: ClientID,
  dagStore: Store,
  clientMaxInactiveTime: number,
  gcInterval: number,
  onClientsDeleted: OnClientsDeleted,
  lc: LogContext,
  signal: AbortSignal,
  liveClientIDs?: ReadonlySet<ClientID>,
): void {
  let delay = liveClientIDs ? 2_000 : gcInterval;
  initBgIntervalProcess(
    'ClientGC',
    () => {
      latestGCUpdate = gcClients(
        clientID,
        dagStore,
        clientMaxInactiveTime,
        onClientsDeleted,
        liveClientIDs,
      );
      return latestGCUpdate;
    },
    () => {
      const next = delay;
      delay = gcInterval;
      return next;
    },
    lc,
    signal,
  );
}

function gcClients(
  clientID: ClientID,
  dagStore: Store,
  clientMaxInactiveTime: number,
  onClientsDeleted: OnClientsDeleted,
  liveClientIDs: ReadonlySet<ClientID> | undefined,
): Promise<ClientMap> {
  return withWrite(dagStore, async dagWrite => {
    const now = Date.now();
    const clients = await getClients(dagWrite);
    const deletedClients: WritableDeletedClients = [];
    const newClients: Map<ClientID, Client> = new Map();
    let releasedPins = false;
    for (const [id, client] of clients) {
      if (id === clientID || liveClientIDs?.has(id)) {
        newClients.set(id, client);
      } else if (now - client.heartbeatTimestampMs > clientMaxInactiveTime) {
        deletedClients.push({
          clientGroupID: client.clientGroupID,
          clientID: id,
        });
      } else if (
        liveClientIDs &&
        'refreshHashes' in client &&
        (client.refreshHashes.length > 0 || client.persistHash)
      ) {
        // pending mutations belong to the group, independently of these read pins.
        newClients.set(id, {...client, refreshHashes: [], persistHash: null});
        releasedPins = true;
      } else {
        newClients.set(id, client);
      }
    }

    if (!releasedPins && deletedClients.length === 0) {
      return clients;
    }
    await setClients(newClients, dagWrite);
    if (deletedClients.length > 0) {
      await onClientsDeleted(await addDeletedClients(dagWrite, deletedClients));
    }
    return newClients;
  });
}
