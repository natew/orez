import {assertObject} from '../../../shared/src/asserts.ts';
import {hasOwn} from '../../../shared/src/has-own.ts';
import * as valita from '../../../shared/src/valita.ts';
import {emptyDataNode} from '../btree/node.ts';
import {type Refs, toRefs} from '../dag/chunk.ts';
import type {Read, Store, Write} from '../dag/store.ts';
import {newSnapshotCommitDataDD31} from '../db/commit.ts';
import type {DeletedClients} from '../deleted-clients.ts';
import {type FrozenJSONValue, deepFreeze} from '../frozen-json.ts';
import {type Hash, hashSchema} from '../hash.ts';
import {
  type ClientGroupID,
  type ClientID,
  clientGroupIDSchema,
} from '../sync/ids.ts';
import {withWrite} from '../with-transactions.ts';
import {
  type ClientGroup,
  getClientGroup,
  getClientGroups,
  mutatorNamesEqual,
  setClientGroup,
} from './client-groups.ts';
import {makeClientID} from './make-client-id.ts';

export type ClientMap = ReadonlyMap<ClientID, ClientV5 | ClientV6>;

const clientV5Schema = valita.readonlyObject({
  heartbeatTimestampMs: valita.number(),

  headHash: hashSchema,

  /**
   * The hash of a commit we are in the middle of refreshing into this client's
   * memdag.
   */
  tempRefreshHash: hashSchema.nullable(),

  /**
   * ID of this client's perdag client group. This needs to be sent in pull
   * request (to enable syncing all last mutation ids in the client group).
   */
  clientGroupID: clientGroupIDSchema,
});

export type ClientV5 = valita.Infer<typeof clientV5Schema>;

const clientV6Schema = valita.readonlyObject({
  heartbeatTimestampMs: valita.number(),

  /**
   * A set of hashes, which contains:
   * 1. The hash of the last commit this client refreshed from its client group
   *    (this is the commit it bootstrapped from until it completes its first
   *    refresh).
   * 2. One or more hashes that were added to retain chunks of a commit while it
   *    was being refreshed into this client's memdag. (This can be one or more
   *    because refresh's cleanup step is a separate transaction and can fail).
   * Upon refresh completing and successfully running its clean up step, this
   * set will contain a single hash: the hash of the last commit this client
   * refreshed.
   */
  refreshHashes: valita.readonlyArray(hashSchema),

  /**
   * The hash of the last snapshot commit persisted by this client to this
   * client's client group, or null if has never persisted a snapshot.
   */
  persistHash: hashSchema.nullable(),

  /**
   * ID of this client's perdag client group. This needs to be sent in pull
   * request (to enable syncing all last mutation ids in the client group).
   */
  clientGroupID: clientGroupIDSchema,
});

export type ClientV6 = valita.Infer<typeof clientV6Schema>;

export type Client = ClientV5 | ClientV6;

function isClientV6(client: Client): client is ClientV6 {
  return (client as ClientV6).refreshHashes !== undefined;
}

export const CLIENTS_HEAD_NAME = 'clients';

const clientSchema = valita.union(clientV5Schema, clientV6Schema);

function assertClient(value: unknown): asserts value is Client {
  valita.assert(value, clientSchema);
}

export function assertClientV6(value: unknown): asserts value is ClientV6 {
  valita.assert(value, clientV6Schema);
}

function chunkDataToClientMap(chunkData: unknown): ClientMap {
  assertObject(chunkData);
  const clients = new Map();
  for (const key in chunkData) {
    if (hasOwn(chunkData, key)) {
      const value = chunkData[key];
      if (value !== undefined) {
        assertClient(value);
        clients.set(key, value);
      }
    }
  }
  return clients;
}

function clientMapToChunkData(
  clients: ClientMap,
  dagWrite: Write,
): FrozenJSONValue {
  for (const client of clients.values()) {
    if (isClientV6(client)) {
      client.refreshHashes.forEach(dagWrite.assertValidHash);
      if (client.persistHash) {
        dagWrite.assertValidHash(client.persistHash);
      }
    } else {
      dagWrite.assertValidHash(client.headHash);
      if (client.tempRefreshHash) {
        dagWrite.assertValidHash(client.tempRefreshHash);
      }
    }
  }
  return deepFreeze(Object.fromEntries(clients));
}

export async function getClients(dagRead: Read): Promise<ClientMap> {
  const hash = await dagRead.getHead(CLIENTS_HEAD_NAME);
  return getClientsAtHash(hash, dagRead);
}

async function getClientsAtHash(
  hash: Hash | undefined,
  dagRead: Read,
): Promise<ClientMap> {
  if (!hash) {
    return new Map();
  }
  const chunk = await dagRead.getChunk(hash);
  return chunkDataToClientMap(chunk?.data);
}

/**
 * Used to signal that a client does not exist. Maybe it was garbage collected?
 */
export class ClientStateNotFoundError extends Error {
  name = 'ClientStateNotFoundError';
  readonly id: string;
  constructor(id: ClientID) {
    super(`Client state not found, id: ${id}`);
    this.id = id;
  }
}

/**
 * Throws a `ClientStateNotFoundError` if the client does not exist.
 */
export async function assertHasClientState(
  id: ClientID,
  dagRead: Read,
): Promise<void> {
  if (!(await hasClientState(id, dagRead))) {
    throw new ClientStateNotFoundError(id);
  }
}

export async function hasClientState(
  id: ClientID,
  dagRead: Read,
): Promise<boolean> {
  return !!(await getClient(id, dagRead));
}

export async function getClient(
  id: ClientID,
  dagRead: Read,
): Promise<Client | undefined> {
  const clients = await getClients(dagRead);
  return clients.get(id);
}

export async function mustGetClient(
  id: ClientID,
  dagRead: Read,
): Promise<Client> {
  const client = await getClient(id, dagRead);
  if (!client) {
    throw new ClientStateNotFoundError(id);
  }
  return client;
}

export function initClientV6(
  newClientID: ClientID,
  perdag: Store,
  mutatorNames: string[],
): Promise<[client: ClientV6, headHash: Hash, isNewClientGroup: boolean]> {
  return withWrite(perdag, async dagWrite => {
    const clients = await getClients(dagWrite);
    const clientGroups = await getClientGroups(dagWrite);
    const mutatorNamesSet = new Set(mutatorNames);
    for (const [clientGroupID, clientGroup] of clientGroups) {
      if (
        !clientGroup.disabled &&
        mutatorNamesEqual(mutatorNamesSet, clientGroup.mutatorNames) &&
        Object.keys(clientGroup.indexes).length === 0
      ) {
        const {headHash} = clientGroup;
        const client: ClientV6 = {
          clientGroupID,
          refreshHashes: [headHash],
          heartbeatTimestampMs: Date.now(),
          persistHash: null,
        };
        await setClients(new Map(clients).set(newClientID, client), dagWrite);
        return [client, headHash, false];
      }
    }

    const emptyBTreeChunk = dagWrite.createChunk(emptyDataNode, []);
    await dagWrite.putChunk(emptyBTreeChunk);
    const snapshot = dagWrite.createChunk(
      newSnapshotCommitDataDD31(null, {}, null, emptyBTreeChunk.hash, []),
      [emptyBTreeChunk.hash],
    );
    const clientGroupID = makeClientID();
    const client: ClientV6 = {
      clientGroupID,
      refreshHashes: [snapshot.hash],
      heartbeatTimestampMs: Date.now(),
      persistHash: null,
    };
    const clientGroup: ClientGroup = {
      headHash: snapshot.hash,
      mutatorNames,
      indexes: {},
      mutationIDs: {},
      lastServerAckdMutationIDs: {},
      disabled: false,
    };
    await Promise.all([
      dagWrite.putChunk(snapshot),
      setClients(new Map(clients).set(newClientID, client), dagWrite),
      setClientGroup(clientGroupID, clientGroup, dagWrite),
    ]);
    return [client, snapshot.hash, true];
  });
}

function getRefsForClients(clients: ClientMap): Refs {
  const refs: Set<Hash> = new Set();
  for (const client of clients.values()) {
    if (isClientV6(client)) {
      for (const hash of client.refreshHashes) {
        refs.add(hash);
      }
      if (client.persistHash) {
        refs.add(client.persistHash);
      }
    } else {
      refs.add(client.headHash);
      if (client.tempRefreshHash) {
        refs.add(client.tempRefreshHash);
      }
    }
  }
  return toRefs(refs);
}

export async function getClientGroupForClient(
  clientID: ClientID,
  read: Read,
): Promise<ClientGroup | undefined> {
  const clientGroupID = await getClientGroupIDForClient(clientID, read);
  if (!clientGroupID) {
    return undefined;
  }
  return getClientGroup(clientGroupID, read);
}

export async function getClientGroupIDForClient(
  clientID: ClientID,
  read: Read,
): Promise<ClientGroupID | undefined> {
  const client = await getClient(clientID, read);
  return client?.clientGroupID;
}

/**
 * Adds a Client to the ClientMap and updates the 'clients' head to point at
 * the updated clients.
 */
export async function setClient(
  clientID: ClientID,
  client: Client,
  dagWrite: Write,
): Promise<Hash> {
  const clients = await getClients(dagWrite);
  const newClients = new Map(clients).set(clientID, client);
  return setClients(newClients, dagWrite);
}

/**
 * Sets the ClientMap and updates the 'clients' head top point at the new
 * clients.
 */
export async function setClients(
  clients: ClientMap,
  dagWrite: Write,
): Promise<Hash> {
  const chunkData = clientMapToChunkData(clients, dagWrite);
  const chunk = dagWrite.createChunk(chunkData, getRefsForClients(clients));
  await dagWrite.putChunk(chunk);
  await dagWrite.setHead(CLIENTS_HEAD_NAME, chunk.hash);
  return chunk.hash;
}

/**
 * Callback function for when Replicache has deleted one or more clients.
 */
export type OnClientsDeleted = (
  deletedClients: DeletedClients,
) => Promise<void>;
