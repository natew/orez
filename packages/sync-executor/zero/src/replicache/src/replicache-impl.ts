import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {consoleLogSink} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {AbortError} from '../../shared/src/abort-error.ts';
import {assert} from '../../shared/src/asserts.ts';
import {getBrowserGlobal} from '../../shared/src/browser-env.ts';
import {getDocumentVisibilityWatcher} from '../../shared/src/document-visible.ts';
import type {JSONValue, ReadonlyJSONValue} from '../../shared/src/json.ts';
import {promiseVoid} from '../../shared/src/resolved-promises.ts';
import type {MaybePromise} from '../../shared/src/types.ts';
import type {NoIndexDiff} from './btree/node.ts';
import {ConnectionLoop} from './connection-loop.ts';
import {assertCookie, type Cookie} from './cookies.ts';
import {LazyStore} from './dag/lazy-store.ts';
import {StoreImpl} from './dag/store-impl.ts';
import {ChunkNotFoundError, mustGetHeadHash, type Store} from './dag/store.ts';
import {
  baseSnapshotFromHash,
  DEFAULT_HEAD_NAME,
  isLocalMetaDD31,
  type LocalMeta,
} from './db/commit.ts';
import {readFromDefaultHead} from './db/read.ts';
import {rebaseMutationAndCommit} from './db/rebase.ts';
import {newWriteLocal} from './db/write.ts';
import {
  isClientStateNotFoundResponse,
  isVersionNotSupportedResponse,
  type VersionNotSupportedResponse,
} from './error-responses.ts';
import * as FormatVersion from './format-version-enum.ts';
import {deepFreeze} from './frozen-json.ts';
import {getKVStoreProvider} from './get-kv-store-provider.ts';
import {assertHash, emptyHash, type Hash, newRandomHash} from './hash.ts';
import type {HTTPRequestInfo} from './http-request-info.ts';
import type {StoreProvider} from './kv/store.ts';
import {createLogContext} from './log-options.ts';
import {makeIDBName} from './make-idb-name.ts';
import {MutationRecovery} from './mutation-recovery.ts';
import {initNewClientChannel} from './new-client-channel.ts';
import {
  initOnPersistChannel,
  type OnPersist,
  type PersistInfo,
} from './on-persist-channel.ts';
import {
  type PendingMutation,
  pendingMutationsForAPI,
} from './pending-mutations.ts';
import {
  CLIENT_MAX_INACTIVE_TIME,
  GC_INTERVAL,
  initClientGC,
} from './persist/client-gc.ts';
import {initClientGroupGC} from './persist/client-group-gc.ts';
import {disableClientGroup} from './persist/client-groups.ts';
import {
  ClientStateNotFoundError,
  assertClientV6,
  getClient,
  initClientV6,
  type OnClientsDeleted,
  hasClientState as persistHasClientState,
  setClient,
} from './persist/clients.ts';
import {
  COLLECT_IDB_INTERVAL,
  initCollectIDBDatabases,
  INITIAL_COLLECT_IDB_DELAY,
} from './persist/collect-idb-databases.ts';
import {HEARTBEAT_INTERVAL, startHeartbeats} from './persist/heartbeat.ts';
import {
  IDBDatabasesStore,
  type IndexedDBDatabase,
} from './persist/idb-databases-store.ts';
import {makeClientID} from './persist/make-client-id.ts';
import {persistDD31} from './persist/persist.ts';
import {refresh} from './persist/refresh.ts';
import {ProcessScheduler} from './process-scheduler.ts';
import type {Puller} from './puller.ts';
import {type Pusher, PushError} from './pusher.ts';
import type {
  MutationTrackingData,
  ReplicacheOptions,
  ZeroOption,
} from './replicache-options.ts';
import {ReportError} from './report-error.ts';
import {setIntervalWithSignal} from './set-interval-with-signal.ts';
import {SubscriptionsManager} from './subscriptions.ts';
import type {DiffsMap} from './sync/diff.ts';
import * as HandlePullResponseResultEnum from './sync/handle-pull-response-result-type-enum.ts';
import type {ClientGroupID, ClientID} from './sync/ids.ts';
import {PullError} from './sync/pull-error.ts';
import {beginPullV1, handlePullResponseV1, maybeEndPull} from './sync/pull.ts';
import {push, PUSH_VERSION_DD31} from './sync/push.ts';
import {newRequestID} from './sync/request-id.ts';
import {SYNC_HEAD_NAME} from './sync/sync-head-name.ts';
import {throwIfClosed} from './transaction-closed-error.ts';
import type {ReadTransaction, WriteTransaction} from './transactions.ts';
import {ReadTransactionImpl, WriteTransactionImpl} from './transactions.ts';
import type {
  BeginPullResult,
  MakeMutator,
  MakeMutators,
  MutatorDefs,
  MutatorReturn,
  PokeInternal,
  QueryInternal,
  UpdateNeededReason,
} from './types.ts';
import {version} from './version.ts';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from './with-transactions.ts';

const PERSIST_IDLE_TIMEOUT_MS = 1000;
const REFRESH_IDLE_TIMEOUT_MS = 1000;

const PERSIST_THROTTLE_MS = 500;
const REFRESH_THROTTLE_MS = 500;

const LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT = 100 * 2 ** 20; // 100 MB

const RECOVER_MUTATIONS_INTERVAL_MS = 5 * 60 * 1000; // 5 mins

const updateNeededReasonNewClientGroup: UpdateNeededReason = {
  type: 'NewClientGroup',
} as const;

export interface ReplicacheImplOptions {
  /**
   * Defaults to true.
   */
  enableMutationRecovery?: boolean | undefined;

  /**
   * Defaults to true.
   */
  enableScheduledPersist?: boolean | undefined;

  /**
   * Defaults to true.
   */
  enableScheduledRefresh?: boolean | undefined;

  /**
   * Defaults to `() => true`.
   */
  enableRefresh?: (() => boolean) | undefined;

  /**
   * Callback for when Replicache has deleted clients.
   */
  onClientsDeleted?: OnClientsDeleted | undefined;

  /**
   * Internal option used by Zero.
   * Replicache will call this to and, if zero is enabled, will
   * invoke various hooks to allow Zero the keep IVM in sync with Replicache's b-trees.
   */
  zero?: ZeroOption | undefined;
}

export class ReplicacheImpl<MD extends MutatorDefs = {}> {
  /** The authorization token used when doing a push request. */
  #auth: string;

  /** The name of the Replicache database. Populated by {@link ReplicacheOptions#name}. */
  readonly name: string;

  readonly #subscriptions: SubscriptionsManager;

  /**
   * Client groups gets disabled when the server does not know about it.
   * A disabled client group prevents the client from pushing and pulling.
   */
  isClientGroupDisabled = false;

  readonly #kvStoreProvider: StoreProvider;

  lastMutationID: number = 0;

  /**
   * This is the name Replicache uses for the IndexedDB database where data is
   * stored.
   */
  get idbName(): string {
    return makeIDBName(this.name, this.schemaVersion);
  }

  set auth(auth: string) {
    if (this.#zero) {
      this.#zero.auth = auth;
    }

    this.#auth = auth;
  }

  get auth() {
    return this.#auth;
  }

  /** The schema version of the data understood by this application. */
  readonly schemaVersion: string;

  get #idbDatabase(): IndexedDBDatabase {
    return {
      name: this.idbName,
      replicacheName: this.name,
      replicacheFormatVersion: FormatVersion.Latest,
      schemaVersion: this.schemaVersion,
    };
  }
  #closed = false;
  readonly #refreshes = new Set<Promise<void>>();
  #online = true;
  readonly #clientID = makeClientID();
  readonly #ready: Promise<void>;
  readonly #profileIDPromise: Promise<string>;
  readonly #clientGroupIDPromise: Promise<string>;
  readonly #mutatorRegistry: MutatorDefs = {};

  /**
   * The mutators that was registered in the constructor.
   */
  readonly mutate: MakeMutators<MD>;

  readonly #pullConnectionLoop: ConnectionLoop;
  readonly #pushConnectionLoop: ConnectionLoop;

  /**
   * The function to use to pull data from the server.
   */
  puller: Puller;

  /**
   * The function to use to push data to the server.
   */
  pusher: Pusher;

  readonly memdag: LazyStore;
  readonly perdag: Store;
  readonly #idbDatabases: IDBDatabasesStore;
  readonly #lc: LogContext;
  readonly #zero: ZeroOption | undefined;
  readonly #mutationRecovery: MutationRecovery;

  readonly #closeAbortController = new AbortController();

  readonly #persistLock = new Lock();
  readonly #enableScheduledPersist: boolean;
  readonly #enableScheduledRefresh: boolean;
  readonly #enableRefresh: () => boolean;
  #persistScheduler = new ProcessScheduler(
    () => this.persist(),
    PERSIST_IDLE_TIMEOUT_MS,
    PERSIST_THROTTLE_MS,
    this.#closeAbortController.signal,
  );
  readonly #onPersist: OnPersist;
  #refreshScheduler = new ProcessScheduler(
    () => this.refresh(),
    REFRESH_IDLE_TIMEOUT_MS,
    REFRESH_THROTTLE_MS,
    this.#closeAbortController.signal,
  );

  /**
   * `onClientStateNotFound` is called when the persistent client has been
   * garbage collected. This can happen if the client has no pending mutations
   * and has not been used for a while.
   *
   * The default behavior is to reload the page (using `location.reload()`). Set
   * this to `null` or provide your own function to prevent the page from
   * reloading automatically.
   */
  onClientStateNotFound: (() => void) | null = reload;

  /**
   * `onUpdateNeeded` is called when a code update is needed.
   *
   * A code update can be needed because:
   * - the server no longer supports the {@link pushVersion},
   *   {@link pullVersion} or {@link schemaVersion} of the current code.
   * - a new Replicache client has created a new client group, because its code
   *   has different mutators, indexes, schema version and/or format version
   *   from this Replicache client. This is likely due to the new client having
   *   newer code. A code update is needed to be able to locally sync with this
   *   new Replicache client (i.e. to sync while offline, the clients can still
   *   sync with each other via the server).
   *
   * The default behavior is to reload the page (using `location.reload()`). Set
   * this to `null` or provide your own function to prevent the page from
   * reloading automatically. You may want to provide your own function to
   * display a toast to inform the end user there is a new version of your app
   * available and prompting them to refresh.
   */
  onUpdateNeeded: ((reason: UpdateNeededReason) => void) | null = reload;

  constructor(
    options: ReplicacheOptions<MD>,
    implOptions: ReplicacheImplOptions = {},
  ) {
    validateOptions(options);
    const {
      name,
      logLevel = 'info',
      logSinks = [consoleLogSink],
      auth,
      schemaVersion = '',
      mutators = {} as MD,
      puller,
      pusher,
      clientMaxAgeMs = CLIENT_MAX_INACTIVE_TIME,
    } = options;
    const {
      enableMutationRecovery = true,
      enableScheduledPersist = true,
      enableScheduledRefresh = true,
      enableRefresh = () => true,
      onClientsDeleted = () => promiseVoid,
    } = implOptions;
    this.#zero = implOptions.zero;
    this.#auth = auth ?? '';
    this.name = name;
    this.schemaVersion = schemaVersion;
    this.puller = puller;
    this.pusher = pusher;

    this.#enableScheduledPersist = enableScheduledPersist;
    this.#enableScheduledRefresh = enableScheduledRefresh;
    this.#enableRefresh = enableRefresh;

    this.#lc = createLogContext(logLevel, logSinks, {name});
    this.#lc.debug?.('Constructing Replicache', {
      name,
      'replicache version': version,
    });

    this.#subscriptions = new SubscriptionsManager(
      this.#queryInternal,
      this.#lc,
      this.#closeAbortController.signal,
    );

    const kvStoreProvider = getKVStoreProvider(this.#lc, options.kvStore);
    this.#kvStoreProvider = kvStoreProvider;

    const perKVStore = kvStoreProvider.create(this.idbName);

    this.#idbDatabases = new IDBDatabasesStore(kvStoreProvider.create);
    this.perdag = new StoreImpl(perKVStore, newRandomHash, assertHash);
    this.memdag = new LazyStore(
      this.perdag,
      LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT,
      newRandomHash,
      assertHash,
    );

    // Use a promise-resolve pair so that we have a promise to use even before
    // we call the Open RPC.
    const readyResolver = resolver<void>();
    this.#ready = readyResolver.promise;

    const visibilityWatcher = getDocumentVisibilityWatcher(
      getBrowserGlobal('document'),
      0,
      this.#closeAbortController.signal,
    );

    this.#pullConnectionLoop = new ConnectionLoop(
      this.#lc.withContext('PULL'),
      () => this.#invokePull(),
      60_000,
      visibilityWatcher,
    );

    this.#pushConnectionLoop = new ConnectionLoop(
      this.#lc.withContext('PUSH'),
      () => this.#invokePush(),
      null,
    );

    this.mutate = this.#registerMutators(mutators);

    const profileIDResolver = resolver<string>();
    this.#profileIDPromise = profileIDResolver.promise;
    const clientGroupIDResolver = resolver<string>();
    this.#clientGroupIDPromise = clientGroupIDResolver.promise;

    this.#mutationRecovery = new MutationRecovery({
      delegate: this,
      lc: this.#lc,
      enableMutationRecovery,
      wrapInOnlineCheck: this.#wrapInOnlineCheck.bind(this),
      runRequest: this.#runRequest.bind(this),
      clientGroupIDPromise: this.#clientGroupIDPromise,
    });

    this.#onPersist = initOnPersistChannel(
      this.name,
      this.#closeAbortController.signal,
      persistInfo => {
        void this.#handlePersist(persistInfo);
      },
    );

    void this.#open(
      enableMutationRecovery,
      clientMaxAgeMs,
      profileIDResolver.resolve,
      clientGroupIDResolver.resolve,
      readyResolver.resolve,
      onClientsDeleted,
    );
  }

  async #open(
    enableMutationRecovery: boolean,
    clientMaxAgeMs: number,
    profileIDResolver: (profileID: string) => void,
    resolveClientGroupID: (clientGroupID: ClientGroupID) => void,
    resolveReady: () => void,
    onClientsDeleted: OnClientsDeleted,
  ): Promise<void> {
    const {clientID} = this;
    // If we are currently closing a Replicache instance with the same name,
    // wait for it to finish closing.
    await closingInstances.get(this.name);
    const liveClientIDs = this.#kvStoreProvider.singleProcess ? singleProcessClients : undefined;
    liveClientIDs?.add(clientID);
    await this.#idbDatabases.getProfileID().then(profileIDResolver);
    await this.#idbDatabases.putDatabase(this.#idbDatabase);
    const [client, headHash, isNewClientGroup] = await initClientV6(
      clientID,
      this.perdag,
      Object.keys(this.#mutatorRegistry),
    );

    resolveClientGroupID(client.clientGroupID);
    await withWrite(this.memdag, write =>
      write.setHead(DEFAULT_HEAD_NAME, headHash),
    );

    // Now we have a profileID, a clientID, a clientGroupID and DB!
    await this.#zero?.init(headHash, this.memdag, liveClientIDs ? 64 : 1);
    resolveReady();

    const {signal} = this.#closeAbortController;

    startHeartbeats(
      clientID,
      this.perdag,
      () => {
        this.#clientStateNotFoundOnClient(clientID);
      },
      HEARTBEAT_INTERVAL,
      this.#lc,
      signal,
    );
    initClientGC(
      clientID,
      this.perdag,
      clientMaxAgeMs,
      GC_INTERVAL,
      onClientsDeleted,
      this.#lc,
      signal,
      liveClientIDs,
    );
    initCollectIDBDatabases(
      this.#idbDatabases,
      this.#kvStoreProvider,
      COLLECT_IDB_INTERVAL,
      liveClientIDs ? 0 : INITIAL_COLLECT_IDB_DELAY,
      2 * clientMaxAgeMs,
      enableMutationRecovery,
      onClientsDeleted,
      this.#lc,
      signal,
    );
    initClientGroupGC(this.perdag, enableMutationRecovery, this.#lc, signal, liveClientIDs !== undefined);
    initNewClientChannel(
      this.name,
      this.idbName,
      signal,
      client.clientGroupID,
      isNewClientGroup,
      () => {
        this.#fireOnUpdateNeeded(updateNeededReasonNewClientGroup);
      },
      this.perdag,
    );

    setIntervalWithSignal(
      () => this.recoverMutations(),
      RECOVER_MUTATIONS_INTERVAL_MS,
      signal,
    );
    void this.recoverMutations();

    getBrowserGlobal('document')?.addEventListener(
      'visibilitychange',
      this.#onVisibilityChange,
    );
  }

  #onVisibilityChange = async () => {
    if (this.#closed) {
      return;
    }

    // In case of running in a worker, we don't have a document.
    if (getBrowserGlobal('document')?.visibilityState !== 'visible') {
      return;
    }

    await this.#checkForClientStateNotFoundAndCallHandler();
  };

  async #checkForClientStateNotFoundAndCallHandler(): Promise<boolean> {
    const {clientID} = this;
    const hasClientState = await withRead(this.perdag, read =>
      persistHasClientState(clientID, read),
    );
    if (!hasClientState) {
      this.#clientStateNotFoundOnClient(clientID);
    }
    return !hasClientState;
  }

  /**
   * The browser profile ID for this browser profile. Every instance of Replicache
   * browser-profile-wide shares the same profile ID.
   */
  get profileID(): Promise<string> {
    return this.#profileIDPromise;
  }

  /**
   * The client ID for this instance of Replicache. Each instance of Replicache
   * gets a unique client ID.
   */
  get clientID(): string {
    return this.#clientID;
  }

  /**
   * The client group ID for this instance of Replicache. Instances of
   * Replicache will have the same client group ID if and only if they have
   * the same name, mutators, indexes, schema version, format version, and
   * browser profile.
   */
  get clientGroupID(): Promise<string> {
    return this.#clientGroupIDPromise;
  }

  /**
   * A rough heuristic for whether the client is currently online. Note that
   * there is no way to know for certain whether a client is online - the next
   * request can always fail. This property returns true if the last sync attempt succeeded,
   * and false otherwise.
   */
  get online(): boolean {
    return this.#online;
  }

  /**
   * Whether the Replicache database has been closed. Once Replicache has been
   * closed it no longer syncs and you can no longer read or write data out of
   * it. After it has been closed it is pretty much useless and should not be
   * used any more.
   */
  get closed(): boolean {
    return this.#closed;
  }

  /**
   * Closes this Replicache instance.
   *
   * When closed all subscriptions end and no more read or writes are allowed.
   */
  async close(): Promise<void> {
    this.#closed = true;
    const {promise, resolve} = resolver();
    closingInstances.set(this.name, promise);

    try {
      this.#closeAbortController.abort();

      getBrowserGlobal('document')?.removeEventListener(
        'visibilitychange',
        this.#onVisibilityChange,
      );

      await this.#ready;
      await Promise.allSettled(this.#refreshes);
      await this.#persistLock.withLock(() => promiseVoid);
      try {
        // drain readers before collecting chunks still needed by their lazy reads.
        await withWriteNoImplicitCommit(this.memdag, () =>
          withWrite(this.perdag, async write => {
            const client = await getClient(this.clientID, write);
            if (!client) return;
            assertClientV6(client);
            // retain identity and the group containing persisted unacknowledged mutations.
            await setClient(this.clientID, {...client, refreshHashes: [], persistHash: null}, write);
          }),
        );
      } catch (error) {
        this.#lc.debug?.('Failed to release snapshot pins during close', error);
      }

      const closingPromises = [
        this.memdag.close(),
        this.perdag.close(),
        this.#idbDatabases.close(),
      ];

      this.#pullConnectionLoop.close();
      this.#pushConnectionLoop.close();
      this.#subscriptions.clear();

      await Promise.all(closingPromises);
    } finally {
      singleProcessClients.delete(this.clientID);
      closingInstances.delete(this.name);
      resolve();
    }
  }

  async maybeEndPull(syncHead: Hash, requestID: string): Promise<void> {
    for (;;) {
      if (this.#closed) {
        return;
      }

      await this.#ready;
      const {clientID} = this;
      const lc = this.#lc
        .withContext('maybeEndPull')
        .withContext('requestID', requestID);
      const {replayMutations, diffs, oldMainHead, mainHead} =
        await maybeEndPull<LocalMeta>(
          this.memdag,
          lc,
          syncHead,
          clientID,
          this.#subscriptions,
          FormatVersion.Latest,
        );

      if (!replayMutations || replayMutations.length === 0) {
        // All done.
        this.#zero?.advance(oldMainHead, mainHead, diffs.get('') ?? []);
        await this.#subscriptions.fire(diffs);
        void this.#schedulePersist();
        return;
      }

      // Replay.
      const zeroData = await this.#zero?.getTxData?.(syncHead);
      for (const mutation of replayMutations) {
        // TODO(greg): I'm not sure why this was in Replicache#_mutate...
        // Ensure that we run initial pending subscribe functions before starting a
        // write transaction.
        if (this.#subscriptions.hasPendingSubscriptionRuns) {
          await Promise.resolve();
        }
        const {meta} = mutation;
        syncHead = await withWriteNoImplicitCommit(this.memdag, dagWrite =>
          rebaseMutationAndCommit(
            mutation,
            dagWrite,
            syncHead,
            SYNC_HEAD_NAME,
            this.#mutatorRegistry,
            lc,
            isLocalMetaDD31(meta) ? meta.clientID : clientID,
            FormatVersion.Latest,
            zeroData,
          ),
        );
      }
    }
  }

  #invokePull(): Promise<boolean> {
    if (this.isClientGroupDisabled) {
      return Promise.resolve(true);
    }

    return this.#wrapInOnlineCheck(async () => {
      try {
        const {syncHead, requestID, ok} = await this.beginPull();
        if (!ok) {
          return false;
        }
        if (syncHead !== emptyHash) {
          await this.maybeEndPull(syncHead, requestID);
        }
      } catch (e) {
        throw await this.#convertToClientStateNotFoundError(e);
      }
      return true;
    }, 'Pull');
  }

  async #wrapInOnlineCheck(
    f: () => Promise<boolean>,
    name: string,
  ): Promise<boolean> {
    let online = true;

    try {
      return await f();
    } catch (e) {
      // The error paths of beginPull and maybeEndPull need to be reworked.
      //
      // We want to distinguish between:
      // a) network requests failed -- we're offline basically
      // b) sync was aborted because one's already in progress
      // c) oh noes - something unexpected happened
      //
      // Right now, all of these come out as errors. We distinguish (b) with a
      // hacky string search. (a) and (c) are not distinguishable currently
      // because repc doesn't provide sufficient information, so we treat all
      // errors that aren't (b) as (a).
      if (e instanceof PushError || e instanceof PullError) {
        online = false;
        this.#lc.debug?.(`${name} threw:\n`, e, '\nwith cause:\n', e.causedBy);
      } else if (e instanceof ReportError) {
        this.#lc.error?.(e);
      } else {
        this.#lc.info?.(`${name} threw:\n`, e);
      }
      return false;
    } finally {
      if (this.#online !== online) {
        this.#online = online;
        if (online) {
          void this.recoverMutations();
        }
      }
    }
  }

  async #runRequest<R>(
    f: (
      requestID: string,
      requestLc: LogContext,
    ) => Promise<{
      httpRequestInfo: HTTPRequestInfo | undefined;
      result: R;
    }>,
    verb: string,
    lc: LogContext,
  ): Promise<R> {
    const requestID = newRequestID(this.clientID);
    const requestLc = lc.withContext(verb).withContext('requestID', requestID);
    const {httpRequestInfo, result} = await f(requestID, requestLc);
    if (httpRequestInfo) {
      const {errorMessage, httpStatusCode} = httpRequestInfo;
      if (errorMessage || httpStatusCode !== 200) {
        requestLc.error?.(
          `Got a non 200 response doing ${verb}: ${httpStatusCode}` +
            (errorMessage ? `: ${errorMessage}` : ''),
        );
      }
    }
    return result;
  }

  async #invokePush(): Promise<boolean> {
    if (this.isClientGroupDisabled) {
      return true;
    }

    await this.#ready;
    const profileID = await this.#profileIDPromise;
    const {clientID} = this;
    const clientGroupID = await this.#clientGroupIDPromise;
    return this.#wrapInOnlineCheck(async () => {
      const pusherResult = await this.#runRequest(
        async (requestID, requestLc) => {
          const result = await push(
            requestID,
            this.memdag,
            requestLc,
            profileID,
            clientGroupID,
            clientID,
            this.pusher,
            this.schemaVersion,
            PUSH_VERSION_DD31,
          );
          return {result, httpRequestInfo: result?.httpRequestInfo};
        },
        'push',
        this.#lc,
      );

      if (pusherResult === undefined) {
        // No pending mutations.
        return true;
      }

      const {response, httpRequestInfo} = pusherResult;

      if (isVersionNotSupportedResponse(response)) {
        this.#handleVersionNotSupportedResponse(response);
      } else if (isClientStateNotFoundResponse(response)) {
        await this.#clientStateNotFoundOnServer();
      }

      // No pushResponse means we didn't do a push because there were no
      // pending mutations.
      return httpRequestInfo.httpStatusCode === 200;
    }, 'Push');
  }

  #handleVersionNotSupportedResponse(response: VersionNotSupportedResponse) {
    const reason: UpdateNeededReason = {
      type: response.error,
    };
    if (response.versionType) {
      reason.versionType = response.versionType;
    }
    this.#fireOnUpdateNeeded(reason);
  }

  push(): Promise<void> {
    return throwIfError(this.#pushConnectionLoop.send());
  }

  async poke(poke: PokeInternal): Promise<void> {
    await this.#ready;
    // TODO(MP) Previously we created a request ID here and included it with the
    // PullRequest to the server so we could tie events across client and server
    // together. Since the direction is now reversed, creating and adding a request ID
    // here is kind of silly. We should consider creating the request ID
    // on the *server* and passing it down in the poke for inclusion here in the log
    // context
    const {clientID} = this;
    const requestID = newRequestID(clientID);
    const lc = this.#lc
      .withContext('handlePullResponse')
      .withContext('requestID', requestID);

    const {pullResponse} = poke;

    if (isVersionNotSupportedResponse(pullResponse)) {
      this.#handleVersionNotSupportedResponse(pullResponse);
      return;
    }

    if (isClientStateNotFoundResponse(pullResponse)) {
      await this.#clientStateNotFoundOnServer();
      return;
    }

    const result = await handlePullResponseV1(
      lc,
      this.memdag,
      deepFreeze(poke.baseCookie),
      pullResponse,
      clientID,
      FormatVersion.Latest,
    );

    switch (result.type) {
      case HandlePullResponseResultEnum.Applied:
        await this.maybeEndPull(result.syncHead, requestID);
        break;
      case HandlePullResponseResultEnum.CookieMismatch:
        throw new Error(
          'unexpected base cookie for poke: ' + JSON.stringify(poke),
        );
      case HandlePullResponseResultEnum.NoOp:
        break;
    }
  }

  async beginPull(): Promise<BeginPullResult> {
    await this.#ready;
    const profileID = await this.profileID;
    const {clientID} = this;
    const clientGroupID = await this.#clientGroupIDPromise;
    const {beginPullResponse, requestID} = await this.#runRequest(
      async (requestID: string, requestLc: LogContext) => {
        const beginPullResponse = await beginPullV1(
          profileID,
          clientID,
          clientGroupID,
          this.schemaVersion,
          this.puller,
          requestID,
          this.memdag,
          FormatVersion.Latest,
          requestLc,
        );
        return {
          result: {beginPullResponse, requestID},
          httpRequestInfo: beginPullResponse.httpRequestInfo,
        };
      },
      'pull',
      this.#lc,
    );

    const {pullResponse} = beginPullResponse;
    if (isVersionNotSupportedResponse(pullResponse)) {
      this.#handleVersionNotSupportedResponse(pullResponse);
    } else if (isClientStateNotFoundResponse(beginPullResponse.pullResponse)) {
      await this.#clientStateNotFoundOnServer();
    }

    const {syncHead, httpRequestInfo} = beginPullResponse;
    return {requestID, syncHead, ok: httpRequestInfo.httpStatusCode === 200};
  }

  persist(): Promise<void> {
    // Prevent multiple persist calls from running at the same time.
    return this.#persistLock.withLock(async () => {
      const {clientID} = this;
      await this.#ready;
      if (this.#closed) {
        return;
      }
      try {
        await persistDD31(
          this.#lc,
          clientID,
          this.memdag,
          this.perdag,
          this.#mutatorRegistry,
          () => this.#closed,
          FormatVersion.Latest,
          this.#zero?.getTxData,
        );
      } catch (e) {
        if (e instanceof ClientStateNotFoundError) {
          this.#clientStateNotFoundOnClient(clientID);
        } else if (this.#closed) {
          this.#lc.debug?.('Exception persisting during close', e);
        } else {
          throw e;
        }
      }

      const clientGroupID = await this.#clientGroupIDPromise;
      assert(clientGroupID, 'Expected clientGroupID to be defined');
      this.#onPersist({clientID, clientGroupID});
    });
  }

  refresh(): Promise<void> {
    const running = this.#refresh().finally(() => this.#refreshes.delete(running));
    this.#refreshes.add(running);
    return running;
  }

  async #refresh(): Promise<void> {
    await this.#ready;
    const {clientID} = this;
    if (this.#closed || !this.#enableRefresh()) {
      return;
    }
    let refreshResult: Awaited<ReturnType<typeof refresh>>;
    try {
      refreshResult = await refresh(
        this.#lc,
        this.memdag,
        this.perdag,
        clientID,
        this.#mutatorRegistry,
        this.#subscriptions,
        () => this.closed,
        FormatVersion.Latest,
        this.#zero,
      );
    } catch (e) {
      if (e instanceof ClientStateNotFoundError) {
        this.#clientStateNotFoundOnClient(clientID);
      } else if (this.#closed) {
        this.#lc.debug?.('Exception refreshing during close', e);
      } else {
        throw e;
      }
    }
    if (refreshResult !== undefined) {
      await this.#subscriptions.fire(refreshResult.diffs);
    }
  }

  #fireOnClientStateNotFound() {
    this.onClientStateNotFound?.();
  }

  #clientStateNotFoundOnClient(clientID: ClientID) {
    this.#lc.error?.(`Client state not found on client, clientID: ${clientID}`);
    this.#fireOnClientStateNotFound();
  }

  async #clientStateNotFoundOnServer() {
    const clientGroupID = await this.#clientGroupIDPromise;
    this.#lc.error?.(
      `Client state not found on server, clientGroupID: ${clientGroupID}`,
    );
    await this.disableClientGroup();
    this.#fireOnClientStateNotFound();
  }

  async disableClientGroup(): Promise<void> {
    const clientGroupID = await this.#clientGroupIDPromise;
    assert(clientGroupID, 'Expected clientGroupID to be defined');
    this.isClientGroupDisabled = true;
    await withWrite(this.perdag, dagWrite =>
      disableClientGroup(clientGroupID, dagWrite),
    );
  }

  #fireOnUpdateNeeded(reason: UpdateNeededReason) {
    this.#lc.debug?.(`Update needed, reason: ${reason}`);
    this.onUpdateNeeded?.(reason);
  }

  async #schedulePersist(): Promise<void> {
    if (!this.#enableScheduledPersist) {
      return;
    }
    await this.#schedule('persist', this.#persistScheduler);
  }

  async #handlePersist(persistInfo: PersistInfo): Promise<void> {
    this.#lc.debug?.('Handling persist', persistInfo);
    const clientGroupID = await this.#clientGroupIDPromise;
    if (persistInfo.clientGroupID === clientGroupID) {
      void this.#scheduleRefresh();
    }
  }

  async #scheduleRefresh(): Promise<void> {
    if (!this.#enableScheduledRefresh) {
      return;
    }
    await this.#schedule('refresh from storage', this.#refreshScheduler);
  }

  async #schedule(name: string, scheduler: ProcessScheduler): Promise<void> {
    try {
      await scheduler.schedule();
    } catch (e) {
      if (e instanceof AbortError) {
        this.#lc.debug?.(`Scheduled ${name} did not complete before close.`);
      } else {
        this.#lc.error?.(`Error during ${name}`, e);
      }
    }
  }

  /**
   * Runs a refresh as soon as possible through the refresh scheduler.
   */
  runRefresh(): Promise<void> {
    return this.#refreshScheduler.run();
  }

  watch(prefix: string, onDiff: (diff: NoIndexDiff) => void): void {
    this.#subscriptions.add(prefix, onDiff);
  }

  /**
   * Query is used for read transactions. It is recommended to use transactions
   * to ensure you get a consistent view across multiple calls to `get`, `has`
   * and `scan`.
   */
  query<R>(body: (tx: ReadTransaction) => Promise<R> | R): Promise<R> {
    return this.#queryInternal(body);
  }

  get cookie(): Promise<Cookie> {
    return this.#ready.then(() =>
      withRead(this.memdag, async dagRead => {
        const mainHeadHash = await dagRead.getHead(DEFAULT_HEAD_NAME);
        if (!mainHeadHash) {
          throw new Error('Internal no main head found');
        }
        const baseSnapshot = await baseSnapshotFromHash(mainHeadHash, dagRead);
        const baseSnapshotMeta = baseSnapshot.meta;
        const cookie = baseSnapshotMeta.cookieJSON;
        assertCookie(cookie);
        return cookie;
      }),
    );
  }

  #queryInternal: QueryInternal = async body => {
    await this.#ready;
    const {clientID} = this;
    return withRead(this.memdag, async dagRead => {
      try {
        const dbRead = await readFromDefaultHead(dagRead, FormatVersion.Latest);
        const tx = new ReadTransactionImpl(clientID, dbRead, this.#lc);
        return await body(tx);
      } catch (ex) {
        throw await this.#convertToClientStateNotFoundError(ex);
      }
    });
  };

  #register<Return extends ReadonlyJSONValue | void, Args extends JSONValue>(
    name: string,
    mutatorImpl: (tx: WriteTransaction, args?: Args) => MaybePromise<Return>,
  ): (
    args?: Args,
  ) => Promise<Return> | {client: Promise<Return>; server: Promise<unknown>} {
    this.#mutatorRegistry[name] = mutatorImpl as (
      tx: WriteTransaction,
      args: JSONValue | undefined,
    ) => Promise<void | JSONValue>;

    return (
      args?: Args,
    ):
      | Promise<Return>
      | {client: Promise<Return>; server: Promise<unknown>} => {
      // DO NOT track CRUD mutations as they do not receive responses from
      // the server.
      const trackingData =
        name === '_zero_crud' ? undefined : this.#zero?.trackMutation();

      const result = this.#mutate(
        trackingData,
        name,
        mutatorImpl,
        args,
        performance.now(),
      );

      if (trackingData) {
        return {
          client: result,
          server: trackingData.serverPromise,
        };
      }

      return result;
    };
  }

  #registerMutators<
    M extends {
      [key: string]: (
        tx: WriteTransaction,
        args?: ReadonlyJSONValue,
      ) => MutatorReturn;
    },
  >(regs: M): MakeMutators<M> {
    type Mut = MakeMutators<M>;
    const rv: Partial<Mut> = Object.create(null);
    for (const k in regs) {
      rv[k] = this.#register(k, regs[k]) as MakeMutator<M[typeof k]>;
    }
    return rv as Mut;
  }

  async #mutate<
    R extends ReadonlyJSONValue | void,
    A extends ReadonlyJSONValue,
  >(
    trackingData: MutationTrackingData | undefined,
    name: string,
    mutatorImpl: (tx: WriteTransaction, args?: A) => MaybePromise<R>,
    args: A | undefined,
    timestamp: number,
  ): Promise<R> {
    const frozenArgs = deepFreeze(args ?? null);

    // Ensure that we run initial pending subscribe functions before starting a
    // write transaction.
    if (this.#subscriptions.hasPendingSubscriptionRuns) {
      await Promise.resolve();
    }

    await this.#ready;
    const {clientID} = this;
    return withWriteNoImplicitCommit(this.memdag, async dagWrite => {
      try {
        let result: R;
        let newHead: Hash;
        let diffs: DiffsMap;
        let headHash: Hash;
        try {
          headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
          const originalHash = null;

          const dbWrite = await newWriteLocal(
            headHash,
            name,
            frozenArgs,
            originalHash,
            dagWrite,
            timestamp,
            clientID,
            FormatVersion.Latest,
          );

          const mutationID = await dbWrite.getMutationID();
          const tx = new WriteTransactionImpl(
            clientID,
            mutationID,
            'initial',
            await this.#zero?.getTxData(headHash, {
              openLazyRead: dagWrite,
            }),
            dbWrite,
            this.#lc,
          );

          if (trackingData) {
            this.#zero?.mutationIDAssigned(
              trackingData.ephemeralID,
              mutationID,
            );
          }

          result = await mutatorImpl(tx, args);

          throwIfClosed(dbWrite);
          const lastMutationID = await dbWrite.getMutationID();
          [newHead, diffs] = await dbWrite.commitWithDiffs(
            DEFAULT_HEAD_NAME,
            this.#subscriptions,
          );

          // Update this after the commit in case the commit fails.
          this.lastMutationID = lastMutationID;
        } catch (e) {
          // If we threw before we could persist the mutation
          // then we need to reject the mutation.
          if (trackingData) {
            this.#zero?.rejectMutation(trackingData.ephemeralID, e);
          }
          throw e;
        }

        this.#zero?.advance(headHash, newHead, diffs.get('') ?? []);

        // Send is not supposed to reject
        this.#pushConnectionLoop.send().catch(() => void 0);
        await this.#subscriptions.fire(diffs);
        void this.#schedulePersist();
        return result;
      } catch (ex) {
        throw await this.#convertToClientStateNotFoundError(ex);
      }
    });
  }

  /**
   * In the case we get a ChunkNotFoundError we check if the client got garbage
   * collected and if so change the error to a ClientStateNotFoundError instead
   */
  async #convertToClientStateNotFoundError(ex: unknown): Promise<unknown> {
    if (
      ex instanceof ChunkNotFoundError &&
      (await this.#checkForClientStateNotFoundAndCallHandler())
    ) {
      return new ClientStateNotFoundError(this.clientID);
    }

    return ex;
  }

  recoverMutations(): Promise<boolean> {
    return this.#mutationRecovery.recoverMutations(
      this.#ready,
      this.perdag,
      this.#idbDatabase,
      this.#idbDatabases,
      this.#kvStoreProvider.create,
    );
  }

  /**
   * List of pending mutations. The order of this is from oldest to newest.
   *
   * Gives a list of local mutations that have `mutationID` >
   * `syncHead.mutationID` that exists on the main client group.
   *
   * @experimental This method is experimental and may change in the future.
   */
  experimentalPendingMutations(): Promise<readonly PendingMutation[]> {
    return withRead(this.memdag, pendingMutationsForAPI);
  }
}

// This map is used to keep track of closing instances of Replicache. When an
// instance is opening we wait for any currently closing instances.
const closingInstances: Map<string, Promise<unknown>> = new Map();
const singleProcessClients = new Set<ClientID>();

async function throwIfError(p: Promise<undefined | {error: unknown}>) {
  const res = await p;
  if (res) {
    throw res.error;
  }
}

function reload(): void {
  if (typeof location !== 'undefined') {
    location.reload();
  }
}

function validateOptions<MD extends MutatorDefs>(
  options: ReplicacheOptions<MD>,
): void {
  const {name, clientMaxAgeMs} = options;
  if (typeof name !== 'string' || !name) {
    throw new TypeError('name is required and must be non-empty');
  }

  if (clientMaxAgeMs !== undefined) {
    const min = Math.max(GC_INTERVAL, HEARTBEAT_INTERVAL);
    if (typeof clientMaxAgeMs !== 'number' || clientMaxAgeMs <= min) {
      throw new TypeError(
        `clientAgeMaxMs must be a number larger than ${min}ms`,
      );
    }
  }
}
