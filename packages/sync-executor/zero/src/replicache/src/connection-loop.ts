import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {DocumentVisibilityWatcher} from '../../shared/src/document-visible.ts';
import {sleep} from '../../shared/src/sleep.ts';

export class ConnectionLoop {
  // Controls whether the next iteration of the loop will wait at the pending
  // state.
  #pendingResolver = resolver<void>();

  /**
   * Resolver for the next send. Never rejects. Returns an error instead since
   * this resolver is used in cases where they might not be someone waiting,
   * and we don't want an unhandled promise rejection in that case.
   */
  #sendResolver = resolver<undefined | {error: unknown}>();

  readonly #invokeSend: () => Promise<boolean>;
  readonly #watchdogTimer: number | null;
  #closed = false;
  #abortSignal = new AbortController();

  /**
   * Number of pending send calls.
   *
   * We keep track of this because if close happens while we are waiting for the
   * send to resolve we should reject the send promise.
   */
  #sendCounter = 0;
  readonly #lc: LogContext;
  readonly #visibilityWatcher: DocumentVisibilityWatcher | undefined;

  constructor(
    lc: LogContext,
    invokeSend: () => Promise<boolean>,
    watchdogTimer: number | null,
    visibilityWatcher?: DocumentVisibilityWatcher,
  ) {
    this.#lc = lc;
    this.#invokeSend = invokeSend;
    this.#watchdogTimer = watchdogTimer;
    this.#visibilityWatcher = visibilityWatcher;
    void this.run();
  }

  close(): void {
    this.#closed = true;
    this.#abortSignal.abort();
    if (this.#sendCounter > 0) {
      this.#sendResolver.resolve({error: closeError()});
    }
  }

  /**
   *
   * @returns Returns undefined if ok, otherwise it return the error that caused
   * the send to fail.
   */
  async send(): Promise<undefined | {error: unknown}> {
    if (this.#closed) {
      return {error: closeError()};
    }
    this.#sendCounter++;
    this.#lc.debug?.('send', false);
    await this.#visibilityWatcher?.waitForVisible();

    this.#pendingResolver.resolve();

    const result = await this.#sendResolver.promise;
    this.#sendCounter--;
    return result;
  }

  async run(): Promise<void> {
    let lastSendFailed = false;
    let sending = false;
    const lc = this.#lc;
    lc.debug?.('Starting connection loop');

    while (!this.#closed) {
      lc.debug?.(
        lastSendFailed
          ? 'Last request failed. Trying again'
          : 'Waiting for a send',
      );

      // Wait until send is called or until the watchdog timer fires.
      const races = [this.#pendingResolver.promise];
      const t = this.#watchdogTimer;
      if (t !== null) {
        // Wait for the watchdog timer to fire or the abort signal to be triggered.
        races.push(sleep(t, this.#abortSignal.signal).catch(() => {}));
      }
      await Promise.race(races);
      if (this.#closed) break;

      lc.debug?.('Waiting for debounce');
      // keep the debounce microtask boundary so queued sends can coalesce.
      await Promise.resolve().then(() => undefined);
      if (this.#closed) break;
      lc.debug?.('debounced');

      // This resolver is used to wait for incoming push calls.
      this.#pendingResolver = resolver();

      if (sending) {
        lc.debug?.('Too many request in flight. Waiting until one finishes...');
        await this.#waitUntilAvailableConnection();
        if (this.#closed) break;
        lc.debug?.('...finished');
      }

      sending = true;
      void (async () => {
        const start = Date.now();
        let ok: boolean;
        let error: unknown;
        try {
          lc.debug?.('Sending request');
          ok = await this.#invokeSend();
          lc.debug?.('Send returned', ok);
        } catch (e) {
          lc.debug?.('Send failed', e);
          error = e;
          ok = false;
        }
        if (this.#closed) {
          lc.debug?.('Closed after invokeSend');
          return;
        }
        lc.debug?.('Request done', {duration: Date.now() - start, ok});
        lastSendFailed = !ok;
        sending = false;
        this.#connectionAvailable();
        const sendResolver = this.#sendResolver;
        this.#sendResolver = resolver();
        if (error) {
          sendResolver.resolve({error});
        } else {
          sendResolver.resolve(undefined);
        }
        if (!ok) {
          // Keep trying
          this.#pendingResolver.resolve();
        }
      })();
    }
  }

  #waitingConnectionResolve: (() => void) | undefined = undefined;

  #connectionAvailable() {
    if (this.#waitingConnectionResolve) {
      const resolve = this.#waitingConnectionResolve;
      this.#waitingConnectionResolve = undefined;
      resolve();
    }
  }

  #waitUntilAvailableConnection() {
    const {promise, resolve} = resolver();
    this.#waitingConnectionResolve = resolve;
    return promise;
  }
}

function closeError() {
  return new Error('Closed');
}
