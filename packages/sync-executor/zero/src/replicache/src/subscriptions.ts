import type {LogContext} from '@rocicorp/logger';
import {promiseVoid} from '../../shared/src/resolved-promises.ts';
import type {InternalDiff, NoIndexDiff} from './btree/node.ts';
import {type DiffComputationConfig, type DiffsMap, diffBinarySearch} from './sync/diff.ts';
import type {QueryInternal} from './types.ts';

type Watch = {
  prefix: string;
  onDiff: (diff: NoIndexDiff) => void;
};

export class SubscriptionsManager implements DiffComputationConfig {
  readonly #watches = new Set<Watch>();
  readonly #pending = new Set<Watch>();
  readonly #query: QueryInternal;
  readonly #lc: LogContext;
  readonly #signal: AbortSignal;

  constructor(query: QueryInternal, lc: LogContext, signal: AbortSignal) {
    this.#query = query;
    this.#lc = lc;
    this.#signal = signal;
  }

  get hasPendingSubscriptionRuns(): boolean {
    return this.#pending.size > 0;
  }

  add(prefix: string, onDiff: Watch['onDiff']): void {
    const watch = {prefix, onDiff};
    this.#watches.add(watch);
    this.#pending.add(watch);
    if (this.#pending.size === 1) {
      void this.#runInitialWatches();
    }
  }

  async #runInitialWatches(): Promise<void> {
    await Promise.resolve();
    const watches = [...this.#pending];
    this.#pending.clear();
    await this.#fire(watches);
  }

  fire(diffs: DiffsMap): Promise<void> {
    const diff = diffs.get('');
    if (!diff) return promiseVoid;

    const watches = [...this.#watches].filter(({prefix}) => {
      const start = diffBinarySearch(diff, prefix);
      return diff[start]?.key.startsWith(prefix);
    });
    return this.#fire(watches, diff);
  }

  async #fire(watches: readonly Watch[], diff?: InternalDiff): Promise<void> {
    if (this.#signal.aborted || watches.length === 0) return;

    const results = await this.#query(tx =>
      Promise.allSettled(
        watches.map(async ({prefix}): Promise<NoIndexDiff> => {
          const result = [];
          if (diff) {
            for (let i = diffBinarySearch(diff, prefix); i < diff.length; i++) {
              const operation = diff[i];
              if (!operation.key.startsWith(prefix)) break;
              result.push({...operation});
            }
          } else {
            for await (const [key, newValue] of tx.scan({prefix}).entries()) {
              result.push({op: 'add' as const, key, newValue});
            }
          }
          return result;
        }),
      ),
    );

    for (let i = 0; i < watches.length; i++) {
      const result = results[i];
      if (result.status === 'fulfilled') {
        watches[i].onDiff(result.value);
      } else {
        this.#lc.error?.('Error in subscription body:', result.reason);
      }
    }
  }

  clear(): void {
    this.#watches.clear();
  }

  shouldComputeDiffs(): boolean {
    return this.#watches.size > 0;
  }
}
