import {compareUTF8} from 'compare-utf8';
import {binarySearch} from '../../../shared/src/binary-search.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import {diff as btreeDiff} from '../btree/diff.ts';
import type {InternalDiff} from '../btree/node.ts';
import {BTreeRead} from '../btree/read.ts';
import type {Read} from '../dag/store.ts';
import type {Commit} from '../db/commit.ts';
import {commitFromHash, type Meta} from '../db/commit.ts';
import type * as FormatVersion from '../format-version-enum.ts';
import type {Hash} from '../hash.ts';

type FormatVersion = Enum<typeof FormatVersion>;

/**
 * Interface allowing different diff functions to skip costly diff computations.
 */
export interface DiffComputationConfig {
  shouldComputeDiffs(): boolean;
}

/**
 * The diffs in different indexes. The key of the map is the index name.
 * "" is used for the primary index.
 */
export class DiffsMap extends Map<string, InternalDiff> {
  override set(key: string, value: InternalDiff): this {
    if (value.length === 0) {
      return this;
    }
    return super.set(key, value);
  }
}

/**
 * Diffs the primary index at two different commit hashes.
 */
export async function diff(
  oldHash: Hash,
  newHash: Hash,
  read: Read,
  diffConfig: DiffComputationConfig,
  formatVersion: FormatVersion,
): Promise<DiffsMap> {
  const [oldCommit, newCommit] = await Promise.all([
    commitFromHash(oldHash, read),
    commitFromHash(newHash, read),
  ]);

  return diffCommits(oldCommit, newCommit, read, diffConfig, formatVersion);
}

/**
 * Diffs the primary index at two different commits.
 */
// TODO: this should probably move to db/
export async function diffCommits(
  oldCommit: Commit<Meta>,
  newCommit: Commit<Meta>,
  read: Read,
  diffConfig: DiffComputationConfig,
  formatVersion: FormatVersion,
): Promise<DiffsMap> {
  const diffsMap = new DiffsMap();
  if (!diffConfig.shouldComputeDiffs()) {
    return diffsMap;
  }

  const oldMap = new BTreeRead(read, formatVersion, oldCommit.valueHash);
  const newMap = new BTreeRead(read, formatVersion, newCommit.valueHash);
  const valueDiff = await btreeDiff(oldMap, newMap);
  diffsMap.set('', valueDiff);

  return diffsMap;
}

export function diffBinarySearch(
  diff: readonly {readonly key: string}[],
  prefix: string,
): number {
  return binarySearch(diff.length, i => compareUTF8(prefix, diff[i].key));
}
