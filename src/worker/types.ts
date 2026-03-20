/**
 * orez/worker types.
 *
 * interfaces for the embeddable orez worker that runs PGlite + change
 * tracking without Node.js dependencies. designed for CF Workers/DO
 * but usable in any JS runtime (browser, vitest, bun, deno).
 */

import type { Mutex } from '../mutex.js'
import type { ChangeRecord } from '../replication/change-tracker.js'
import type { ReplicationWriter } from '../replication/handler.js'
import type { PGlite, PGliteOptions, Results } from '@electric-sql/pglite'

/** options for creating an orez worker */
export interface OrezWorkerOptions {
  /**
   * pre-created PGlite instance. when provided, orez wraps it
   * without managing its lifecycle (caller is responsible for closing).
   */
  pglite?: PGlite

  /**
   * PGlite constructor options. used when `pglite` is not provided.
   * in CF Workers, pass wasmModule/fsBundle/loadDataDir here.
   */
  pgliteOptions?: PGliteOptions

  /** publication names to track. defaults to all public tables. */
  publications?: string[]

  /** log level (default: 'warn') */
  logLevel?: 'error' | 'warn' | 'info' | 'debug'
}

/** the orez worker instance */
export interface OrezWorker {
  /** the underlying PGlite instance */
  readonly db: PGlite

  /** mutex for serializing PGlite access */
  readonly mutex: Mutex

  /** run a parameterized query */
  query<T extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: unknown[]
  ): Promise<Results<T>>

  /** execute raw SQL (DDL, multi-statement) */
  exec(sql: string): Promise<void>

  /** install/reinstall change tracking triggers on all published tables */
  installChangeTracking(): Promise<void>

  /** get changes since a watermark */
  getChangesSince(watermark: number, limit?: number): Promise<ChangeRecord[]>

  /** get current watermark value */
  getCurrentWatermark(): Promise<number>

  /** purge consumed changes up to watermark */
  purgeChanges(watermark: number): Promise<number>

  /**
   * start streaming replication to a writer.
   * runs until the writer is closed or the worker is shut down.
   */
  startReplication(writer: ReplicationWriter): Promise<void>

  /** whether this worker owns the PGlite instance (manages its lifecycle) */
  readonly ownsInstance: boolean

  /** close the worker. if ownsInstance, also closes PGlite. */
  close(): Promise<void>
}
