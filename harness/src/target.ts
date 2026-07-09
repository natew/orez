import type { Schema, mutators } from './fixture.js'
// the one abstraction the whole harness hangs off: a SyncTarget is a running
// sync stack the harness can point stock zero clients at, write to upstream
// directly, and read fresh oracle answers from. three implementations planned
// (stock-zero, orez-local sqlite, orez-cf); see plans/zero-conformance-harness.md.
import type { Zero } from '@rocicorp/zero'

export type Rows = Record<string, unknown>[]

export type FixtureZero = Zero<Schema, typeof mutators>

export type SyncTarget = {
  readonly name: string

  // a stock @rocicorp/zero client wired for this target (server url +
  // transport differ per target; the client code never does)
  createClient(userID: string): FixtureZero

  // upstream write straight to the authoritative store, bypassing sync —
  // exercises the replication path
  sql(query: string): Promise<Rows>

  // fresh read from the authoritative store: the oracle every synced client
  // result is compared against
  oracle(query: string): Promise<Rows>

  metrics(): Promise<{ serverRssMb?: number }>

  close(): Promise<void>
}
