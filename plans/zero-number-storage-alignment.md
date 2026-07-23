# Align orez timestamp storage with Zero's number model

Status: proposal, needs Nate's go-ahead. Written 2026-07-23 by p1570 after the
Soot prod timestamp incident. The companion mount fix (decode/throw in
`toZeroValue`, `src/zero-http/mount.ts`) is committed on this branch and is
safe to land independently; this document scopes the deeper storage change.

## The divergence

Zero has no date type. Its SQLite replica stores the upstream pg TYPENAME
as-is in the column definition (`zero-cache/src/types/lite.ts`:
"pgDataType values are stored as-is in the SQLite column defs", e.g.
`timestamptz|NOT_NULL`), which under sqlite affinity rules is NUMERIC, and it
stores epoch-millisecond NUMBERS, converted once at the replication boundary
(`timestampToFpMillis` in `zero-cache/src/types/pg.ts`, which throws on
unparseable input). A Zero client can never receive a string for a number
column.

Orez's pg proxy instead maps `timestamptz` to sqlite `text`
(`src/pg-proxy-do-backend.ts` type map) and stores pg timestamp TEXT. Every
read boundary then needs its own decoder: the Rust engine's
`timestamp_text_to_epoch_ms` (`crates/sync-core/src/value.rs`), the cf-do
`normalizeRow` NaN-passthrough, and (until this branch) the zero-http mount,
which forwarded raw strings and let them detonate in the client's
`compareValues`.

The text DDL also interacts badly with Cloudflare: workerd's DO SQL API binds
every JS number as a double (`workerd src/workerd/api/sql.h` — BindingValue is
bytes|String|double, no int64 path), and sqlite renders an integral REAL
stored into a TEXT-affinity column as decimal text (`1784788681197.0`). That
minted unreadable timestamp cells in Soot prod continuously until Soot rebuilt
its columns to NUMERIC affinity.

## The alignment

Store what Zero stores:

1. DDL: map pg `timestamp`/`timestamptz` (and `date`/`time` if we keep pg
   parity there) to a column whose declared type keeps the upstream typename
   (NUMERIC affinity), exactly like Zero's replica.
2. Writes: convert pg timestamp text/Date params to epoch-ms numbers at the
   pg-proxy write boundary (the inverse of today's `postgresTimestampText*`
   helpers), so the stored value is a number.
3. pg-wire reads: render numbers back to pg timestamp text only when a client
   speaks the postgres protocol/SQL surface — mirroring Zero's
   `serializeTimestamp`, which formats numbers to pg text on the way OUT.
4. Then delete the per-boundary decoders: Rust
   `timestamp_text_to_epoch_ms` call sites for declared-number columns, cf-do
   `normalizeRow`'s NaN-passthrough branch, and (long-term) the mount decode
   added on this branch becomes a strict assert only.

## Migration story

Existing orez deployments hold pg-text cells in text columns. The change needs
a per-table rebuild (sqlite cannot alter a column type) plus a value
conversion, sequenced the way Soot did it in July 2026:
delete-oversize-capture rows if the deployment captures full rows, convert
values, rebuild columns to NUMERIC affinity. Soot's
`20260723120000_rebuild_synced_timestamp_column_storage` is a working
reference including the foreign-key and trigger constraints (never DROP TABLE
a parent under enforced FKs; column swap instead).

## Cost and risk

- pg-proxy read/write paths and the datetime compiler passes assume text
  storage in several places; this is a multi-session change with conformance
  tests (`src/pg-sqlite-compiler/test/datetime.oracle.test.ts`) as the safety
  net.
- Any orez release ships the behavior to every consumer; existing deployments
  must run the migration before upgrading the read paths.
- Do not start without explicit approval; the mount fix on this branch already
  removes the acute client-crash class.
