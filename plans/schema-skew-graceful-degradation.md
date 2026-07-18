# Graceful schema skew for Zero clients

## Recommendation

Treat the schema fingerprint as a diagnostic identity, not an admission token.
Mount Zero whenever the endpoint is valid and let Zero's existing server-side
schema check decide whether the client schema is structurally compatible.

Use these compatibility outcomes:

1. `exact`: fingerprints match and sync runs normally.
2. `compatible-skew`: fingerprints differ, but Zero accepts the connection.
   Sync runs normally and the application shows a persistent update warning.
3. `incompatible`: Zero returns `SchemaVersionNotSupported`. The client group is
   already disabled by Zero. Keep the local database mounted for cached reads,
   stop writes, and show the server's concrete incompatibility message.

Track health and connectivity on a separate axis. A failed health probe does not
prove schema incompatibility, and a successful Zero connection is stronger
evidence that the service is usable.

This preserves strictness for breaking changes while allowing stale clients to
survive additive server deployments. It also prevents the current blank or
infinite-loading failure mode because a health probe never unmounts the local
query engine.

## Findings

The hard fingerprint gate is downstream, in Agentbus, rather than orez:

- `gui/features/agentbus/zeroClient.tsx` sets `disable=true` while the gate is
  checking, unavailable, or blocked.
- `gui/features/agentbus/zeroSchemaFingerprint.ts` treats every unequal hash as
  incompatible.
- `gui/features/agentbus/zeroSchemaGate.ts` maps that inequality to `blocked`.
- The server health response reports the schema hash and whether its publication
  contains the server's own expected tables and columns. It does not compare the
  client requirements with the server shape.

That gate rejected the stale mobile fingerprint `zsf1-a977fcea` against server
`zsf1-5b2957c9`, so no Zero client was created and no cached or live session rows
could be queried. Clearing local state cannot change a fingerprint compiled into
the application bundle.

The installed Zero 1.7.0 server already checks compatibility directionally in
`zero-cache/src/services/view-syncer/client-schema`. It requires every table,
column, type, and primary key declared by the client. Server-only tables and
columns are ignored. A direct runtime probe against the installed implementation
confirmed:

| Change | Zero result |
| --- | --- |
| exact schema | accepted |
| server adds a column and a table | accepted |
| client requires a missing server column | `SchemaVersionNotSupported` |
| shared column changes type | `SchemaVersionNotSupported` |
| shared table changes primary key | `SchemaVersionNotSupported` |

Recent Agentbus schema changes added optional server-projected fields such as
`session.latest_summary_status` and
`session.latest_summary_status_confidence`. An older client that does not declare
those columns is the compatible subset Zero is designed to accept. The exact-hash
preflight is stricter than the sync protocol it guards.

When Zero finds a real incompatibility, it disconnects and marks the Replicache
client group disabled, which prevents pulls and pushes. The application-provided
`onUpdateNeeded` callback currently deletes local state and reloads for
`SchemaVersionNotSupported`. That recovery is also unsuitable for graceful
degradation because it destroys the only useful cached view and cannot repair a
compiled schema mismatch.

## Proposed client flow

### Health is observational

Start the Zero client as soon as a valid server URL and authentication target
exist. Run the schema health request in parallel.

- A matching fingerprint records `exact`.
- A mismatching fingerprint records `skew-detected`, then waits for the Zero
  connection result.
- A failed health request records `health-unavailable` and never disables the
  client.
- An unhealthy publication is reported as a server fault and never unmounts the
  client's local database.

The health response remains valuable for build attribution, publication repair,
and a visible warning. It should not duplicate Zero's authoritative connection
check.

### Let the protocol classify compatibility

If the connection succeeds while fingerprints differ, promote the state to
`compatible-skew`. This is stronger evidence than a separately maintained hash
policy because the running Zero server checked the actual replicated schema
against the schema sent by this client.

If the server returns `SchemaVersionNotSupported`, record `incompatible` with the
server message. Do not delete the local database and do not reload. Zero has
already disabled pulls and pushes for that client group, so cached queries can
remain readable without syncing unsafe changes.

The UI should distinguish these states:

- `compatible-skew`: live data plus a warning such as "Server schema changed;
  update this app. Compatible fields are still syncing."
- `incompatible`: cached data with its last-sync time, disabled write controls,
  and the concrete missing table, column, type, or key error.
- `health-unavailable`: the live Zero connection decides whether data is current;
  the health diagnostic remains a separate warning.

An empty cache in `incompatible` state should render an explanatory empty state,
never a spinner that implies progress.

### Writes during skew

Table shape alone cannot prove that an older mutation still has the same
semantics. Server validators, permissions, and database constraints protect
against many malformed writes, but they cannot detect every semantic contract
change.

Use the following policy:

- `exact`: reads and writes enabled.
- `compatible-skew`: reads enabled. Writes may remain enabled only when the
  application explicitly declares its mutation contract backward-compatible.
  Otherwise use read-only mode.
- `incompatible`: new server writes disabled. Existing local state stays
  readable. Ordinary offline behavior remains governed by the application's
  existing Zero mutation policy.

For a general Zero client API, expose an explicit policy such as
`schemaSkew: 'strict' | 'read-only' | 'allow-compatible'`. The safe default is
`read-only`. `allow-compatible` should require server-side mutation validation
and visible operation errors. Agentbus's mutators are daemon-owned triggers and
its new columns are optional projections, so it can deliberately opt into writes
after verifying each mutator contract.

The core would need a pull-only control to make generic read-only mode airtight.
An application-level write gate is useful immediately, but it does not stop an
already queued mutation. Until Zero exposes separate pull and push control,
queued writes should reach the server validator and any rejection must be shown
to the user.

## Why field defaults are not the answer

Do not synthesize defaults for fields absent from the server. A missing value can
change filtering, ordering, permissions, and mutation meaning. Treat every
client-declared column as required by the sync contract even when its row value
is nullable or optional. This matches Zero's current check.

Extra fields from a newer server need no default. Older clients do not declare
or query them, and Zero already accepts that additive direction.

## Optional structural preflight

Some products may want to classify skew before opening a sync connection. In
that case, extend the health response with the canonical server schema signature,
including table names, column Zero types, candidate primary keys, nullability,
and default/generated metadata. Compare it directionally:

- every client table must exist;
- every client column must exist with the same Zero type;
- every client primary key must still be a valid non-null unique key;
- server-only tables and columns are read-compatible;
- a new non-null server column without a default is write-unsafe for direct CRUD.

The comparator should share implementation or fixtures with Zero's
`checkClientSchema`. A second independently maintained compatibility algorithm
will drift. The connection result should remain authoritative when the preflight
and server disagree.

## Rollout

### Phase 1: downstream safe fix

1. Remove schema health state from Agentbus's `ProvideZeroCore.disable`
   expression. Keep URL validity, fixtures, and explicit re-mint recovery as the
   only disable reasons.
2. Change an unequal fingerprint from `blocked` to `skew-detected`.
3. Treat a successful Zero connection under skew as `compatible-skew`.
4. Intercept `SchemaVersionNotSupported` in `onUpdateNeeded`: preserve local
   state, avoid reload, record `incompatible`, and disable write affordances.
5. Surface every non-exact state in diagnostics and the connection banner.
6. Add behavioral tests for additive skew, breaking skew, health failure, cache
   preservation, and write gating. Validate the mobile overview with a bundle
   carrying an older additive schema against the current server.

This phase needs no orez protocol change.

### Phase 2: reusable compatibility contract

1. Add a versioned structural signature to the on-zero health endpoint if early
   classification is still useful.
2. Move the directional comparator into a shared package or upstream Zero.
3. Add a pull-only mode before advertising generic read-only skew support.
4. Add a separate mutation-contract epoch or compatible range. Do not infer
   semantic mutation compatibility from table hashes.

## Rejected approaches

- Keeping exact fingerprint equality and adding cache resets: the compiled hash
  remains unchanged, so resets repeat the failure.
- Ignoring all schema errors: breaking column, type, and key changes must still
  stop sync.
- Supplying client defaults for missing server fields: this can produce plausible
  but incorrect rows and permission decisions.
- A hidden opt-in that suppresses errors: availability without a visible skew
  state makes stale results indistinguishable from current results.
- Building a query-by-query compatible subset first: named query arguments,
  permissions, relationships, and mutations also form contracts. The existing
  whole-client directional check gives a much safer first step with far less new
  machinery.

## Decision

Implement Phase 1 in Agentbus after review. Keep orez unchanged for now. The
evidence shows orez and Zero already accept the important additive case; the
downstream exact-hash admission gate and destructive mismatch recovery create the
outage. Consider Phase 2 only if more applications need a preflight API or true
pull-only operation.
