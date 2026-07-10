# Rust sync incident runbook

This runbook covers the Rust native and Cloudflare Durable Object sync hosts.
Production cutover, rollback, or namespace routing changes still require the
normal operator approval. Diagnostic requests must use the authenticated
operator endpoint; never paste admin keys, bearer tokens, query arguments, or
row contents into an incident log.

## First response

1. Identify one affected namespace and hash it before recording it. Use the
   `namespaceHash` already present in `sync_request` events rather than the raw
   namespace.
2. Record host version, engine version, request kind, result class, input and
   output cookie, retained floor, watermark, transaction latency, and total
   latency from the structured event.
3. Read authenticated `/admin/status` for the namespace. Capture the engine
   floor/watermark and aggregate counters, database size, boot ID, hibernation
   count, and connected wake socket count. Do not use `/admin/sql` until the
   narrower evidence is insufficient.
4. Stop any automated rollout if invariant failures, unexpected resets, or
   mutation ordering errors are increasing. Keep exactly one writer enabled
   for the namespace.

## Cookie or reset failures

Expected invariant: cookies are monotone and no client can advance past the
namespace watermark. A 409 is valid only when the client cookie is older than
the retained floor, invalidated by an operator drill, or otherwise cannot be
represented safely.

Check:

- compare `inputCookie`, `outputCookie`, `retainedFloor`, and
  `currentWatermark` on the failing pull;
- inspect the aggregate `resets` counter and the structured `resetReason`;
- verify the client did not switch namespace, client group, or host route;
- verify counters were not rounded through JavaScript. Persisted counters above
  `Number.MAX_SAFE_INTEGER` must fail loud rather than emit a rounded number;
- reproduce with one authenticated pull before requesting a full client reset.

If the input cookie is below the floor, a snapshot reset is expected. If it is
at or above the floor, preserve the request metadata and database backup before
restarting or invalidating anything; this is an engine invariant incident.

## Retention failures

Expected invariant: pruning advances the retained floor without deleting
changes needed by active or permitted offline clients.

Check:

- graph floor and watermark over time and compare them with `retentionRuns`;
- confirm the configured retention window for the affected namespace;
- identify the oldest offline client cookie without recording client data;
- check database size and change-row growth;
- run the retention-pressure harness against a test namespace with the same
  retention setting.

A client below the floor must receive a reset/snapshot, not an incomplete
incremental patch. A floor that passes an eligible cookie, moves backward, or
fails to advance under sustained writes is an invariant failure. Pause rollout
and preserve a backup.

## Mutation ordering failures

Expected invariant: mutation IDs advance once, in order, per client. A replay
returns the stored result and does not reapply application writes or external
effects.

Check:

- compare the mutation ID with the stored LMID and the `lmidAdvances` event
  field;
- distinguish an application error response from a transport failure;
- inspect `applicationErrors`, `invariantFailures`, and
  `externalEffectFailures` counters;
- verify deferred effects were staged in the durable outbox or registered with
  `context.defer`, and never executed before commit;
- reproduce with the lost-push-response lane. The retry must converge without
  duplicate rows or duplicate outbox entries.

An LMID gap, duplicate application write, or effect executed from a rolled-back
transaction is a stop-the-writer incident. Disable the new writer before any
route rollback; only then restore the old route.

## Query-membership failures

Expected invariant: only server-resolved named query ASTs reach the engine.
Membership is scoped by client group and query hash, permission transforms are
versioned by the server, and rows leave the client store when no desired query
retains them.

Check:

- confirm raw client-authored AST puts and unknown names return HTTP 400;
- compare `queriesRecomputed`, `queryRecompilations`, row puts/deletes, and the
  query acknowledgement version;
- compare boot IDs around the first missing acknowledgement and verify the
  query-aware, visibility, and retention controls still read their persisted
  values. These controls must live in `_zsync_host_control`; an instance-local
  override reverting after eviction produces repeated `{cookie, unchanged}`
  pulls even while the client keeps sending desired-query patches;
- verify the query transform version stored for the affected group/hash;
- test permission expansion and contraction separately;
- run the named-query lifecycle and stock-Zero differential lanes with the
  exact committed host and engine versions.

An unauthorized row is a security incident: disable the writer and query-aware
route for that test/canary namespace, preserve the namespace backup, and do not
log the row. A missing row with a valid query is a correctness incident; capture
only hashes, versions, counters, and canonical row counts.

## Wake and propagation failures

Wake frames are hints, not data. Check `wakeBatches`, `wakeFrames`, connected
sockets, and commit-to-seen latency. A wake arriving during a pull must schedule
another pull. If propagation falls back to the safety poll, separate server
commit latency from commit-to-seen latency before changing coalescing or idle
teardown settings.

## Backup, restore, and rollback

Before a destructive diagnostic or rollback:

1. stop the new writer for the namespace;
2. prove pushes to it are rejected;
3. capture a current backup plus host/engine/schema versions;
4. restore into an isolated test namespace;
5. verify watermark, retained floor, mutation ordering, query state, and health
   counters;
6. only then restore the old route or enable another writer.

Never enable the old and new writers concurrently. A rollback that restores a
route before stopping the new writer is invalid even if the data later
converges.

## Escalation evidence

Attach only:

- committed source SHA and deployed version ID;
- hashed namespace, boot ID, engine state, counters, and database size;
- redacted structured event fields listed above;
- exact harness command, budget, and pass/fail output;
- backup identifier and drill namespace.

Do not attach HTTP bodies, row patches, named-query arguments, authorization
headers, admin keys, or raw namespace identifiers.
