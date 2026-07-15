# Production-shape Zero write profile

`production-shape.ts` runs the real Cloudflare Zero embed in local workerd with
separate SQLite Durable Objects for the upstream SQL store and Zero's replica.
It measures every SQL `rowsWritten` cursor delta through
`trackSqlCursorRowsWritten`, using the same workerd storage API and Zero CF
overlay as a deployment. [Cloudflare defines a cursor's final
`rowsWritten`](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#exec)
as the value used for SQL billing, including index updates and deletes. The
harness therefore reports billable SQL rows rather than logical application
rows.

The deterministic fixture has 51 published tables, 4,663 rows, and 176 indexes.
The row distribution matches the production backup observed during the July
2026 Chat incident. The run performs a clean initial sync, then a second clean
store whose ready deadline is forced to expire after the full initial sync and
which is retried on the same Durable Object.

Run it with:

```sh
bun run perf:write-profile
```

The final JSON separates `source` and `cache`, then ranks routes, statements,
and target tables for every phase.

## July 2026 result

| Phase                                               | Source rows | Cache rows | Result                  |
| --------------------------------------------------- | ----------: | ---------: | ----------------------- |
| Clean initial sync, old all-table rollback fallback |      10,888 |     25,811 | ready in 633 ms         |
| Clean initial sync, targeted rollback snapshots     |         890 |     25,811 | ready in 608 ms         |
| Forced 250 ms ready timeout, targeted snapshots     |         890 |     25,811 | timed out after cleanup |
| Retry on the timed-out store                        |         375 |      2,550 | ready in 116 ms         |

In the controlled clean sync, the source multiplier was the rollback guard for
implicit trigger and foreign-key writes. Two Zero metadata writes caused that
guard to copy every published table into `_orez_tx_*` snapshots. Those copies
accounted for 10,714 of 10,888 source rows in the original clean run. Following
only the transitive trigger and foreign-key targets reduces source writes by
91.8% while retaining the all-table fallback for trigger SQL the parser cannot
understand.

The cache total is dominated by required replica materialization. In this
fixture, inserting the 4,663 rows and building the 176 indexes accounts for
23,204 rows before Zero's own schema and transaction bookkeeping. A retry loop
therefore pays roughly 25,800 cache rows every time it discards and recreates a
replica. The generation cleanup in commit `e197416` makes a forced timeout stop
before its retry starts; the measured retry reused the completed durable
replica and paid 2,550 rows instead of another 25,811-row initial sync.

## Incident accounting boundary

This harness establishes the cost and statement mix of one healthy
initialization. It does not, by itself, attribute the entire production spike.
The incident window recorded 514,346 source rows and 486,876 cache rows. Those
totals equal 47.2 of the measured old source initializations but only 18.9 cache
initializations. One old clean initialization was 36,699 SQL rows across both
objects. The unequal per-object multipliers prove that a single repeated clean
initialization pattern cannot explain the full window.

The forced-timeout run after the fix, including its retry, measured 1,265 source
rows and 28,361 cache rows. It covers embed shutdown and restart on one durable
replica. It does not run Chat's production wrapper, whose boot path can apply
schema migrations, wipe a replica after a schema-tag change, repair a partial
replica or poisoned change log, clear retained change-streamer state, and then
start another initial sync. The healthy fixture also does not reproduce the
internal change-streamer crash loop documented by the replica-rank repair.
Application writes and those production repair/restart cycles remain unmetered
in this profile.

The SQL totals are comparable to Cloudflare billing because they use the
runtime's billing counter. The harness does not meter Durable Object key-value
methods or alarms. The production wrapper uses those for small tags, instance
names, and boot alarms. Each is billable separately, and this profile does not
count them.

Local workerd does not expose the browser `MessageChannel` global used by the
in-isolate postgres bridge, and its timer host function must retain its global
receiver. The harness supplies a same-isolate queued port pair and a bound
timer. All SQL execution, Durable Object persistence, Zero startup, initial
sync, timeout, and retry behavior still runs inside workerd.
