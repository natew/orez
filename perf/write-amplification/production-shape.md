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

The incident window recorded 514,346 source rows and 486,876 cache rows. The
direct embed profile establishes the cost and statement mix of one healthy
initialization. Its unequal per-object multipliers show that repeated clean
initialization alone cannot explain the window.

The forced-timeout run after the fix, including its retry, measured 1,265 source
rows and 28,361 cache rows. It covers embed shutdown and restart on one durable
replica. The wrapper profile below measures the remaining production boot
behavior.

## Chat data-worker wrapper profile

`chat-wrapper-production-shape.ts` builds and instruments Chat's current
`CLOUDFLARE_DO_SHIM_SOURCE`. It runs the real generated `ZeroSqlDO` and
`ZeroCacheDO` in local workerd, including Chat migrations, schema-tag reset,
partial replica repair, poisoned change-log repair, retained change-streamer
cleanup, NULL replica-rank repair, boot alarms, and retry backoff.

Run it from an Orez checkout with a current Chat checkout at `~/chat`:

```sh
bun run perf:write-wrapper-profile
```

Set `OREZ_CHAT_PROFILE_REPO` to profile a different Chat checkout. The report
records a SHA-256 hash of the generated Chat shim, so results remain tied to the
exact wrapper source. The July run used source hash
`f33b3333e13571cf77b50f5315217152838df5f72c8f56997b73cca38f2b9089`.

| Wrapper phase                                        | Source rows | Cache rows | Outcome                                     |
| ---------------------------------------------------- | ----------: | ---------: | ------------------------------------------- |
| First migration and clean boot                       |       4,212 |     25,821 | ready after one attempt                     |
| Intact replica restart                               |       1,494 |      2,559 | ready without materializing the replica     |
| Schema-tag reset                                     |       3,025 |     25,817 | repaired and ready                          |
| Partial replica repair                               |       3,025 |     25,817 | repaired and ready                          |
| Poisoned change-log repair                           |       3,025 |     25,817 | repaired and ready                          |
| Retained change-streamer clear, 4,663 retained rows  |       3,025 |     25,817 | cleared, repaired, and ready                |
| NULL replica-rank repair                             |           6 |          0 | rank replaced with a non-NULL timestamp     |
| Two forced boot failures, alarm recovery, clean boot |       4,212 |     25,821 | third alarm-carried attempt became ready    |
| Verified NULL rank with repair disabled              |       1,498 |      2,559 | current Zero 1.7.0 stack still became ready |

All four destructive repair paths converge on the same 3,025-source and
25,817-cache resync. Clearing 4,663 retained change-streamer rows did not raise
the local SQL billing cursor beyond that signature. An intact restart is much
smaller on the cache object. The two injected failures remained stopped until a
request or alarm initiated another attempt, honored the 15-second backoff after
the second failure, and added no measured SQL beyond the eventual clean boot.
The current wrapper therefore has a bounded retry path in this reproduction.

The wrapper's source writes during one destructive repair are dominated by
2,202 `_orez_tx_schema` rows and 768 `_chat_write_circuit` rows. Cache writes
come primarily from materializing the fixture and its 176 indexes. This
breakdown identifies measurable optimization targets, but neither cost proves
the production incident's unknown source-only writer.

The production cache total is 18.86 destructive repair cycles. That many
cycles predict about 57,048 source rows, leaving about 457,298 of the observed
source rows unexplained. Conversely, enough destructive repairs to produce the
observed source total would write about 4.39 million cache rows. Mixing intact
restarts with destructive repairs also has no nonnegative solution for the two
observed totals. The Chat wrapper's measured repair, restart, and alarm paths
cannot be the sole writer in that incident. A source-only application writer or
an unmeasured source-side restart loop was active during the window.

The historical NULL-rank failure also does not reproduce on the current
Orez/Zero dependencies. The harness verified a persisted NULL rank, disabled
Chat's rank repair, and reached ready with the intact-restart signature. This is
consistent with that crash belonging to the older dependency release rather
than the current stack.

The SQL totals are comparable to Cloudflare billing because they use the
runtime's billing counter. Neither harness meters Durable Object key-value
methods or alarms. The production wrapper uses those for small tags, instance
names, and boot alarms. Each is billable separately, and these profiles do not
count them.

Local workerd does not expose the browser `MessageChannel` global used by the
in-isolate postgres bridge, and its timer host function must retain its global
receiver. The harness supplies a same-isolate queued port pair and a bound
timer. All SQL execution, Durable Object persistence, Zero startup, initial
sync, timeout, and retry behavior still runs inside workerd.
