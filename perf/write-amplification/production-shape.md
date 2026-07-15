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

### Historical all-table rollback guard

Set `OREZ_PROFILE_ROLLBACK_MODE=historical-all-table` to replace only the
copied profile build's rollback guard with the exact implementation from
`478bd9b54ea69fdc01f5fa972e4234a52aadd51e`, the Orez 0.5.9 commit immediately
before `d66e626`. The regular checkout and package output remain unchanged.
This isolates the old all-published-table snapshots while retaining the current
Chat wrapper, Zero 1.7.0, cleanup, and generation-recovery code.

```sh
OREZ_PROFILE_ROLLBACK_MODE=historical-all-table \
  OREZ_CHAT_PROFILE_REPO=~/chat \
  bun run perf:write-wrapper-profile
```

The incident replay used Chat commit `632f07122`, including its local terminal
deploy-probe change on top of failed-rollout commit `5c20c203d`. Its generated
data-shim source hash remained
`f33b3333e13571cf77b50f5315217152838df5f72c8f56997b73cca38f2b9089`.
The terminal-probe change only makes `/keepalive?deploy=1` return 409 after a
persisted boot failure. It does not change migrations, embed startup, replica
repair, or their SQL writes.

The full historical-guard scenario costs were:

| Wrapper phase                  | Source rows | Cache rows |
| ------------------------------ | ----------: | ---------: |
| First migration and clean boot |      14,561 |     25,821 |
| Intact replica restart         |       1,494 |      2,559 |
| Four destructive repair paths  | 8,219–8,224 |     25,817 |

The attempted 47-boot and 19-materialization incident schedule records each
attempt separately:

| Cycle and attempt               | Boot result | Source rows | Cache rows | Elapsed |
| ------------------------------- | ----------- | ----------: | ---------: | ------: |
| First cycle, forced failure 1   | failed      |       1,090 |          0 |  124 ms |
| First cycle, forced failure 2   | failed      |           0 |          0 |   13 ms |
| First cycle, recovery           | ready       |      13,473 |     25,821 | 16.25 s |
| Later reset, forced failure 1   | failed      |           0 |          0 |   10 ms |
| Later reset, forced failure 2   | failed      |           0 |          0 |    6 ms |
| Later reset, recovery           | ready       |       8,226 |     25,817 | 15.79 s |
| Later reset, one forced failure | failed      |           0 |          0 |    4 ms |
| Later reset, recovery           | ready       |       8,219 |     25,817 |  687 ms |

The recovery elapsed times after two failures include the persisted 15-second
backoff. The first forced failure applies the initial migration and publication
work. Once that is durable, a forced failure immediately before embed startup
writes no SQL rows. The old 10,888-source direct-embed signature occurs during
the embed attempt that also materializes the cache. Fast wrapper failures do
not pay that source cost independently.

The nearest integer incident model uses 28 fast failures and 19 recoveries:
nine cycles with two failures and ten cycles with one. The measured attempts
project 162,561 source rows and 490,527 cache rows in 153.8 seconds. Matching
the production cache total fractionally gives 18.86 materializations and about
161,343 source rows, leaving about 353,003 source rows unexplained. The proposed
47.24 × 10,888 source calculation assigns full-embed metadata writes to fast
failures that produce zero rows in the wrapper profile.

Forty-seven attempts can fit inside 25 minutes only when successful
materializations repeatedly reset the failure counter. A single uninterrupted
failure streak reaches at most ten attempts in 25 minutes under the 15-second,
30-second, 60-second, 120-second, 240-second, then five-minute backoff. A failed
alarm does not re-arm another boot by itself; another ordinary client request
must call `ensureReady`. The deploy terminal probe stops at the first persisted
failure, so deploy warm polling cannot drive this repeated-attempt schedule.
The schedule is temporally possible through ordinary client reconnects, but
its measured source total rules it out as the incident writer.

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
