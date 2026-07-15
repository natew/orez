# Production-shape Zero write profile

`production-shape.ts` runs the real Cloudflare Zero embed in local workerd with
separate SQLite Durable Objects for the upstream SQL store and Zero's replica.
It measures every `rowsWritten` cursor delta through
`trackSqlCursorRowsWritten`, using the same workerd storage API and Zero CF
overlay as a deployment.

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

The source multiplier was the rollback guard for implicit trigger and
foreign-key writes. Two Zero metadata writes per startup caused that guard to
copy every published table into `_orez_tx_*` snapshots. Those copies accounted
for 10,714 of 10,888 source rows in the original clean run. Following only the
transitive trigger and foreign-key targets reduces source writes by 91.8% while
retaining the all-table fallback for trigger SQL the parser cannot understand.

The cache total is dominated by required replica materialization. In this
fixture, inserting the 4,663 rows and building the 176 indexes accounts for
23,204 rows before Zero's own schema and transaction bookkeeping. A retry loop
therefore pays roughly 25,800 cache rows every time it discards and recreates a
replica. The generation cleanup in commit `e197416` makes a forced timeout stop
before its retry starts; the measured retry reused the completed durable
replica and paid 2,550 rows instead of another 25,811-row initial sync.

Local workerd does not expose the browser `MessageChannel` global used by the
in-isolate postgres bridge, and its timer host function must retain its global
receiver. The harness supplies a same-isolate queued port pair and a bound
timer. All SQL execution, Durable Object persistence, Zero startup, initial
sync, timeout, and retry behavior still runs inside workerd.
