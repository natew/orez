# orez Production Readiness Plan

## Goal

Make orez viable for production use. Target: run reliably in ~1GB memory (singleDb mode, "free plan") and scale up for larger deployments.

## Architecture Overview

```
┌─────────────┐    TCP/WS    ┌─────────────┐    fork     ┌─────────────┐
│  app client  │◄──────────►│    orez      │◄──────────►│  zero-cache  │
│  (postgres)  │             │              │             │  (node)      │
└─────────────┘             │  ┌─────────┐ │             │  ┌────────┐  │
                             │  │ PGlite  │ │             │  │change- │  │
                             │  │ (WASM)  │ │             │  │streamer│  │
                             │  └─────────┘ │             │  ├────────┤  │
                             │  ┌─────────┐ │             │  │syncer  │  │
                             │  │ pg-proxy│ │             │  └────────┘  │
                             │  └─────────┘ │             └─────────────┘
                             └─────────────┘
```

Key components to measure:

1. **pg-proxy** — TCP wire protocol → PGlite queries (main hot path)
2. **change-tracker** — trigger-based CDC capturing changes
3. **replication handler** — fakes PG logical replication for zero-cache
4. **PGlite manager** — manages SQLite instances (WASM/native)
5. **zero-cache child process** — spawned by orez, does actual sync work

## Phase 1: Instrumentation & Insights (CURRENT)

Build measurement infrastructure before optimizing anything.

### 1.1 Load Testing Harness

- [ ] `perf/load/harness.ts` — configurable load generator
  - Concurrent connections (1..N)
  - Mix of read/write queries
  - Replication throughput (mutations → WS poke latency)
  - Sustained load patterns (steady, burst, ramp-up)
- [ ] `perf/load/scenarios/` — specific load scenarios
  - `basic-crud.ts` — simple CRUD at various concurrency levels
  - `replication-pressure.ts` — many mutations, measure sync lag
  - `connection-churn.ts` — connect/disconnect cycling
  - `large-queries.ts` — big result sets, memory pressure
  - `mixed-workload.ts` — realistic app traffic pattern
- [ ] `perf/load/report.ts` — summarize results as JSON/Markdown

### 1.2 Memory Profiling

- [ ] `perf/memory/profile.ts` — heap snapshot at key lifecycle points
  - Startup baseline
  - After N queries
  - After N mutations
  - After zero-cache sync
  - After connection churn (detect leaks)
  - Long-running stability check
- [ ] `perf/memory/leak-detector.ts` — detect growing maps/arrays
  - Track schemaQueryCache size
  - Track schemaQueryInFlight size
  - Track proxy connection maps
  - Track change-tracker trigger state
- [ ] `perf/memory/compare.ts` — compare snapshots, find deltas

### 1.3 Performance Micro-Benchmarks

- [ ] `perf/scripts/bench-proxy.ts` — measure proxy overhead
  - Raw PGlite query vs. through-proxy query latency
  - Query classification overhead
  - Wire protocol serialization cost
  - Mutex contention under load
- [ ] `perf/scripts/bench-replication.ts` — measure replication latency
  - INSERT → trigger fire → change captured
  - Change captured → replication streamed
  - Replication streamed → zero-cache acknowledged
  - End-to-end: INSERT → WS poke
- [ ] `perf/scripts/bench-startup.ts` — cold start and warm start timing
  - First startup (fresh data dir)
  - Restart (existing data dir)
  - singleDb vs multi-instance startup

### 1.4 Correctness Testing

- [ ] `perf/stability/correctness.test.ts` — property-based tests
  - All rows inserted = all rows replicated
  - No duplicate replication events
  - Ordering: watermark strictly increasing
  - Schema changes mid-sync don't corrupt
  - Concurrent mutations don't lose data
- [ ] `perf/stability/fuzz.test.ts` — fuzzing
  - Random SQL through proxy
  - Random connection patterns
  - Random kill/restart of zero-cache

### 1.5 Long-Term Stability

- [ ] `perf/stability/soak.ts` — multi-hour soak test
  - Sustained load for N hours
  - Periodic health checks
  - Memory heap sampling every M minutes
  - Log anomalies
  - Auto-restart on crash, record
- [ ] `perf/stability/crash-recovery.ts` — crash resilience
  - SIGKILL zero-cache mid-sync
  - SIGKILL orez mid-query
  - Power-loss simulation (kill -9 everything)
  - Verify recovery works each time

## Phase 2: Iterate & Improve (AFTER INSIGHTS)

### 2.1 Memory Optimization Targets

- Proxy connection tracking maps — ensure cleanup
- Schema query cache — TTL, size limit, eviction
- Change tracker trigger state — verify no unbounded growth
- PGlite worker message buffers — verify garbage collected
- Zero-cache child process — max-old-space-size tuning

### 2.2 Latency Optimization Targets

- Proxy hot path allocation reduction
- Query classification pre-computation
- Wire protocol buffer reuse
- Replication WAL encoding optimization

### 2.3 Correctness Hardening

- Transaction isolation edge cases
- Concurrent DDL + DML safety
- Publication membership edge cases
- CDC corruption recovery hardening

### 2.4 Stability Hardening

- File descriptor leak detection
- Timer/setInterval leak detection
- Child process zombie prevention
- Lock file / pid file race conditions

## Key Metrics to Track

| Metric                         | Target      | How to Measure              |
| ------------------------------ | ----------- | --------------------------- |
| Startup time (cold)            | < 5s        | wall clock in bench-startup |
| Startup time (warm)            | < 2s        | wall clock in bench-startup |
| Query latency (simple)         | < 5ms p50   | proxy benchmark             |
| Query latency (through proxy)  | < 10ms p50  | proxy benchmark             |
| Replication latency            | < 100ms p95 | replication latency test    |
| Memory idle (singleDb)         | < 200MB     | heap snapshot               |
| Memory under load (singleDb)   | < 500MB     | heap snapshot               |
| Memory idle (3-instance)       | < 500MB     | heap snapshot               |
| Memory under load (3-instance) | < 1GB       | heap snapshot               |
| Memory growth rate             | < 10MB/hour | soak test                   |
| Crashes per 24h                | 0           | soak test                   |
| Recovery success rate          | 100%        | crash-recovery test         |

## Running Tests

```bash
# Full load test
bun run perf/load/harness.ts

# Memory profile
bun run perf/memory/profile.ts

# Performance benchmarks
bun run perf/scripts/bench-proxy.ts

# Correctness tests
bun test perf/stability/correctness.test.ts

# Soak test (24h)
bun run perf/stability/soak.ts --duration=24h
```

## Notes

- The existing `test-chat-e2e.ts` is the strongest integration test but too heavy for fast iteration (~20min). We'll use it as a validation gate, not a development loop.
- The existing `src/bench/` tests are good micro-benchmarks but need to be expanded into a comprehensive suite.
- singleDb mode is critical for memory-constrained targets — need dedicated profiling.
- WASM vs native SQLite modes have very different performance profiles — need to test both.
- The proxy's schema query cache, mutex contention, and wire protocol overhead are the likely hot spots.
