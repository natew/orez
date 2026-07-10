// Reproducible M6 lane dispatcher. Production routes are intentionally absent.
// The full suite uses the budgets in plans/rust-sync-m6-qualification.md.
import { parseArgs } from 'node:util'

const { values: args } = parseArgs({
  options: {
    suite: { type: 'string', default: 'native' },
    quick: { type: 'boolean', default: false },
  },
})

if (!['native', 'cf', 'all'].includes(args.suite)) {
  throw new Error('suite must be native, cf, or all')
}

type Lane = { name: string; command: string[] }
const quick = args.quick
const native: Lane[] = [
  {
    name: 'native-protocol-fuzz',
    command: [
      'bun',
      'src/protocol-fuzz.ts',
      '--target',
      'rust-local',
      '--cases',
      quick ? '100' : '10000',
      '--seed',
      '1',
      '--concurrency',
      '20',
    ],
  },
  {
    name: 'native-eviction',
    command: [
      'bun',
      'src/eviction.ts',
      '--target',
      'rust-local',
      '--clients',
      quick ? '5' : '20',
    ],
  },
  {
    name: 'native-retention-reconnect',
    command: ['bun', 'src/reconnect.ts', '--target', 'rust-local'],
  },
  {
    name: 'native-query-tab-churn',
    command: ['bun', 'src/multi-tab.ts', '--target', 'rust-local'],
  },
  {
    name: 'native-clock-skew',
    command: [
      'bun',
      'src/clock-skew.ts',
      '--target',
      'rust-local',
      '--clock-skew-hours',
      '24',
    ],
  },
  {
    name: 'native-storage-faults',
    command: ['bun', 'src/storage-faults.ts', '--target', 'rust-local'],
  },
  {
    name: 'native-backup-restore',
    command: ['bun', 'src/backup-restore.ts', '--target', 'rust-local'],
  },
]

const cf: Lane[] = [
  {
    name: 'cf-protocol-fuzz',
    command: [
      'bun',
      'src/protocol-fuzz.ts',
      '--target',
      'rust-cf',
      '--cases',
      quick ? '100' : '10000',
      '--seed',
      '1',
      '--concurrency',
      '20',
    ],
  },
  {
    name: 'cf-eviction',
    command: [
      'bun',
      'src/eviction.ts',
      '--target',
      'rust-cf',
      '--clients',
      quick ? '5' : '20',
    ],
  },
  {
    name: 'cf-retention-reconnect',
    command: ['bun', 'src/reconnect.ts', '--target', 'rust-cf'],
  },
  {
    name: 'cf-query-tab-churn',
    command: ['bun', 'src/multi-tab.ts', '--target', 'rust-cf'],
  },
  {
    name: 'cf-clock-skew',
    command: [
      'bun',
      'src/clock-skew.ts',
      '--target',
      'rust-cf',
      '--clock-skew-hours',
      '24',
    ],
  },
  {
    name: 'cf-storage-faults',
    command: ['bun', 'src/storage-faults.ts', '--target', 'rust-cf'],
  },
  {
    name: 'cf-backup-restore',
    command: ['bun', 'src/backup-restore.ts', '--target', 'rust-cf'],
  },
  {
    name: 'cf-wasm-memory',
    command: [
      'bun',
      'src/memory-soak.ts',
      '--target',
      'rust-cf',
      '--blocks',
      '3',
      '--ops',
      quick ? '10' : '1000',
    ],
  },
  {
    name: 'cf-rollback-one-writer',
    command: ['bun', 'src/rollback-drill.ts', '--confirm-test-only'],
  },
]

const lanes =
  args.suite === 'native' ? native : args.suite === 'cf' ? cf : [...native, ...cf]
const started = performance.now()
for (const lane of lanes) {
  console.log(`[m6] START ${lane.name}`)
  const child = Bun.spawn(['mise', 'exec', 'node@24.3.0', '--', ...lane.command], {
    cwd: import.meta.dir.replace(/\/src$/, ''),
    stdin: 'ignore',
    stdout: 'inherit',
    stderr: 'inherit',
  })
  const exitCode = await child.exited
  if (exitCode !== 0) throw new Error(`${lane.name} failed with exit ${exitCode}`)
  console.log(`[m6] PASS ${lane.name}`)
}

console.log(
  JSON.stringify({
    lane: 'm6-suite',
    result: 'PASS',
    suite: args.suite,
    quick,
    lanes: lanes.map(({ name }) => name),
    elapsedMs: Math.round(performance.now() - started),
  })
)
