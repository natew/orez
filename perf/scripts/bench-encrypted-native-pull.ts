// bun perf/scripts/bench-encrypted-native-pull.ts
import {
  createEncryptedColumnCodec,
  type EncryptedColumnManifest,
  type EncryptedRowBatch,
  type JSONValue,
  type PullResponse,
} from '../../packages/limesync/src/client/encrypted-column-codec'
import { RowTable } from '../../packages/limesync/src/zero/native/rows'

const manifest = {
  version: 1,
  networkID: 'benchmark-network',
  schemaID: 'benchmark-v1',
  rowMutations: { 'cloud.applyBatch': { argumentIndex: 0, format: 'orez-row-batch-v1' } },
  tables: {
    session_roster: { primaryKey: ['id'], columns: { metadata: {} } },
    message: {
      primaryKey: ['id'],
      columns: { title: {}, body: {}, options: {}, sender: {} },
    },
  },
} satisfies EncryptedColumnManifest
function sortedJSON(value: unknown) {
  return JSON.stringify(value, (_key, value: unknown) =>
    value && typeof value === 'object' && !Array.isArray(value)
      ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)))
      : value
  )
}
const key = new Uint8Array(32).fill(19)
const codec = createEncryptedColumnCodec({
  manifest,
  keyring: {
    async current() {
      return { epoch: 7, key }
    },
    async get(epoch) {
      return epoch === 7 ? key : undefined
    },
  },
})
const rows: EncryptedRowBatch['rows'] = [
  ...Array.from({ length: 50 }, (_, i) => ({
    seq: i + 1,
    table: 'session_roster',
    op: 'put' as const,
    value: {
      id: 's' + i,
      machine_id: 'm' + (i % 3),
      state: i % 4 ? 'running' : 'exited',
      kind: 'worker',
      agent: 'agent-' + i,
      last_state_change_ms: 1_789_000_000_000 + i,
      metadata: {
        title: 'Session ' + i,
        directory: '/Volumes/worktrees/' + i,
        runner: { provider: 'codex', options: ['hard', 'local'] },
        detail: 'Session description '.repeat(20),
      },
    },
  })),
  ...Array.from({ length: 100 }, (_, i) => ({
    seq: i + 51,
    table: 'message',
    op: 'put' as const,
    value: {
      id: 'm' + i,
      session_id: 's' + (i % 10),
      seq: i,
      ts_ms: 1_789_000_000_000 + i,
      kind: i % 5 ? 'message' : 'choice',
      status: 'sent',
      title: 'Message ' + i,
      body: 'A message body with a few lines.\n'.repeat(18),
      sender: { direction: i % 2 ? 'in' : 'out', name: 'Agent ' + (i % 10) },
      options: i % 5 ? null : [{ label: 'Continue' }, { label: 'Stop' }],
    },
  })),
]
const encrypted = await codec.encodePush({
  clientGroupID: 'benchmark',
  pushVersion: 1,
  schemaVersion: '1',
  timestamp: 0,
  mutations: [
    {
      type: 'custom',
      clientID: 'benchmark',
      id: 1,
      name: 'cloud.applyBatch',
      args: [{ sourceID: 'benchmark', fromSeq: 1, throughSeq: rows.length, rows }],
      timestamp: 0,
    },
  ],
})
const batch = (encrypted.mutations[0] as { args: [EncryptedRowBatch] }).args[0]
const wire = JSON.stringify({
  cookie: 10,
  lastMutationIDChanges: { benchmark: 1 },
  rowsPatch: batch.rows.map((row) => ({
    op: 'put',
    tableName: row.table,
    value: row.value,
  })),
})

type Host = typeof globalThis & {
  benchmarkJSIBridge?: (request: unknown) => void
}
const bridge = (globalThis as Host).benchmarkJSIBridge
const samples = new Map<string, number[]>()
function record(label: string, start: number, i: number) {
  if (i < 10) return
  const times = samples.get(label)
  const elapsed = performance.now() - start
  if (times) times.push(elapsed)
  else samples.set(label, [elapsed])
}

for (let i = 0; i < 90; ++i) {
  let start = performance.now()
  const parsed = JSON.parse(wire) as PullResponse
  record('wire JSON.parse', start, i)
  start = performance.now()
  const decoded = await codec.decodePull(parsed)
  record('authenticated column decode', start, i)
  if (bridge) {
    start = performance.now()
    bridge({ op: 'beginPull', response: decoded })
    record('JSI input object copy', start, i)
  }
  start = performance.now()
  const serialized = JSON.stringify(decoded, (_key, value: unknown) => {
    if (typeof value === 'number' && !Number.isFinite(value))
      throw new TypeError('Native Zero values must contain finite numbers')
    return value
  })
  if (bridge) bridge({ op: 'beginPull', response: serialized })
  record('validated JSON stringify + optional JSI string copy', start, i)
  if (decoded.unchanged) throw new Error('missing row patch')
  const values = decoded.rowsPatch.map((row) => (row as { value: JSONValue }).value)
  start = performance.now()
  const result = new RowTable().result(values)
  record('RowTable cold intern + freeze', start, i)
  if (i === 0 && sortedJSON(result.value) !== sortedJSON(rows.map((row) => row.value)))
    throw new Error('decoded data differs from seed')
}

console.log(wire.length + ' wire characters, 150 rows, 450 encrypted cells (ms)')
for (const [label, times] of samples) {
  times.sort((left, right) => left - right)
  console.log(label + ': p50=' + times[40].toFixed(3) + ' p95=' + times[76].toFixed(3))
}
