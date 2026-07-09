// zero-import-free half of the fixture: DDL, deterministic seed, the table
// spec, and the server-side mutator executor. the cloudflare worker bundles
// THIS module + orez src/sync-server only — keeping @rocicorp/zero (a client
// package) out of the worker bundle. fixture.ts re-exports and a guard in
// fixture.ts asserts TABLES stays equal to tablesFromZeroSchema(schema).
import {
  MutationAppError,
  type SyncDb,
  type SyncTables,
} from '../../src/sync-server/sync-server'

// mirror of the zero schema's tables (guarded against drift in fixture.ts)
export const TABLES: SyncTables = {
  user: { columns: { id: 'string', name: 'string' }, primaryKey: ['id'] },
  project: {
    columns: { id: 'string', ownerId: 'string', name: 'string' },
    primaryKey: ['id'],
  },
  member: {
    columns: { id: 'string', projectId: 'string', userId: 'string' },
    primaryKey: ['id'],
  },
  task: {
    columns: {
      id: 'string',
      projectId: 'string',
      title: 'string',
      rank: 'number',
      done: 'boolean',
      meta: 'json',
      dueAt: 'number',
    },
    primaryKey: ['id'],
  },
}

export function jsonColumnsOf(tableName: string): Set<string> {
  return new Set(
    Object.entries(TABLES[tableName]?.columns ?? {})
      .filter(([, type]) => type === 'json')
      .map(([name]) => name)
  )
}

// column names are unmapped, so store columns must match the zero schema
// exactly. this DDL is valid in postgres AND sqlite — every target runs the
// same statements. rank is double precision, NOT real: pg float4 would
// round-trip with float32 noise while sqlite REAL is always 8-byte.
export const DDL = [
  `CREATE TABLE "user" (id text PRIMARY KEY, name text NOT NULL)`,
  `CREATE TABLE project (id text PRIMARY KEY, "ownerId" text NOT NULL, name text NOT NULL)`,
  `CREATE TABLE member (id text PRIMARY KEY, "projectId" text NOT NULL, "userId" text NOT NULL)`,
  `CREATE TABLE task (id text PRIMARY KEY, "projectId" text NOT NULL, title text NOT NULL,
    rank double precision NOT NULL, done boolean NOT NULL, meta jsonb, "dueAt" bigint)`,
]

// deterministic dataset: same rows on every target, every run. exercises
// unicode, LIKE-able substrings, float/negative ranks, null json/dueAt,
// nested json, and SCALAR json values (string/number/bool) — jsonb holds any
// json type and both stacks must round-trip them identically.
function mulberry32(seed: number) {
  let a = seed
  return () => {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

export function generateSeed(seed = 1) {
  const rng = mulberry32(seed)
  const pick = <T>(arr: T[]) => arr[Math.floor(rng() * arr.length)]!

  const users = Array.from({ length: 8 }, (_, i) => ({
    id: `u${i}`,
    name: pick(['ann', 'bob 🌵', 'çelik', 'dee', 'evan fix', 'frida', 'gus', 'hana']) + ` ${i}`,
  }))

  const projects = Array.from({ length: 12 }, (_, i) => ({
    id: `p${i}`,
    ownerId: `u${i % users.length}`,
    name: pick(['alpha', 'fixup', 'Zenith', 'delta x', 'ütopia', 'omega']) + ` ${i}`,
  }))

  const members: { id: string; projectId: string; userId: string }[] = []
  let m = 0
  for (const p of projects) {
    const count = 1 + Math.floor(rng() * 3)
    for (let j = 0; j < count; j++) {
      members.push({
        id: `m${m++}`,
        projectId: p.id,
        userId: `u${Math.floor(rng() * users.length)}`,
      })
    }
  }

  // writer discipline matters for json: postgres.js stores a js string param
  // into jsonb as a json string (double-encoded), so seed paths encode
  // schema-driven (pg: sql.json; sqlite: JSON.stringify)
  const metas = [
    null,
    { tags: ['a', 'b'], depth: { n: 1 } },
    { emoji: '✅', list: [1, 2.5, -3] },
    { s: 'plain' },
    [1, 'two', null],
    'scalar string',
    42.5,
    true,
  ]
  const tasks = Array.from({ length: 48 }, (_, i) => ({
    id: `t${i}`,
    projectId: `p${Math.floor(rng() * 10)}`, // p10/p11 stay task-less
    title:
      pick(['fix login', 'polish ux', 'refactor sync', 'fix flaky test', 'ship it 🚀', 'triage']) +
      ` ${i}`,
    rank: Math.round((rng() * 20 - 4) * 100) / 100,
    done: rng() > 0.6,
    meta: pick(metas),
    dueAt: rng() > 0.3 ? 1750000000000 + Math.floor(rng() * 10_000_000_000) : null,
  }))

  return { user: users, project: projects, member: members, task: tasks }
}

export const SEED = generateSeed()

// seed a sqlite SyncDb (orez-local and the DO share this path)
export function seedSqlite(db: SyncDb) {
  for (const stmt of DDL) db.exec(stmt)
  for (const [tableName, rows] of Object.entries(SEED)) {
    const jsonCols = jsonColumnsOf(tableName)
    for (const row of rows) {
      const cols = Object.keys(row)
      // sqlite json storage = the JSON-ENCODED text of the value (matches
      // zero's replica model) so scalar json round-trips too
      const values = Object.entries(row).map(([k, v]) =>
        jsonCols.has(k) && v !== null
          ? JSON.stringify(v)
          : typeof v === 'boolean'
            ? v
              ? 1
              : 0
            : v
      )
      db.exec(
        `INSERT INTO "${tableName}" (${cols.map((c) => `"${c}"`).join(', ')})
         VALUES (${cols.map(() => '?').join(', ')})`,
        values
      )
    }
  }
}

// server-side custom mutator execution against sqlite. same names/semantics
// as the client registry in fixture.ts; plain SQL like soot's server side.
// semantics must MATCH the client impls exactly (e.g. project.delete does not
// cascade, because the client mutator doesn't).
export function executeMutator(tx: SyncDb, name: string, args: unknown, _ctx: { userID: string }) {
  switch (name) {
    case 'project.create': {
      const a = args as { id: string; ownerId: string; name: string }
      const exists = tx.all(`SELECT 1 FROM project WHERE id = ?`, [a.id])
      if (exists.length > 0) throw new MutationAppError('exists')
      tx.exec(`INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)`, [
        a.id,
        a.ownerId,
        a.name,
      ])
      return
    }
    case 'project.rename': {
      const a = args as { id: string; name: string }
      tx.exec(`UPDATE project SET name = ? WHERE id = ?`, [a.name, a.id])
      return
    }
    case 'project.delete': {
      const a = args as { id: string }
      tx.exec(`DELETE FROM project WHERE id = ?`, [a.id])
      return
    }
    case 'member.add': {
      const a = args as { id: string; projectId: string; userId: string }
      tx.exec(`INSERT INTO member (id, "projectId", "userId") VALUES (?, ?, ?)`, [
        a.id,
        a.projectId,
        a.userId,
      ])
      return
    }
    case 'member.remove': {
      const a = args as { id: string }
      tx.exec(`DELETE FROM member WHERE id = ?`, [a.id])
      return
    }
    case 'task.create': {
      const a = args as {
        id: string
        projectId: string
        title: string
        rank: number
        done: boolean
        meta?: unknown
        dueAt?: number
      }
      tx.exec(
        `INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt")
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [
          a.id,
          a.projectId,
          a.title,
          a.rank,
          a.done ? 1 : 0,
          a.meta === undefined || a.meta === null ? null : JSON.stringify(a.meta),
          a.dueAt ?? null,
        ]
      )
      return
    }
    case 'task.toggle': {
      const a = args as { id: string }
      const existing = tx.all(`SELECT done FROM task WHERE id = ?`, [a.id])
      if (existing.length === 0) throw new MutationAppError('not-found')
      tx.exec(`UPDATE task SET done = ? WHERE id = ?`, [existing[0]!.done ? 0 : 1, a.id])
      return
    }
    default:
      throw new Error(`unknown mutator: ${name}`)
  }
}

export function userIDFromAuth(header: string | undefined | null): string | null {
  return header?.match(/^Bearer token-(.+)$/)?.[1] ?? null
}
