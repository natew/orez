// differential oracle: runs the TypeScript reference core
// (src/sync-server/sync-server.ts) on a shared operation trace and emits the
// pull responses as JSON, so the Rust differential test can compare its own run
// of the SAME trace against this ground truth.
//
// run with: bun crates/sync-core/ts-oracle/run-oracle.ts <trace.json>
// the trace is a JSON array of ops; the runner maintains a per-client mutation
// id counter and per-client cookie EXACTLY as the Rust runner does, so both
// stay in lockstep. output on stdout: a JSON array of pull responses in order.
import { Database } from 'bun:sqlite'

import {
  createSyncServer,
  MutationAppError,
  type SyncDb,
  type SyncTables,
} from '../../../src/sync-server/sync-server.ts'

function bunSqliteDb(sqlite: Database): SyncDb {
  return {
    exec(sql, params = []) {
      sqlite.query(sql).run(...(params as never[]))
    },
    all(sql, params = []) {
      return sqlite.query(sql).all(...(params as never[])) as Record<string, unknown>[]
    },
    transaction<T>(fn: () => T): T {
      sqlite.run('BEGIN')
      try {
        const result = fn()
        sqlite.run('COMMIT')
        return result
      } catch (error) {
        sqlite.run('ROLLBACK')
        throw error
      }
    },
  }
}

const TABLES: SyncTables = {
  item: {
    columns: {
      id: 'string',
      label: 'string',
      rank: 'number',
      done: 'boolean',
      meta: 'json',
    },
    primaryKey: ['id'],
  },
}

function mutate(tx: SyncDb, name: string, args: unknown, _ctx: { userID: string }) {
  const a = args as Record<string, unknown>
  if (name === 'item.put') {
    tx.exec(
      `INSERT INTO item (id, label, rank, done, meta) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (id) DO UPDATE SET label = excluded.label, rank = excluded.rank,
       done = excluded.done, meta = excluded.meta`,
      [
        a.id,
        a.label,
        a.rank,
        a.done ? 1 : 0,
        a.meta === null ? null : JSON.stringify(a.meta),
      ]
    )
    return
  }
  if (name === 'item.del') {
    tx.exec(`DELETE FROM item WHERE id = ?`, [a.id])
    return
  }
  if (name === 'item.reject') {
    tx.exec(`INSERT INTO item (id, label, rank, done) VALUES ('rejected', 'x', 0, 0)`)
    throw new MutationAppError('nope')
  }
  throw new Error(`unknown mutator ${name}`)
}

type Op = Record<string, any>

type Row = Record<string, unknown> & { id: string }
type Ast = {
  table: string
  where?: Condition
  related?: Relation[]
  orderBy?: [string, 'asc' | 'desc'][]
  limit?: number
  start?: { row: Record<string, unknown>; exclusive: boolean }
}
type Condition =
  | {
      type: 'simple'
      op: string
      left: { type: 'column'; name: string } | { type: 'literal'; value: unknown }
      right: { type: 'literal'; value: unknown }
    }
  | { type: 'and' | 'or'; conditions: Condition[] }
  | { type: 'correlatedSubquery'; op: 'EXISTS' | 'NOT EXISTS'; related: Relation }
type Relation = {
  correlation: { parentField: string[]; childField: string[] }
  subquery: Ast
}
type Member = { table: string; row: Row }

function memberKey(table: string, row: Row): string {
  return `${table}:${row.id}`
}

function compareScalar(left: unknown, right: unknown): number {
  if (left === right) return 0
  if (left === null || left === undefined) return -1
  if (right === null || right === undefined) return 1
  if (typeof left === 'number' && typeof right === 'number') return left < right ? -1 : 1
  return String(left).localeCompare(String(right))
}

class QueryOracle {
  readonly rows = new Map<string, Map<string, Row>>()
  readonly queries = new Map<string, Ast>()
  readonly desired = new Set<string>()
  memberships = new Map<string, Map<string, Member>>()
  cookie: number | null = null
  watermark = 0
  version = 0
  changed = new Set<string>()

  constructor() {
    for (const table of ['project', 'member', 'task', 'user']) {
      this.rows.set(table, new Map())
    }
    for (const row of [
      { id: 'p0', ownerId: 'u0', name: 'A' },
      { id: 'p1', ownerId: 'u1', name: 'B' },
      { id: 'p2', ownerId: 'u0', name: 'C' },
    ])
      this.rows.get('project')!.set(row.id, row)
    for (const row of [
      { id: 'm0', projectId: 'p0', userId: 'u0' },
      { id: 'm1', projectId: 'p0', userId: 'u1' },
      { id: 'm2', projectId: 'p2', userId: 'u2' },
    ])
      this.rows.get('member')!.set(row.id, row)
    for (const row of [
      { id: 't0', projectId: 'p0', title: 'T0', rank: 1, done: false, dueAt: null },
      { id: 't1', projectId: 'p0', title: 'T1', rank: 2, done: true, dueAt: 10 },
      { id: 't2', projectId: 'p0', title: 'T2', rank: 3, done: false, dueAt: 20 },
      { id: 't3', projectId: 'p2', title: 'T3', rank: 5, done: true, dueAt: null },
    ])
      this.rows.get('task')!.set(row.id, row)
    for (const row of [
      { id: 'u0', name: 'U0' },
      { id: 'u1', name: 'U1' },
      { id: 'u2', name: 'U2' },
    ])
      this.rows.get('user')!.set(row.id, row)
  }

  upsert(table: string, row: Row) {
    const existed = this.rows.get(table)!.has(row.id)
    this.rows.get(table)!.set(row.id, row)
    this.watermark += existed ? 2 : 1
    this.changed.add(memberKey(table, row))
  }

  delete(table: string, id: string) {
    if (!this.rows.get(table)!.delete(id)) return
    this.watermark++
    this.changed.add(`${table}:${id}`)
  }

  correlatedRows(relation: Relation, parent: Row): Row[] {
    const pairs = relation.correlation.parentField.map((field, index) => [
      relation.correlation.childField[index]!,
      parent[field],
    ]) as [string, unknown][]
    return this.select(relation.subquery, pairs)
  }

  test(condition: Condition, row: Row): boolean {
    if (condition.type === 'and')
      return condition.conditions.every((part) => this.test(part, row))
    if (condition.type === 'or')
      return condition.conditions.some((part) => this.test(part, row))
    if (condition.type === 'correlatedSubquery') {
      const exists = this.correlatedRows(condition.related, row).length > 0
      return condition.op === 'EXISTS' ? exists : !exists
    }
    const left =
      condition.left.type === 'column' ? row[condition.left.name] : condition.left.value
    const right = condition.right.value
    switch (condition.op) {
      case '=':
        return left !== null && right !== null && left === right
      case '!=':
        return left !== null && right !== null && left !== right
      case 'IS':
        return left === right
      case 'IS NOT':
        return left !== right
      case '<':
        return left !== null && right !== null && compareScalar(left, right) < 0
      case '>':
        return left !== null && right !== null && compareScalar(left, right) > 0
      case '<=':
        return left !== null && right !== null && compareScalar(left, right) <= 0
      case '>=':
        return left !== null && right !== null && compareScalar(left, right) >= 0
      case 'IN':
        return Array.isArray(right) && right.includes(left)
      case 'NOT IN':
        return Array.isArray(right) && !right.includes(left)
      default:
        throw new Error(`query oracle does not implement operator ${condition.op}`)
    }
  }

  ordering(ast: Ast): [string, 'asc' | 'desc'][] {
    const order = [...(ast.orderBy ?? [])]
    if (!order.some(([column]) => column === 'id')) order.push(['id', 'asc'])
    return order
  }

  compareRows(ast: Ast, left: Row, right: Record<string, unknown>): number {
    for (const [column, direction] of this.ordering(ast)) {
      const compared = compareScalar(left[column], right[column])
      if (compared !== 0) return direction === 'desc' ? -compared : compared
    }
    return 0
  }

  select(ast: Ast, correlation: [string, unknown][] = []): Row[] {
    let selected = [...this.rows.get(ast.table)!.values()].filter(
      (row) =>
        correlation.every(([column, value]) => row[column] === value) &&
        (!ast.where || this.test(ast.where, row))
    )
    selected.sort((left, right) => this.compareRows(ast, left, right))
    if (ast.start) {
      selected = selected.filter((row) => {
        const compared = this.compareRows(ast, row, ast.start!.row)
        return ast.start!.exclusive ? compared > 0 : compared >= 0
      })
    }
    if (ast.limit !== undefined) selected = selected.slice(0, ast.limit)
    return selected
  }

  collectCondition(
    condition: Condition | undefined,
    parent: Row,
    out: Map<string, Member>
  ) {
    if (!condition) return
    if (condition.type === 'and' || condition.type === 'or') {
      for (const part of condition.conditions) this.collectCondition(part, parent, out)
      return
    }
    if (condition.type !== 'correlatedSubquery') return
    for (const row of this.correlatedRows(condition.related, parent)) {
      this.collectAst(condition.related.subquery, row, out)
    }
  }

  collectAst(ast: Ast, row: Row, out: Map<string, Member>, includeRoot = true) {
    if (includeRoot) out.set(memberKey(ast.table, row), { table: ast.table, row })
    this.collectCondition(ast.where, row, out)
    for (const relation of ast.related ?? []) {
      for (const related of this.correlatedRows(relation, row)) {
        this.collectAst(relation.subquery, related, out)
      }
    }
  }

  evaluate(ast: Ast): Map<string, Member> {
    const out = new Map<string, Member>()
    for (const row of this.select(ast)) this.collectAst(ast, row, out)
    return out
  }

  union(memberships: Map<string, Map<string, Member>>): Map<string, Member> {
    const out = new Map<string, Member>()
    for (const membership of memberships.values()) {
      for (const [key, member] of membership) out.set(key, member)
    }
    return out
  }

  pull(patch?: Op[]): Record<string, unknown> {
    const fresh = this.cookie === null
    const caughtUp = this.cookie === this.watermark
    const oldUnion = this.union(this.memberships)
    const rehydrate = new Set<string>()
    if (patch) {
      this.version++
      for (const operation of patch) {
        if (operation.op === 'put') {
          this.queries.set(operation.hash, operation.ast as Ast)
          if (!this.desired.has(operation.hash)) rehydrate.add(operation.hash)
          this.desired.add(operation.hash)
        } else if (operation.op === 'del') {
          this.desired.delete(operation.hash)
        } else if (operation.op === 'clear') {
          this.desired.clear()
        }
      }
    }

    const memberships = new Map<string, Map<string, Member>>()
    for (const hash of [...this.desired].sort()) {
      memberships.set(hash, this.evaluate(this.queries.get(hash)!))
    }
    const nextUnion = this.union(memberships)
    const puts = new Map<string, Member>()
    const dels = new Map<string, Member>()
    if (fresh) {
      for (const [key, member] of nextUnion) puts.set(key, member)
    } else {
      for (const [key, member] of nextUnion) {
        if (!oldUnion.has(key) || this.changed.has(key)) puts.set(key, member)
      }
      for (const hash of rehydrate) {
        for (const [key, member] of memberships.get(hash) ?? []) puts.set(key, member)
      }
      for (const [key, member] of oldUnion) {
        if (!nextUnion.has(key)) dels.set(key, member)
      }
    }
    this.memberships = memberships
    this.cookie = this.watermark
    this.changed.clear()

    if (!fresh && !patch && caughtUp && puts.size === 0 && dels.size === 0) {
      return { cookie: this.watermark, unchanged: true }
    }
    const rowsPatch: Record<string, unknown>[] = fresh ? [{ op: 'clear' }] : []
    for (const [, member] of [...dels].sort(([left], [right]) =>
      left.localeCompare(right)
    )) {
      rowsPatch.push({ op: 'del', tableName: member.table, id: { id: member.row.id } })
    }
    for (const [, member] of [...puts].sort(([left], [right]) =>
      left.localeCompare(right)
    )) {
      rowsPatch.push({ op: 'put', tableName: member.table, value: member.row })
    }
    return {
      cookie: this.watermark,
      lastMutationIDChanges: { 'query-client': 0 },
      rowsPatch,
      gotQueries: {
        version: this.version,
        patch: [...this.desired].sort().map((hash) => ({ op: 'put', hash })),
      },
    }
  }
}

const tracePath = process.argv[2]
if (!tracePath) throw new Error('usage: run-oracle.ts <trace.json>')
const trace = JSON.parse(await Bun.file(tracePath).text()) as Op[]

const sqlite = new Database(':memory:')
const db = bunSqliteDb(sqlite)
db.exec(
  `CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL,
   rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)`
)
const sync = createSyncServer({ db, tables: TABLES, mutate })
const query = new QueryOracle()

const nextId: Record<string, number> = {}
const cookies: Record<string, number | null> = {}
const pulls: unknown[] = []

for (const op of trace) {
  switch (op.op) {
    case 'put':
    case 'del':
    case 'reject': {
      const client = op.client as string
      const id = (nextId[client] = (nextId[client] ?? 0) + 1)
      const name =
        op.op === 'put' ? 'item.put' : op.op === 'del' ? 'item.del' : 'item.reject'
      const args =
        op.op === 'put'
          ? { id: op.item, label: op.label, rank: op.rank, done: op.done, meta: op.meta }
          : op.op === 'del'
            ? { id: op.item }
            : {}
      sync.handlePush(
        {
          clientGroupID: 'g1',
          mutations: [
            { type: 'custom', id, clientID: client, name, args: [args], timestamp: 0 },
          ],
          pushVersion: 1,
          requestID: 'r',
        },
        'u1'
      )
      break
    }
    case 'upstream':
      db.exec(op.sql as string)
      break
    case 'invalidate':
      sync.invalidate()
      break
    case 'pull': {
      const client = op.client as string
      const cookie = cookies[client] ?? null
      const resp = sync.handlePull(
        { clientID: client, clientGroupID: 'g1', cookie },
        'u1'
      ) as {
        cookie: number
      }
      cookies[client] = resp.cookie
      pulls.push({ lane: 'base', response: resp })
      break
    }
    case 'queryput':
      pulls.push({
        lane: 'query',
        response: query.pull([
          {
            op: 'put',
            hash: op.hash,
            ast: op.ast,
            transformVersion: op.transform_version,
          },
        ]),
      })
      break
    case 'querydel':
      pulls.push({ lane: 'query', response: query.pull([{ op: 'del', hash: op.hash }]) })
      break
    case 'queryclear':
      pulls.push({ lane: 'query', response: query.pull([{ op: 'clear' }]) })
      break
    case 'querypull':
      pulls.push({ lane: 'query', response: query.pull() })
      break
    case 'queryproject':
      query.upsert('project', { id: op.id, ownerId: op.owner_id, name: op.name })
      break
    case 'querymember':
      query.upsert('member', { id: op.id, projectId: op.project_id, userId: op.user_id })
      break
    case 'querytask':
      query.upsert('task', {
        id: op.id,
        projectId: op.project_id,
        title: op.title,
        rank: op.rank,
        done: op.done,
        dueAt: op.due_at,
      })
      break
    case 'queryuser':
      query.upsert('user', { id: op.id, name: op.name })
      break
    case 'querydelete':
      query.delete(op.table, op.id)
      break
    default:
      throw new Error(`unknown op ${op.op}`)
  }
}

process.stdout.write(JSON.stringify(pulls))
