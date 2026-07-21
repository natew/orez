import { MutationApplicationError } from 'orez-sync-executor/core'
import { createBrowserSyncHost } from 'orez/sync-browser-host'

import {
  createBrowserSyncHostInternal,
  type BrowserHostTestFaultPoint,
} from '../../src/host.js'
import { serveBrowserSyncHostPortInternal } from '../../src/message-port.js'

import type { BrowserSyncHost, BrowserSyncHostConfig } from '../../src/types.js'
import type { MutatorRegistry } from 'orez-sync-executor'

const schema = {
  tables: {
    todo: {
      name: 'todo',
      columns: {
        id: { type: 'string' },
        title: { type: 'string' },
        done: { type: 'boolean' },
      },
      primaryKey: ['id'],
    },
    todoTag: {
      name: 'todoTag',
      columns: {
        id: { type: 'string' },
        todoId: { type: 'string' },
        label: { type: 'string' },
      },
      primaryKey: ['id'],
    },
  },
  relationships: {},
} as const

const mutators = Object.freeze({
  async 'todo.create'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string; title: string; done?: boolean }
    const existing = await sql.query('SELECT 1 FROM todo WHERE id = ?', [value.id])
    if (existing.length > 0) throw new MutationApplicationError('already exists')
    await sql.exec('INSERT INTO todo (id, title, done) VALUES (?, ?, ?)', [
      value.id,
      value.title,
      value.done ? 1 : 0,
    ])
  },
  async 'todo.rename'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string; title: string }
    await sql.exec('UPDATE todo SET title = ? WHERE id = ?', [value.title, value.id])
  },
  async 'todo.delete'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string }
    await sql.exec('DELETE FROM todo WHERE id = ?', [value.id])
  },
  async 'todo.createDeferred'({ tx, args, ctx }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string; title: string }
    await sql.exec('INSERT INTO todo (id, title, done) VALUES (?, ?, ?)', [
      value.id,
      value.title,
      0,
    ])
    ctx.defer(() => {
      self.postMessage({ type: 'effect-complete', id: value.id })
    })
  },
  async 'todo.addTag'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { id: string; todoId: string; label: string }
    await sql.exec('INSERT INTO todoTag (id, todoId, label) VALUES (?, ?, ?)', [
      value.id,
      value.todoId,
      value.label,
    ])
  },
  async 'todo.copyFromQuery'({ tx, args }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { sourceId: string; targetId: string }
    const source = await sql.queryAst<
      | {
          id: string
          title: string
          done: boolean
          tags: Array<{ id: string; todoId: string; label: string }>
        }
      | undefined
    >(
      {
        table: 'todo',
        where: {
          type: 'simple',
          op: '=',
          left: { type: 'column', name: 'id' },
          right: { type: 'literal', value: value.sourceId },
        },
        related: [
          {
            correlation: { parentField: ['id'], childField: ['todoId'] },
            subquery: { table: 'todoTag', alias: 'tags', orderBy: [['id', 'asc']] },
          },
        ],
      },
      {
        singular: true,
        relationships: { tags: { singular: false, relationships: {} } },
      },
      'todoWithTags'
    )
    if (!source) throw new MutationApplicationError('source does not exist')
    await sql.exec('INSERT INTO todo (id, title, done) VALUES (?, ?, ?)', [
      value.targetId,
      `${source.title}:${source.tags.map((tag) => tag.label).join(',')}`,
      source.done ? 1 : 0,
    ])
  },
  async 'test.applicationTransaction'({ tx, args, ctx }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { messageID: string }
    await sql.exec('INSERT INTO todo (id, title, done) VALUES (?, ?, ?)', [
      'application-transaction',
      'trusted',
      0,
    ])
    ctx.defer(() => {
      self.postMessage({
        type: 'application-transaction-effect',
        id: value.messageID,
      })
    })
  },
  async 'test.applicationTransactionRollback'({ tx, args, ctx }) {
    const sql = tx.dbTransaction.wrappedTransaction
    const value = args as { messageID: string }
    await sql.exec('INSERT INTO todo (id, title, done) VALUES (?, ?, ?)', [
      'application-transaction-rollback',
      'must roll back',
      0,
    ])
    ctx.defer(() => {
      self.postMessage({
        type: 'application-transaction-rollback-effect',
        id: value.messageID,
      })
    })
    throw new Error('rollback requested')
  },
} satisfies MutatorRegistry<typeof schema>)

type WorkerMessage =
  | {
      type: 'start'
      storageKey: string
      faultPoint?: BrowserHostTestFaultPoint
      checkpointFailure?: boolean
      port: MessagePort
    }
  | { type: 'connect'; id: string; port: MessagePort }
  | { type: 'application-transaction'; id: string }
  | { type: 'application-transaction-rollback'; id: string }

let host: BrowserSyncHost<typeof schema> | undefined

self.addEventListener('message', (event: MessageEvent<WorkerMessage>) => {
  const message = event.data
  if (message.type === 'application-transaction-rollback') {
    if (!host) {
      self.postMessage({
        type: 'application-transaction-rollback-error',
        id: message.id,
        message: 'host is not ready',
      })
      return
    }
    void host.executor
      .execute(
        'test.applicationTransactionRollback',
        { messageID: message.id },
        { userID: 'preview-user' }
      )
      .then(() => {
        self.postMessage({
          type: 'application-transaction-rollback-error',
          id: message.id,
          message: 'transaction unexpectedly committed',
        })
      })
      .catch((error) => {
        self.postMessage({
          type: 'application-transaction-rollback-complete',
          id: message.id,
          message: error instanceof Error ? error.message : String(error),
        })
      })
    return
  }
  if (message.type === 'application-transaction') {
    if (!host) {
      self.postMessage({
        type: 'application-transaction-error',
        id: message.id,
        message: 'host is not ready',
      })
      return
    }
    void host.executor
      .execute(
        'test.applicationTransaction',
        { messageID: message.id },
        { userID: 'preview-user' }
      )
      .then(() =>
        host!.executor.query({ userID: 'preview-user' }, (tx) =>
          tx.dbTransaction.wrappedTransaction.queryAst<
            { id: string; title: string; done: boolean } | undefined
          >(
            {
              table: 'todo',
              where: {
                type: 'simple',
                op: '=',
                left: { type: 'column', name: 'id' },
                right: { type: 'literal', value: 'application-transaction' },
              },
            },
            { singular: true, relationships: {} },
            'applicationTransactionTodo'
          )
        )
      )
      .then((row) => {
        self.postMessage({
          type: 'application-transaction-complete',
          id: message.id,
          rows: row ? [row] : [],
        })
      })
      .catch((error) => {
        self.postMessage({
          type: 'application-transaction-error',
          id: message.id,
          message: error instanceof Error ? error.message : String(error),
        })
      })
    return
  }
  if (message.type === 'connect') {
    if (!host) {
      self.postMessage({ type: 'boot-error', message: 'host is not ready' })
      return
    }
    serveBrowserSyncHostPortInternal(host, message.port)
    self.postMessage({ type: 'connected', id: message.id })
    return
  }
  void (async () => {
    const { storageKey, faultPoint, checkpointFailure, port } = message
    const hooks = {
      async reach(point: BrowserHostTestFaultPoint) {
        if (point !== faultPoint) return
        self.postMessage({ type: 'fault-reached', point })
        await new Promise<never>(() => {})
      },
    }
    const config = {
      storageKey,
      schema,
      initialize(sql) {
        sql.exec(
          'CREATE TABLE IF NOT EXISTS todo (id TEXT PRIMARY KEY, title TEXT NOT NULL, done INTEGER NOT NULL)'
        )
        sql.exec(
          'CREATE TABLE IF NOT EXISTS todoTag (id TEXT PRIMARY KEY, todoId TEXT NOT NULL, label TEXT NOT NULL)'
        )
      },
      authenticate(request) {
        return request.headers.get('authorization') === 'Bearer preview-token'
          ? {
              id: 'preview-user',
              queryAware: request.headers.get('x-query-aware') === '1',
            }
          : null
      },
      authorize() {
        return true
      },
      mutators,
      queryAware: (authData) => authData?.queryAware === true,
      resolveQuery(name) {
        if (name !== 'todosDone') throw new Error(`unknown query: ${name}`)
        return {
          table: 'todo',
          where: {
            type: 'simple',
            left: { type: 'column', name: 'done' },
            right: { type: 'literal', value: true },
            op: '=',
          },
          orderBy: [['id', 'asc']],
        }
      },
    } satisfies BrowserSyncHostConfig<typeof schema>
    const createdHost = faultPoint
      ? await createBrowserSyncHostInternal(config, hooks)
      : await createBrowserSyncHost(config)
    host = createdHost
    if (checkpointFailure) {
      const originalPut = IDBObjectStore.prototype.put
      IDBObjectStore.prototype.put = function () {
        IDBObjectStore.prototype.put = originalPut
        throw new DOMException('injected checkpoint failure', 'QuotaExceededError')
      }
    }
    serveBrowserSyncHostPortInternal(createdHost, port, hooks)
    self.postMessage({ type: 'ready' })
  })().catch((error) => {
    self.postMessage({
      type: 'boot-error',
      message: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    })
  })
})
