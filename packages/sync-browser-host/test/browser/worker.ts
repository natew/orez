import {
  createBrowserSyncHost,
  MutationApplicationError,
  registerMutators,
} from 'orez/sync-browser-host'

import {
  createBrowserSyncHostInternal,
  type BrowserHostTestFaultPoint,
} from '../../src/host.js'
import { serveBrowserSyncHostPortInternal } from '../../src/message-port.js'

import type { BrowserSyncHost, BrowserSyncHostConfig } from '../../src/types.js'

const schema = {
  tables: {
    todo: {
      columns: {
        id: { type: 'string' },
        title: { type: 'string' },
        done: { type: 'boolean' },
      },
      primaryKey: ['id'],
    },
    todoTag: {
      columns: {
        id: { type: 'string' },
        todoId: { type: 'string' },
        label: { type: 'string' },
      },
      primaryKey: ['id'],
    },
  },
} as const

const mutators = registerMutators({
  async 'todo.create'(tx, args) {
    const value = args as { id: string; title: string; done?: boolean }
    const existing = await tx.query('SELECT 1 FROM todo WHERE id = ?', [value.id])
    if (existing.length > 0) throw new MutationApplicationError('already exists')
    await tx.exec('INSERT INTO todo (id, title, done) VALUES (?, ?, ?)', [
      value.id,
      value.title,
      value.done ? 1 : 0,
    ])
  },
  async 'todo.rename'(tx, args) {
    const value = args as { id: string; title: string }
    await tx.exec('UPDATE todo SET title = ? WHERE id = ?', [value.title, value.id])
  },
  async 'todo.delete'(tx, args) {
    const value = args as { id: string }
    await tx.exec('DELETE FROM todo WHERE id = ?', [value.id])
  },
  async 'todo.createDeferred'(tx, args, context) {
    const value = args as { id: string; title: string }
    await tx.exec('INSERT INTO todo (id, title, done) VALUES (?, ?, ?)', [
      value.id,
      value.title,
      0,
    ])
    context.defer(() => {
      self.postMessage({ type: 'effect-complete', id: value.id })
    })
  },
  async 'todo.addTag'(tx, args) {
    const value = args as { id: string; todoId: string; label: string }
    await tx.exec('INSERT INTO todoTag (id, todoId, label) VALUES (?, ?, ?)', [
      value.id,
      value.todoId,
      value.label,
    ])
  },
  async 'todo.copyFromQuery'(tx, args) {
    const value = args as { sourceId: string; targetId: string }
    const source = await tx.queryAst<
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
    await tx.exec('INSERT INTO todo (id, title, done) VALUES (?, ?, ?)', [
      value.targetId,
      `${source.title}:${source.tags.map((tag) => tag.label).join(',')}`,
      source.done ? 1 : 0,
    ])
  },
})

type WorkerMessage =
  | {
      type: 'start'
      storageKey: string
      faultPoint?: BrowserHostTestFaultPoint
      checkpointFailure?: boolean
      port: MessagePort
    }
  | { type: 'connect'; id: string; port: MessagePort }

let host: BrowserSyncHost | undefined

self.addEventListener('message', (event: MessageEvent<WorkerMessage>) => {
  const message = event.data
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
              userID: 'preview-user',
              queryAware: request.headers.get('x-query-aware') === '1',
            }
          : null
      },
      mutators,
      queryAware: (claims) => claims.queryAware === true,
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
    } satisfies BrowserSyncHostConfig
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
