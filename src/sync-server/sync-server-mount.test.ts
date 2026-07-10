import { DatabaseSync } from 'node:sqlite'

import { describe, expect, test } from 'vitest'

import {
  type SyncDb,
  type SyncServer,
  SyncHttpError,
  createSyncServer,
  createSyncServerMount,
} from './sync-server'

function nodeSqliteDb(sqlite: DatabaseSync): SyncDb {
  return {
    exec(sql, params = []) {
      sqlite.prepare(sql).run(...(params as never[]))
    },
    all(sql, params = []) {
      return sqlite.prepare(sql).all(...(params as never[])) as Record<string, unknown>[]
    },
    transaction<T>(fn: () => T): T {
      sqlite.exec('BEGIN')
      try {
        const result = fn()
        sqlite.exec('COMMIT')
        return result
      } catch (error) {
        sqlite.exec('ROLLBACK')
        throw error
      }
    },
  }
}

function projectServer(databaseID: string): { db: SyncDb; sync: SyncServer } {
  const db = nodeSqliteDb(new DatabaseSync(':memory:'))
  db.exec(`CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL)`)
  db.exec(`INSERT INTO item (id, label) VALUES ('seed', ?)`, [databaseID])
  const sync = createSyncServer({
    db,
    tables: {
      item: {
        columns: { id: 'string', label: 'string' },
        primaryKey: ['id'],
      },
    },
    mutate(tx, name, args) {
      if (name !== 'item.put') throw new Error(`unknown mutator ${name}`)
      const item = args as { id: string; label: string }
      tx.exec(
        `INSERT INTO item (id, label) VALUES (?, ?)
         ON CONFLICT (id) DO UPDATE SET label = excluded.label`,
        [item.id, item.label]
      )
    },
  })
  return { db, sync }
}

function pullBody(cookie: number | null) {
  return { clientID: 'c1', clientGroupID: 'shared-group', cookie }
}

function pushBody(label: string) {
  return {
    clientGroupID: 'shared-group',
    mutations: [
      {
        type: 'custom',
        id: 1,
        clientID: 'c1',
        name: 'item.put',
        args: [{ id: 'created', label }],
      },
    ],
    pushVersion: 1,
  }
}

type PullResponse = {
  cookie: number
  lastMutationIDChanges?: Record<string, number>
  rowsPatch?: unknown[]
}

describe('createSyncServerMount', () => {
  test('matches one safe database segment and one protocol operation', () => {
    let resolutions = 0
    const mount = createSyncServerMount({
      pathPrefix: '/p-',
      server() {
        resolutions++
        throw new Error('route matching must not resolve a database')
      },
    })

    expect(mount.match('/p-project_1/pull')).toMatchObject({
      databaseID: 'project_1',
      operation: 'pull',
    })
    expect(mount.match('/p-project-2/push')).toMatchObject({
      databaseID: 'project-2',
      operation: 'push',
    })
    for (const path of [
      '/pull',
      '/p-/pull',
      '/p-project/query',
      '/p-project/pull/extra',
      '/p-../pull',
      '/p-%2e/pull',
      '/p-project%2Fother/pull',
      `/p-${'x'.repeat(65)}/pull`,
    ]) {
      expect(mount.match(path), path).toBeNull()
    }
    expect(resolutions).toBe(0)
  })

  test('routes identical client state to independent project databases', () => {
    const projects = new Map<string, ReturnType<typeof projectServer>>()
    const mount = createSyncServerMount({
      pathPrefix: '/p-',
      server(databaseID) {
        let project = projects.get(databaseID)
        if (!project) {
          project = projectServer(databaseID)
          projects.set(databaseID, project)
        }
        return project.sync
      },
    })

    const alphaPull = mount.match('/p-alpha/pull')!
    const alphaPush = mount.match('/p-alpha/push')!
    const betaPull = mount.match('/p-beta/pull')!
    const betaPush = mount.match('/p-beta/push')!

    const alphaInitial = mount.handle(
      alphaPull,
      pullBody(null),
      'alpha-user'
    ) as PullResponse
    const betaInitial = mount.handle(
      betaPull,
      pullBody(null),
      'beta-user'
    ) as PullResponse
    expect(alphaInitial.cookie).toBe(0)
    expect(betaInitial.cookie).toBe(0)
    expect(alphaInitial.rowsPatch).toContainEqual({
      op: 'put',
      tableName: 'item',
      value: { id: 'seed', label: 'alpha' },
    })
    expect(betaInitial.rowsPatch).toContainEqual({
      op: 'put',
      tableName: 'item',
      value: { id: 'seed', label: 'beta' },
    })

    mount.handle(alphaPush, pushBody('only alpha'), 'alpha-user')
    const alphaChanged = mount.handle(
      alphaPull,
      pullBody(0),
      'alpha-user'
    ) as PullResponse
    expect(alphaChanged.cookie).toBe(2)
    expect(alphaChanged.lastMutationIDChanges).toEqual({ c1: 1 })
    expect(alphaChanged.rowsPatch).toEqual([
      {
        op: 'put',
        tableName: 'item',
        value: { id: 'created', label: 'only alpha' },
      },
    ])
    expect(mount.handle(betaPull, pullBody(0), 'beta-user')).toEqual({
      cookie: 0,
      unchanged: true,
    })

    expect(() => mount.handle(alphaPull, pullBody(0), 'beta-user')).toThrowError(
      SyncHttpError
    )
    mount.handle(betaPush, pushBody('only beta'), 'beta-user')
    expect(
      projects.get('alpha')!.db.all(`SELECT label FROM item WHERE id = 'created'`)
    ).toEqual([{ label: 'only alpha' }])
    expect(
      projects.get('beta')!.db.all(`SELECT label FROM item WHERE id = 'created'`)
    ).toEqual([{ label: 'only beta' }])
    expect(projects.get('alpha')!.sync.watermark()).toBe(2)
    expect(projects.get('beta')!.sync.watermark()).toBe(2)
  })
})
