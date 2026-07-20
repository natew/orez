import { afterEach, describe, expect, it } from 'vitest'

import { startZeroHttpServer } from './server.js'

type TestServer = Awaited<ReturnType<typeof startZeroHttpServer>>

let server: TestServer | undefined

afterEach(async () => {
  await server?.close()
  server = undefined
})

async function start(seed?: Parameters<typeof startZeroHttpServer>[0]['seed']) {
  server = await startZeroHttpServer({ seed })
  return server
}

async function pull(
  server: TestServer,
  token: string | null,
  body: { clientID?: string; clientGroupID?: string; cookie?: number | null } = {}
) {
  const headers: Record<string, string> = { 'content-type': 'application/json' }
  if (token) headers.authorization = `Bearer ${token}`

  const res = await fetch(`${server.url}/pull`, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      clientID: body.clientID ?? 'c1',
      clientGroupID: body.clientGroupID ?? 'cg1',
      cookie: body.cookie ?? null,
    }),
  })
  return { res, body: await res.json() }
}

async function push(
  server: TestServer,
  token: string,
  mutation: {
    clientID?: string
    id: number
    name: string
    args: Record<string, string>
  },
  clientGroupID = 'cg1'
) {
  const res = await fetch(`${server.url}/push`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${token}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      timestamp: Date.now(),
      clientGroupID,
      pushVersion: 1,
      requestID: `req-${mutation.id}`,
      mutations: [
        {
          type: 'custom',
          name: mutation.name,
          id: mutation.id,
          clientID: mutation.clientID ?? 'c1',
          args: [mutation.args],
        },
      ],
    }),
  })
  return { res, body: await res.json() }
}

function puts(body: { rowsPatch?: Array<Record<string, unknown>> }) {
  expect(body.rowsPatch?.[0]).toEqual({ op: 'clear' })
  const primaryKeys: Record<string, string> = {
    user_record: 'user_id',
    project_record: 'project_id',
    project_member: 'member_id',
  }
  return body.rowsPatch
    ?.slice(1)
    .map((op) => ({
      tableName: op.tableName,
      value: op.value,
    }))
    .sort((a, b) => {
      const aKey = primaryKeys[String(a.tableName)]
      const bKey = primaryKeys[String(b.tableName)]
      return `${a.tableName}:${(a.value as Record<string, string>)[aKey]}`.localeCompare(
        `${b.tableName}:${(b.value as Record<string, string>)[bKey]}`
      )
    })
}

describe('zero-http fixture server', () => {
  it('returns full snapshots scoped to the authed user', async () => {
    const server = await start({
      user: [
        { id: 'u1', name: 'ada' },
        { id: 'u2', name: 'ben' },
      ],
      project: [
        { id: 'p1', ownerId: 'u1', name: 'u1 project' },
        { id: 'p2', ownerId: 'u2', name: 'u2 shared' },
      ],
      member: [{ id: 'm1', projectId: 'p2', userId: 'u1' }],
    })

    const u1 = await pull(server, 'token-u1', { clientGroupID: 'cg-u1' })
    expect(u1.res.status).toBe(200)
    expect(puts(u1.body)).toEqual([
      {
        tableName: 'project_member',
        value: { member_id: 'm1', project_id: 'p2', user_id: 'u1' },
      },
      {
        tableName: 'project_record',
        value: { project_id: 'p1', owner_id: 'u1', project_name: 'u1 project' },
      },
      {
        tableName: 'project_record',
        value: { project_id: 'p2', owner_id: 'u2', project_name: 'u2 shared' },
      },
      { tableName: 'user_record', value: { user_id: 'u1', display_name: 'ada' } },
    ])

    const u2 = await pull(server, 'token-u2', { clientGroupID: 'cg-u2' })
    expect(u2.res.status).toBe(200)
    expect(puts(u2.body)).toEqual([
      {
        tableName: 'project_member',
        value: { member_id: 'm1', project_id: 'p2', user_id: 'u1' },
      },
      {
        tableName: 'project_record',
        value: { project_id: 'p2', owner_id: 'u2', project_name: 'u2 shared' },
      },
      { tableName: 'user_record', value: { user_id: 'u2', display_name: 'ben' } },
    ])

    expect((await pull(server, null, { clientGroupID: 'cg-missing' })).res.status).toBe(
      401
    )
    expect(
      (await pull(server, 'token-nope', { clientGroupID: 'cg-nope' })).res.status
    ).toBe(401)
  })

  it('returns unchanged pulls by cookie and full snapshots after a push', async () => {
    const server = await start({
      user: [{ id: 'u1', name: 'ada' }],
      project: [],
      member: [],
    })

    const first = await pull(server, 'token-u1')
    expect(first.res.status).toBe(200)
    expect(first.body.cookie).toBe(server.version())
    expect(first.body.rowsPatch).toBeDefined()

    const unchanged = await pull(server, 'token-u1', { cookie: first.body.cookie })
    expect(unchanged.res.status).toBe(200)
    expect(unchanged.body).toEqual({ cookie: first.body.cookie, unchanged: true })
    expect(unchanged.body.rowsPatch).toBeUndefined()

    const future = await pull(server, 'token-u1', { cookie: first.body.cookie + 100 })
    expect(future.res.status).toBe(409)
    expect(future.body.error).toContain('future cookie')

    const created = await push(server, 'token-u1', {
      id: 1,
      name: 'project|create',
      args: { id: 'p1', ownerId: 'u1', name: 'new project' },
    })
    expect(created.res.status).toBe(200)

    const changed = await pull(server, 'token-u1', { cookie: first.body.cookie })
    expect(changed.res.status).toBe(200)
    expect(changed.body.cookie).toBeGreaterThan(first.body.cookie)
    expect(changed.body.rowsPatch[0]).toEqual({ op: 'clear' })
    expect(puts(changed.body)).toContainEqual({
      tableName: 'project_record',
      value: { project_id: 'p1', owner_id: 'u1', project_name: 'new project' },
    })
  })

  it('applies pushes, exposes LMID changes, and acks replays idempotently', async () => {
    const server = await start({
      user: [{ id: 'u1', name: 'ada' }],
      project: [],
      member: [],
    })

    const created = await push(server, 'token-u1', {
      id: 1,
      name: 'project|create',
      args: { id: 'p1', ownerId: 'u1', name: 'new project' },
    })
    expect(created.res.status).toBe(200)
    expect(created.body).toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'c1', id: 1 }, result: {} }],
      },
    })
    expect(server.rows('project')).toEqual([
      { id: 'p1', ownerId: 'u1', name: 'new project' },
    ])

    const afterCreate = await pull(server, 'token-u1')
    expect(afterCreate.body.lastMutationIDChanges).toEqual({ c1: 1 })

    const replay = await push(server, 'token-u1', {
      id: 1,
      name: 'project|create',
      args: { id: 'p1', ownerId: 'u1', name: 'new project' },
    })
    expect(replay.res.status).toBe(200)
    expect(replay.body).toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'c1', id: 1 },
            result: {
              error: 'alreadyProcessed',
              details:
                'Ignoring mutation from c1 with ID 1 as it was already processed. Expected: 2',
            },
          },
        ],
      },
    })
    expect(server.rows('project')).toEqual([
      { id: 'p1', ownerId: 'u1', name: 'new project' },
    ])

    const afterReplay = await pull(server, 'token-u1')
    expect(afterReplay.body.lastMutationIDChanges).toEqual({ c1: 1 })
  })

  it('binds client groups to the first authenticated user', async () => {
    const server = await start({
      user: [
        { id: 'u1', name: 'ada' },
        { id: 'u2', name: 'ben' },
      ],
      project: [],
      member: [],
    })

    const created = await push(
      server,
      'token-u1',
      {
        id: 1,
        name: 'project|create',
        args: { id: 'p1', ownerId: 'u1', name: 'u1 project' },
      },
      'cg-u1'
    )
    expect(created.res.status).toBe(200)

    const u2Pull = await pull(server, 'token-u2', {
      clientGroupID: 'cg-u1',
    })
    expect(u2Pull.res.status).toBe(403)
    expect(u2Pull.body.error).toContain('client group belongs to a different user')

    const u2Push = await push(
      server,
      'token-u2',
      {
        id: 1,
        name: 'project|create',
        args: { id: 'p2', ownerId: 'u2', name: 'u2 project' },
      },
      'cg-u1'
    )
    expect(u2Push.res.status).toBe(403)
    expect(u2Push.body.error).toContain('client group belongs to a different user')
    expect(server.rows('project')).toEqual([
      { id: 'p1', ownerId: 'u1', name: 'u1 project' },
    ])

    const u1Pull = await pull(server, 'token-u1', {
      clientGroupID: 'cg-u1',
    })
    expect(u1Pull.res.status).toBe(200)
    expect(u1Pull.body.lastMutationIDChanges).toEqual({ c1: 1 })
  })

  it('advances LMID and cookie for app-error mutations without changing rows', async () => {
    const server = await start({
      user: [
        { id: 'u1', name: 'ada' },
        { id: 'u2', name: 'ben' },
      ],
      project: [{ id: 'p2', ownerId: 'u2', name: 'u2 project' }],
      member: [],
    })
    const beforeVersion = server.version()

    const missing = await push(server, 'token-u1', {
      id: 1,
      name: 'project|rename',
      args: { id: 'missing', name: 'ghost' },
    })
    expect(missing.res.status).toBe(200)
    expect(missing.body).toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'c1', id: 1 },
            result: { error: 'app', message: 'not-found', details: 'not-found' },
          },
        ],
      },
    })
    expect(server.rows('project')).toEqual([
      { id: 'p2', ownerId: 'u2', name: 'u2 project' },
    ])
    expect(server.version()).toBeGreaterThan(beforeVersion)

    const replayedMissing = await push(server, 'token-u1', {
      id: 1,
      name: 'project|rename',
      args: { id: 'missing', name: 'different ghost' },
    })
    expect(replayedMissing.res.status).toBe(200)
    expect(replayedMissing.body).toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'c1', id: 1 },
            result: {
              error: 'alreadyProcessed',
              details:
                'Ignoring mutation from c1 with ID 1 as it was already processed. Expected: 2',
            },
          },
        ],
      },
    })
    expect(server.rows('project')).toEqual([
      { id: 'p2', ownerId: 'u2', name: 'u2 project' },
    ])

    const afterMissing = await pull(server, 'token-u1', { cookie: beforeVersion })
    expect(afterMissing.body.cookie).toBeGreaterThan(beforeVersion)
    expect(afterMissing.body.lastMutationIDChanges).toEqual({ c1: 1 })
    expect(puts(afterMissing.body)).not.toContainEqual({
      tableName: 'project_record',
      value: { project_id: 'missing', owner_id: 'u1', project_name: 'ghost' },
    })

    const beforeForbidden = server.version()
    const forbidden = await push(server, 'token-u1', {
      id: 2,
      name: 'project|rename',
      args: { id: 'p2', name: 'stolen' },
    })
    expect(forbidden.res.status).toBe(200)
    expect(forbidden.body).toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'c1', id: 2 },
            result: { error: 'app', message: 'forbidden', details: 'forbidden' },
          },
        ],
      },
    })
    expect(server.rows('project')).toEqual([
      { id: 'p2', ownerId: 'u2', name: 'u2 project' },
    ])

    const afterForbidden = await pull(server, 'token-u1', {
      cookie: beforeForbidden,
    })
    expect(afterForbidden.body.cookie).toBeGreaterThan(beforeForbidden)
    expect(afterForbidden.body.lastMutationIDChanges).toEqual({ c1: 2 })
  })

  it('rejects out-of-order mutation ids without corrupting LMID state', async () => {
    const server = await start({
      user: [{ id: 'u1', name: 'ada' }],
      project: [],
      member: [],
    })
    const beforeVersion = server.version()

    const gap = await push(server, 'token-u1', {
      id: 2,
      name: 'project|create',
      args: { id: 'p-gap', ownerId: 'u1', name: 'gap' },
    })
    expect(gap.res.status).toBe(400)
    expect(gap.body.error).toContain('skips lmid')
    expect(server.version()).toBe(beforeVersion)
    expect(server.rows('project')).toEqual([])

    const afterGap = await pull(server, 'token-u1')
    expect(afterGap.body.lastMutationIDChanges).toEqual({ c1: 0 })
    expect(puts(afterGap.body)).toEqual([
      { tableName: 'user_record', value: { user_id: 'u1', display_name: 'ada' } },
    ])
  })

  it('handles member removal app errors and success', async () => {
    const server = await start({
      user: [
        { id: 'u1', name: 'ada' },
        { id: 'u2', name: 'ben' },
      ],
      project: [{ id: 'p2', ownerId: 'u2', name: 'u2 shared' }],
      member: [
        { id: 'm1', projectId: 'p2', userId: 'u1' },
        { id: 'm2', projectId: 'p2', userId: 'u2' },
      ],
    })

    const members = [
      { id: 'm1', projectId: 'p2', userId: 'u1' },
      { id: 'm2', projectId: 'p2', userId: 'u2' },
    ]

    const missing = await push(server, 'token-u2', {
      clientID: 'c-u2',
      id: 1,
      name: 'member|remove',
      args: { id: 'missing' },
    })
    expect(missing.res.status).toBe(200)
    expect(missing.body).toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'c-u2', id: 1 },
            result: { error: 'app', message: 'not-found', details: 'not-found' },
          },
        ],
      },
    })
    expect(server.rows('member')).toEqual(members)

    const forbidden = await push(
      server,
      'token-u1',
      {
        clientID: 'c-u1',
        id: 1,
        name: 'member|remove',
        args: { id: 'm1' },
      },
      'cg-u1'
    )
    expect(forbidden.res.status).toBe(200)
    expect(forbidden.body).toEqual({
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'c-u1', id: 1 },
            result: { error: 'app', message: 'forbidden', details: 'forbidden' },
          },
        ],
      },
    })
    expect(server.rows('member')).toEqual(members)

    const removed = await push(server, 'token-u2', {
      clientID: 'c-u2',
      id: 2,
      name: 'member|remove',
      args: { id: 'm1' },
    })
    expect(removed.res.status).toBe(200)
    expect(removed.body).toEqual({
      pushResponse: {
        mutations: [{ id: { clientID: 'c-u2', id: 2 }, result: {} }],
      },
    })
    expect(server.rows('member')).toEqual([{ id: 'm2', projectId: 'p2', userId: 'u2' }])
  })
})
