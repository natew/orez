import { MessageChannel } from 'node:worker_threads'

import { describe, expect, it, vi } from 'vitest'

import { createBrowserProxy } from './pg-proxy-browser.js'

function startupPacket(database = 'postgres'): Uint8Array {
  const params = new TextEncoder().encode(`user\0user\0database\0${database}\0\0`)
  const packet = new Uint8Array(8 + params.byteLength)
  const view = new DataView(packet.buffer)
  view.setInt32(0, packet.byteLength)
  view.setInt32(4, 196_608)
  packet.set(params, 8)
  return packet
}

function instances(closeSession: () => Promise<void>) {
  const root = {
    createProtocolSession: vi.fn(() => ({
      close: closeSession,
    })),
  }
  return {
    instances: {
      cdb: root,
      cvr: root,
      postgres: root,
      postgresReplicas: [],
    } as any,
    root,
  }
}

async function connect(proxy: Awaited<ReturnType<typeof createBrowserProxy>>) {
  const channel = new MessageChannel()
  proxy.handleConnection(channel.port1 as unknown as MessagePort)
  const packet = startupPacket()
  channel.port2.postMessage(packet.buffer)
  return channel
}

describe('BrowserProxy.close', () => {
  it('closes a connection that has not sent its startup packet', async () => {
    const db = {
      exec: vi.fn(async () => []),
      query: vi.fn(async () => ({ rows: [] })),
    }
    const proxy = await createBrowserProxy(db as any, {
      pgPassword: '',
      pgUser: 'user',
    })
    const channel = new MessageChannel()
    const peerClosed = new Promise<void>((resolve) => {
      channel.port2.once('close', resolve)
    })
    proxy.handleConnection(channel.port1 as unknown as MessagePort)

    await proxy.close()
    await peerClosed
  })

  it('returns one promise and waits for every protocol session close', async () => {
    let releaseFirst!: () => void
    let releaseSecond!: () => void
    const first = new Promise<void>((resolve) => {
      releaseFirst = resolve
    })
    const second = new Promise<void>((resolve) => {
      releaseSecond = resolve
    })
    const closes = vi.fn().mockReturnValueOnce(first).mockReturnValueOnce(second)
    const { instances: dbs, root } = instances(closes)
    const proxy = await createBrowserProxy(dbs, { pgPassword: '', pgUser: 'user' })
    const channels = [await connect(proxy), await connect(proxy)]
    await vi.waitFor(() => expect(root.createProtocolSession).toHaveBeenCalledTimes(2))

    let settled = false
    const closing = proxy.close()
    const sameClosing = proxy.close()
    void closing.then(() => {
      settled = true
    })
    expect(sameClosing).toBe(closing)
    await vi.waitFor(() => expect(closes).toHaveBeenCalledTimes(2))

    releaseFirst()
    await Promise.resolve()
    expect(settled).toBe(false)
    releaseSecond()
    await closing
    expect(settled).toBe(true)

    for (const channel of channels) channel.port2.close()
  })

  it('waits for all sessions and surfaces rejected session cleanup', async () => {
    const rejected = new Error('session close failed')
    let releaseDelayed!: () => void
    const delayed = new Promise<void>((resolve) => {
      releaseDelayed = resolve
    })
    const closes = vi.fn().mockRejectedValueOnce(rejected).mockReturnValueOnce(delayed)
    const { instances: dbs, root } = instances(closes)
    const proxy = await createBrowserProxy(dbs, { pgPassword: '', pgUser: 'user' })
    const channels = [await connect(proxy), await connect(proxy)]
    await vi.waitFor(() => expect(root.createProtocolSession).toHaveBeenCalledTimes(2))

    let settled = false
    const closing = proxy.close().catch((error) => {
      settled = true
      return error
    })
    await Promise.resolve()
    expect(settled).toBe(false)
    releaseDelayed()
    const error = await closing

    expect(error).toBeInstanceOf(AggregateError)
    expect((error as AggregateError).errors).toContain(rejected)
    expect(closes).toHaveBeenCalledTimes(2)
    for (const channel of channels) channel.port2.close()
  })

  it('retains a client-initiated session cleanup failure for proxy teardown', async () => {
    const rejected = new Error('client session close failed')
    const closes = vi.fn().mockRejectedValue(rejected)
    const { instances: dbs, root } = instances(closes)
    const proxy = await createBrowserProxy(dbs, { pgPassword: '', pgUser: 'user' })
    const channel = await connect(proxy)
    await vi.waitFor(() => expect(root.createProtocolSession).toHaveBeenCalledOnce())

    channel.port2.postMessage(new Uint8Array([0x70, 0, 0, 0, 4]).buffer)
    await new Promise((resolve) => setTimeout(resolve, 0))
    channel.port2.postMessage(new Uint8Array([0x58, 0, 0, 0, 4]).buffer)
    await vi.waitFor(() => expect(closes).toHaveBeenCalledOnce())

    const error = await proxy.close().catch((reason) => reason)
    expect(error).toBeInstanceOf(AggregateError)
    expect((error as AggregateError).errors).toContain(rejected)
    channel.port2.close()
  })

  it('rolls back open external sessions before resolving', async () => {
    let releaseRollback!: () => void
    const rollbackGate = new Promise<void>((resolve) => {
      releaseRollback = resolve
    })
    const db = {
      exec: vi.fn(async (sql: string) => {
        if (sql === 'ROLLBACK') await rollbackGate
        return []
      }),
      query: vi.fn(async () => ({ rows: [] })),
    }
    const proxy = await createBrowserProxy(db as any, {
      pgPassword: '',
      pgUser: 'user',
    })
    const session = proxy.createExternalSession()
    await session.exec('BEGIN')

    let settled = false
    const closing = proxy.close().then(() => {
      settled = true
    })
    await vi.waitFor(() => expect(db.exec).toHaveBeenCalledWith('ROLLBACK'))
    expect(settled).toBe(false)

    releaseRollback()
    await closing
    expect(settled).toBe(true)
  })

  it('waits for an in-flight external operation before resolving', async () => {
    let releaseExec!: () => void
    const execGate = new Promise<void>((resolve) => {
      releaseExec = resolve
    })
    const db = {
      exec: vi.fn(async () => {
        await execGate
        return []
      }),
      query: vi.fn(async () => ({ rows: [] })),
    }
    const proxy = await createBrowserProxy(db as any, {
      pgPassword: '',
      pgUser: 'user',
    })
    const operation = proxy.exec('postgres', 'SELECT 1')
    await vi.waitFor(() => expect(db.exec).toHaveBeenCalledOnce())

    let settled = false
    const closing = proxy.close().then(() => {
      settled = true
    })
    await Promise.resolve()
    expect(settled).toBe(false)

    releaseExec()
    await Promise.all([operation, closing])
    expect(settled).toBe(true)
  })

  it('rolls back the default external owner and reports rollback failures', async () => {
    const rejected = new Error('external rollback failed')
    const db = {
      exec: vi.fn(async (sql: string) => {
        if (sql === 'ROLLBACK') throw rejected
        return []
      }),
      query: vi.fn(async () => ({ rows: [] })),
    }
    const proxy = await createBrowserProxy(db as any, {
      pgPassword: '',
      pgUser: 'user',
    })
    await proxy.exec('postgres', 'BEGIN')

    const error = await proxy.close().catch((reason) => reason)

    expect(error).toBeInstanceOf(AggregateError)
    expect((error as AggregateError).errors).toContain(rejected)
    expect(db.exec).toHaveBeenCalledWith('ROLLBACK')
  })
})
