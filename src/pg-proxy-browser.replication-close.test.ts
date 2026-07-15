import { MessageChannel } from 'node:worker_threads'

import { describe, expect, it, vi } from 'vitest'

const replicationHarness = vi.hoisted(() => ({
  handleStartReplication: vi.fn(),
  signalReplicationChange: vi.fn(),
}))

vi.mock('./replication/handler.js', () => ({
  createReplicationFeedbackParser: () => () => {},
  handleReplicationQuery: vi.fn(),
  handleStartReplication: (...args: unknown[]) =>
    replicationHarness.handleStartReplication(...args),
  noteConfirmedFlushLsn: vi.fn(),
  signalReplicationChange: (...args: unknown[]) =>
    replicationHarness.signalReplicationChange(...args),
}))

import { createBrowserProxy } from './pg-proxy-browser.js'

function gate() {
  let resolve!: () => void
  const promise = new Promise<void>((onResolve) => {
    resolve = onResolve
  })
  return { promise, resolve }
}

function startupPacket(): Uint8Array {
  const params = new TextEncoder().encode(
    'user\0user\0database\0postgres\0replication\0database\0\0'
  )
  const packet = new Uint8Array(8 + params.byteLength)
  const view = new DataView(packet.buffer)
  view.setInt32(0, packet.byteLength)
  view.setInt32(4, 196_608)
  packet.set(params, 8)
  return packet
}

function queryPacket(sql: string): Uint8Array {
  const query = new TextEncoder().encode(`${sql}\0`)
  const packet = new Uint8Array(5 + query.byteLength)
  packet[0] = 0x51
  new DataView(packet.buffer).setInt32(1, 4 + query.byteLength)
  packet.set(query, 5)
  return packet
}

describe('BrowserProxy replication shutdown', () => {
  it('aborts and waits for an in-flight replication task before resolving', async () => {
    const replication = gate()
    replicationHarness.handleStartReplication.mockReturnValue(replication.promise)
    const db = {
      exec: vi.fn(async () => []),
      query: vi.fn(async () => ({ rows: [] })),
    }
    const proxy = await createBrowserProxy(db as any, {
      instanceId: 'replication-close-test',
      pgPassword: '',
      pgUser: 'user',
    })
    const channel = new MessageChannel()
    proxy.handleConnection(channel.port1 as unknown as MessagePort)
    channel.port2.postMessage(startupPacket().buffer)
    await new Promise((resolve) => channel.port2.once('message', resolve))
    channel.port2.postMessage(new Uint8Array([0x70, 0, 0, 0, 5, 0]).buffer)
    await new Promise((resolve) => channel.port2.once('message', resolve))
    channel.port2.postMessage(queryPacket('START_REPLICATION SLOT test').buffer)
    await vi.waitFor(() =>
      expect(replicationHarness.handleStartReplication).toHaveBeenCalledOnce()
    )

    let settled = false
    const closing = proxy.close().then(() => {
      settled = true
    })
    await Promise.resolve()

    expect(settled).toBe(false)
    replication.resolve()
    await closing
    expect(settled).toBe(true)
    expect(replicationHarness.signalReplicationChange).toHaveBeenCalledWith(
      'replication-close-test'
    )
    channel.port2.close()
  })
})
