import { createServer } from 'node:http'

import { afterEach, describe, expect, test } from 'vitest'

import { connectRoom, type RoomClient } from './client.js'
import { attachRoomServer } from './node.js'

// real sockets through a real HTTP server, as the Vite dev server hosts them.
async function serve() {
  const server = createServer()
  const rooms = attachRoomServer(server, { limits: { tickHz: 60 } })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('no port')
  return {
    url: (name: string) => `ws://127.0.0.1:${address.port}/__orez/room/${name}`,
    rooms,
    close: () => {
      rooms.close()
      return new Promise((resolve) => server.close(resolve))
    },
  }
}

const until = async (check: () => boolean, ms = 3000) => {
  const start = Date.now()
  while (!check()) {
    if (Date.now() - start > ms) throw new Error('timed out')
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

let cleanup: Array<() => unknown> = []
afterEach(async () => {
  for (const fn of cleanup.reverse()) await fn()
  cleanup = []
})

async function setup() {
  const host = await serve()
  cleanup.push(host.close)
  const join = async (name: string, room = 'race') => {
    const client = connectRoom({ url: host.url(room), meta: { name } })
    cleanup.push(() => client.close())
    await until(() => client.status === 'open')
    return client
  }
  return { host, join }
}

describe('rooms', () => {
  test('members see each other, and each state arrives in the other’s snapshots', async () => {
    const { join } = await setup()
    const a = await join('a')
    const b = await join('b')
    await until(() => a.members.size === 2 && b.members.size === 2)
    expect(b.members.get(a.id!)).toEqual({ name: 'a' })

    // in one process the host clock and the estimate agree closely.
    await until(() => a.rtt > 0)
    expect(Math.abs(a.now() - (performance.timeOrigin + performance.now()))).toBeLessThan(
      20
    )

    for (let i = 1; i <= 5; i++) {
      a.sendState(new Uint8Array([i]), a.now())
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    await until(() => b.latest(a.id!)?.payload[0] === 5)
    // a member never receives its own state back.
    expect(a.latest(a.id!)).toBeNull()
    // drawn in the past, between two samples it has.
    const frame = b.frame(a.id!, b.latest(a.id!)!.sampledAt - 30)
    expect(frame).not.toBeNull()
    expect(frame!.alpha).toBeGreaterThanOrEqual(0)
    expect(frame!.alpha).toBeLessThanOrEqual(1)
    expect(frame!.to.payload[0]! - frame!.from.payload[0]!).toBe(1)
  })

  test('events are ordered by the host, a claim goes to the first, and keys outlive nobody but persist', async () => {
    const { join } = await setup()
    const a = await join('a')
    const b = await join('b')
    const seen: Array<{ seq: number; data: unknown }> = []

    const results = await Promise.allSettled([
      a.send({ key: 'box:1', data: 'a', ifAbsent: true }),
      b.send({ key: 'box:1', data: 'b', ifAbsent: true }),
    ])
    const won = results.filter((r) => r.status === 'fulfilled')
    expect(won).toHaveLength(1)
    expect(results.find((r) => r.status === 'rejected')!.reason.message).toContain(
      'claimed'
    )

    const start = await a.send({ key: 'start', data: { at: 123 }, persist: true })
    await a.send({ key: 'ready:a', data: true })
    expect(start.seq).toBeGreaterThan(0)

    // a late joiner gets the retained state in order.
    const late = await join('late')
    expect([...late.retained.keys()].sort()).toEqual(['box:1', 'ready:a', 'start'].sort())
    for (const event of late.retained.values()) seen.push(event)
    expect(seen.map((e) => e.seq)).toEqual(
      [...seen.map((e) => e.seq)].sort((x, y) => x - y)
    )

    // leaving takes the leaver's unpersisted keys with it, and keeps the
    // persisted one.
    const aId = a.id
    a.close()
    await until(() => !late.members.has(aId!))
    expect(late.retained.has('ready:a')).toBe(false)
    expect(late.retained.has('start')).toBe(true)
    const claimer: RoomClient<unknown> = late.retained.get('box:1')?.from === b.id ? b : a
    expect(late.retained.has('box:1')).toBe(claimer === b)
  })

  test('rooms are separate and go away when empty', async () => {
    const { host, join } = await setup()
    const a = await join('a', 'one')
    const b = await join('b', 'two')
    expect(a.members.size).toBe(1)
    expect(b.members.size).toBe(1)
    expect(host.rooms.rooms().sort()).toEqual(['one', 'two'])
    a.close()
    await until(() => host.rooms.rooms().length === 1)
  })
})
