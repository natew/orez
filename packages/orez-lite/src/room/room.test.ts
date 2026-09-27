import { createServer } from 'node:http'

import { afterEach, describe, expect, test } from 'vitest'
import { WebSocket as RawSocket } from 'ws'

import { connectRoom, type RoomClient } from './client.js'
import { createRoomHost, type RoomSocket } from './host.js'
import { attachRoomServer } from './node.js'
import { signRoomTicket } from './ticket.js'

const SECRET = 'test-room-secret'

// real sockets through a real HTTP server, as the Vite dev server hosts them.
async function serve() {
  const server = createServer()
  const rooms = attachRoomServer(server, { secret: SECRET, limits: { tickHz: 60 } })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('no port')
  const base = (name: string) => `ws://127.0.0.1:${address.port}/__orez/room/${name}`
  return {
    base,
    // a fresh ticket for every connect, as an app hands them out.
    url: async (name: string, user = 'player') =>
      `${base(name)}?ticket=${await signRoomTicket(SECRET, name, user)}`,
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
    const client = connectRoom({ url: () => host.url(room, name), meta: { name } })
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

    // a claim is its holder's: nobody else drops it to take it.
    // the winner's event reached both before the loser's rejection did.
    const holder = a.retained.get('box:1')?.from === a.id ? a : b
    const other = holder === a ? b : a
    await expect(other.send({ key: 'box:1', data: null })).rejects.toThrow('claimed')
    await expect(other.send({ key: 'box:1', data: 'mine' })).rejects.toThrow('claimed')

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

  test('hostile input is turned away without taking the server down, and a late hello joins the live room', async () => {
    const { host, join } = await setup()
    // a raw socket speaks whatever the test tells it to.
    const raw = async (room: string) => {
      const socket = new RawSocket(await host.url(room))
      socket.on('error', () => {})
      return socket
    }
    const opened = (socket: RawSocket) =>
      new Promise<void>((resolve) => socket.once('open', () => resolve()))
    const ended = (socket: RawSocket) =>
      new Promise<void>((resolve) => socket.once('close', () => resolve()))
    for (const text of ['null', '42', '[]', '{"t":"event"}', '{"t":"ping"}']) {
      const socket = await raw('race')
      await opened(socket)
      socket.send(text)
      await ended(socket)
    }
    await ended(new RawSocket(`${host.base('%zz')}?ticket=x`).on('error', () => {}))

    // a socket that has not said hello yet keeps its room alive, so its
    // hello lands in the room the others are in.
    const lurker = await raw('race')
    await opened(lurker)
    const texts: Array<{ t: string; id?: number }> = []
    lurker.on('message', (data, binary) => {
      if (!binary) texts.push(JSON.parse(String(data)))
    })
    const a = await join('a')
    a.close()
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(host.rooms.rooms()).toEqual(['race'])
    lurker.send(JSON.stringify({ t: 'hello', v: 2, meta: { name: 'lurker' } }))
    await until(() => texts.some((m) => m.t === 'welcome'))
    const b = await join('b')
    await until(() => texts.some((m) => m.t === 'join'))
    expect(b.members.size).toBe(2)
    lurker.close()
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

  test('a socket joins only with an unexpired ticket signed for its room', async () => {
    const { host, join } = await setup()
    const status = (url: string) =>
      new Promise<number>((resolve) => {
        const socket = new RawSocket(url)
        socket.on('open', () => {
          socket.close()
          resolve(101)
        })
        socket.on('unexpected-response', (_request, response) => resolve(response.statusCode!))
        socket.on('error', () => {})
      })
    const ticket = (room: string, options?: { ttlMs?: number }) =>
      signRoomTicket(SECRET, room, 'u', options)
    expect(await status(host.base('race'))).toBe(403)
    expect(await status(`${host.base('race')}?ticket=${await ticket('other')}`)).toBe(403)
    expect(
      await status(`${host.base('race')}?ticket=${await ticket('race', { ttlMs: -1 })}`)
    ).toBe(403)
    const forged = `${host.base('race')}?ticket=${await signRoomTicket('not-the-secret', 'race', 'u')}`
    expect(await status(forged)).toBe(403)
    const [expires, user, signature] = (await ticket('race')).split('.')
    expect(
      await status(`${host.base('race')}?ticket=${Number(expires) + 1}.${user}.${signature}`)
    ).toBe(403)
    // a ticket cannot be moved to another user.
    const other = btoa('v').replace(/=+$/, '')
    expect(await status(`${host.base('race')}?ticket=${expires}.${other}.${signature}`)).toBe(403)
    // nothing turned away made a room.
    expect(host.rooms.rooms()).toEqual([])
    expect(await status(`${host.base('race')}?ticket=${await ticket('race')}`)).toBe(101)
    await join('a')
  })
})

describe('room host limits', () => {
  // a socket the test can see into. an attentive one echoes each mark as it
  // arrives, as the client does; an inattentive one never reads.
  function fakeSocket(attentive = true) {
    const socket = {
      texts: [] as Array<{ t: string; reason?: string; m?: string }>,
      closed: null as number | null,
      connection: null as { message(data: string): void } | null,
      send(data: string | Uint8Array) {
        if (typeof data !== 'string') return
        const message = JSON.parse(data)
        socket.texts.push(message)
        if (message.t === 'mark' && attentive)
          socket.connection?.message(JSON.stringify({ t: 'mark', m: message.m }))
      },
      close(code: number) {
        socket.closed = code
      },
    } satisfies RoomSocket & Record<string, unknown>
    return socket
  }
  function room(limits: Parameters<typeof createRoomHost>[0]['limits'] = {}) {
    let clock = 1_000_000
    const host = createRoomHost({ now: () => clock, limits })
    const join = (user: string, attentive = true) => {
      const socket = fakeSocket(attentive)
      const connection = host.open(socket, user)
      socket.connection = connection
      connection.message(JSON.stringify({ t: 'hello', v: 2 }))
      return { socket, connection }
    }
    return { host, join, advance: (ms: number) => (clock += ms) }
  }
  const event = (connection: { message(data: string): void }, ref: number, bytes = 900) =>
    connection.message(JSON.stringify({ t: 'event', ref, data: 'x'.repeat(bytes) }))
  const events = (socket: ReturnType<typeof fakeSocket>) =>
    socket.texts.filter((m) => m.t === 'event').length

  test('each member has its own event byte budget, so one cannot starve the rest', () => {
    const { join, advance } = room({ maxEventBytesPerSecond: 2000 })
    const a = join('a')
    const b = join('b')
    event(a.connection, 1)
    event(a.connection, 2)
    event(a.connection, 3)
    expect(a.socket.texts.at(-1)).toMatchObject({ t: 'reject', reason: 'rate' })
    // a's spending leaves b's allowance whole.
    event(b.connection, 1)
    event(b.connection, 2)
    expect(events(b.socket)).toBe(4)
    advance(1000)
    event(a.connection, 4)
    expect(events(b.socket)).toBe(5)
  })

  test('a receiver that stops reading is closed however it is hosted, and one that reads is not', () => {
    const { join } = room({
      maxQueuedBytes: 40 * 1024,
      maxEventBytesPerSecond: 1 << 20,
      maxEventsPerSecond: 1000,
    })
    const reader = join('reader')
    const stalled = join('stalled', false)
    const sender = join('sender')
    for (let ref = 1; ref <= 100; ref++) event(sender.connection, ref)
    expect(stalled.socket.closed).toBe(1013)
    expect(reader.socket.closed).toBeNull()
    expect(events(reader.socket)).toBe(100)
    // echoing a mark that was never sent proves nothing.
    const liar = join('liar', false)
    liar.connection.message(JSON.stringify({ t: 'mark', m: 'guessed' }))
    for (let ref = 101; ref <= 200; ref++) event(sender.connection, ref)
    expect(liar.socket.closed).toBe(1013)
    expect(reader.socket.closed).toBeNull()
  })

  test('one user holds a bounded number of places, and silent sockets give theirs back', () => {
    const { host, join, advance } = room({ maxMembersPerUser: 2 })
    join('x')
    join('x')
    expect(join('x').socket.closed).toBe(4004)
    const silent = fakeSocket()
    host.open(silent, 'y')
    const talker = join('z')
    host.tick()
    advance(10_001)
    host.tick()
    expect(silent.closed).toBe(4003)
    advance(50_000)
    talker.connection.message(JSON.stringify({ t: 'ping', c: 1 }))
    host.tick()
    expect(talker.socket.closed).toBeNull()
    advance(60_001)
    host.tick()
    expect(talker.socket.closed).toBe(4005)
  })
})
