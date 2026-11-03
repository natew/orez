import { WebSocketServer, type WebSocket } from 'ws'

import { createRoomHost, type RoomHost, type RoomLimits } from './host.js'
import { ROOM_PATH } from './path.js'
import { admitRoom } from './ticket.js'

import type { IncomingMessage, Server } from 'node:http'
import type { Duplex } from 'node:stream'

/**
 * Rooms on a Node HTTP server, for local development: a WebSocket upgrade to
 * `<prefix><name>?ticket=<ticket>` joins the room of that name, creating it on first join and
 * dropping it when the last member leaves. Other upgrades are left alone, so
 * it shares a server with Vite's own sockets.
 */

export type RoomServerOptions = {
  prefix?: string
  limits?: Partial<RoomLimits>
  // what the app signs room tickets with; a socket without one is turned away.
  secret: string
}

export function attachRoomServer(server: Server, options: RoomServerOptions) {
  if (!options.secret) throw new Error('rooms need a ticket secret')
  const prefix = options.prefix ?? ROOM_PATH
  const sockets = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 })
  // a room lives while any socket is open on it, joined or not yet, so a
  // hello that arrives late still finds the room everyone else is in.
  const rooms = new Map<
    string,
    { host: RoomHost; timer: ReturnType<typeof setInterval>; sockets: number }
  >()
  const origin = performance.timeOrigin

  const roomOf = (name: string) => {
    const existing = rooms.get(name)
    if (existing) return existing
    const host = createRoomHost({
      now: () => origin + performance.now(),
      limits: options.limits,
    })
    const room = {
      host,
      timer: setInterval(() => host.tick(), 1000 / host.limits.tickHz),
      sockets: 0,
    }
    rooms.set(name, room)
    return room
  }

  const join = (socket: WebSocket, name: string, user: string) => {
    const room = roomOf(name)
    room.sockets++
    const connection = room.host.open(
      {
        send: (data) => socket.send(data),
        close: (code, reason) => socket.close(code, reason),
      },
      user
    )
    socket.on('message', (data, binary) => {
      const bytes = Array.isArray(data)
        ? Buffer.concat(data)
        : Buffer.from(data as ArrayBuffer)
      connection.message(
        binary
          ? new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength)
          : bytes.toString('utf8')
      )
    })
    socket.on('close', () => {
      connection.close()
      if (--room.sockets === 0 && rooms.get(name) === room) {
        clearInterval(room.timer)
        rooms.delete(name)
      }
    })
  }

  const onUpgrade = async (request: IncomingMessage, socket: Duplex, head: Buffer) => {
    const admitted = await admitRoom(
      new URL(request.url ?? '/', 'http://room'),
      options.secret,
      prefix
    )
    if (admitted === null) return
    if (typeof admitted === 'number') {
      socket.end(
        admitted === 400
          ? 'HTTP/1.1 400 Bad Request\r\n\r\n'
          : 'HTTP/1.1 403 Forbidden\r\n\r\n'
      )
      return
    }
    sockets.handleUpgrade(request, socket, head, (ws) =>
      join(ws, admitted.room, admitted.user)
    )
  }
  server.on('upgrade', onUpgrade)

  return {
    rooms: () => [...rooms.keys()],
    close() {
      server.off('upgrade', onUpgrade)
      for (const room of rooms.values()) clearInterval(room.timer)
      rooms.clear()
      for (const client of sockets.clients) client.terminate()
      sockets.close()
    },
  }
}
