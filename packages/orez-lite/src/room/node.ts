import { WebSocketServer, type WebSocket } from 'ws'

import { createRoomHost, type RoomHost, type RoomLimits } from './host.js'
import { ROOM_PATH } from './path.js'

import type { IncomingMessage, Server } from 'node:http'
import type { Duplex } from 'node:stream'

/**
 * Rooms on a Node HTTP server, for local development: a WebSocket upgrade to
 * `<prefix><name>` joins the room of that name, creating it on first join and
 * dropping it when the last member leaves. Other upgrades are left alone, so
 * it shares a server with Vite's own sockets.
 */

export type RoomServerOptions = {
  prefix?: string
  limits?: Partial<RoomLimits>
  // turns a request away before it joins; the room name is already parsed.
  authorize?: (request: IncomingMessage, room: string) => boolean | Promise<boolean>
}

export function attachRoomServer(server: Server, options: RoomServerOptions = {}) {
  const prefix = options.prefix ?? ROOM_PATH
  const sockets = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 })
  const rooms = new Map<
    string,
    { host: RoomHost; timer: ReturnType<typeof setInterval> }
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
    }
    rooms.set(name, room)
    return room
  }

  const join = (socket: WebSocket, name: string) => {
    const room = roomOf(name)
    const connection = room.host.open({
      send: (data) => socket.send(data),
      close: (code, reason) => socket.close(code, reason),
      buffered: () => socket.bufferedAmount,
    })
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
      if (room.host.size === 0 && rooms.get(name) === room) {
        clearInterval(room.timer)
        rooms.delete(name)
      }
    })
  }

  const onUpgrade = async (request: IncomingMessage, socket: Duplex, head: Buffer) => {
    const path = new URL(request.url ?? '/', 'http://room').pathname
    if (!path.startsWith(prefix)) return
    const name = decodeURIComponent(path.slice(prefix.length))
    if (
      !name ||
      name.length > 256 ||
      (options.authorize && !(await options.authorize(request, name)))
    ) {
      socket.end('HTTP/1.1 403 Forbidden\r\n\r\n')
      return
    }
    sockets.handleUpgrade(request, socket, head, (ws) => join(ws, name))
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
