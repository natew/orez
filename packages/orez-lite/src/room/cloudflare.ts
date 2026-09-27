import { createRoomHost, type RoomHost, type RoomLimits } from './host.js'
import { ROOM_PATH } from './path.js'

/**
 * Rooms on Cloudflare: one Durable Object per room name, so every member of a
 * room reaches the same instance and the room's order is that object's order.
 * Bind the class as `OREZ_ROOM_DO` and `createOrezDataWorker` routes
 * `/__orez/room/<name>` to it.
 */

type WorkersSocket = WebSocket & { accept(): void }
declare const WebSocketPair: { new (): { 0: WebSocket; 1: WorkersSocket } }

export type RoomNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): { fetch(request: Request): Promise<Response> }
}

export class OrezRoomDO {
  #host: RoomHost
  #timer: ReturnType<typeof setInterval> | null = null

  constructor(_ctx: unknown, _env: unknown, limits?: Partial<RoomLimits>) {
    this.#host = createRoomHost({ now: () => Date.now(), limits })
  }

  async fetch(request: Request) {
    if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket')
      return new Response('expected a websocket', { status: 426 })
    const pair = new WebSocketPair()
    const client = pair[0]
    const server = pair[1]
    server.accept()
    const connection = this.#host.open({
      send: (data) => server.send(data),
      close: (code, reason) => server.close(code, reason),
      // workerd does not report a socket's queue; it applies its own limits.
      buffered: () => 0,
    })
    const closed = () => {
      connection.close()
      if (this.#host.size === 0 && this.#timer) {
        clearInterval(this.#timer)
        this.#timer = null
      }
    }
    server.addEventListener('message', (event) =>
      connection.message(
        typeof event.data === 'string'
          ? event.data
          : new Uint8Array(event.data as ArrayBuffer)
      )
    )
    server.addEventListener('close', closed)
    server.addEventListener('error', closed)
    this.#timer ??= setInterval(() => this.#host.tick(), 1000 / this.#host.limits.tickHz)
    return new Response(null, { status: 101, webSocket: client } as ResponseInit)
  }
}

// forwards a room upgrade to its object, or returns null for any other path.
export function routeRoom(request: Request, rooms: RoomNamespace) {
  const path = new URL(request.url).pathname
  if (!path.startsWith(ROOM_PATH)) return null
  const name = decodeURIComponent(path.slice(ROOM_PATH.length))
  if (!name || name.length > 256) return new Response('invalid room', { status: 400 })
  return rooms.get(rooms.idFromName(name)).fetch(request)
}
