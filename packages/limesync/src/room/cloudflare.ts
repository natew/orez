import { createRoomHost, type RoomHost, type RoomLimits } from './host.js'
import { admitRoom } from './ticket.js'

/**
 * Rooms on Cloudflare: one Durable Object per room name, so every member of a
 * room reaches the same instance and the room's order is that object's order.
 * Bind the class as `OREZ_ROOM_DO` and the room ticket secret as
 * `OREZ_ROOM_SECRET`, and `createOrezDataWorker` routes `/__orez/room/<name>`
 * to it.
 */

type WorkersSocket = WebSocket & { accept(): void }
declare const WebSocketPair: { new (): { 0: WebSocket; 1: WorkersSocket } }

// how routeRoom tells a room's object who the ticket was for.
const ROOM_USER_HEADER = 'x-orez-room-user'

export type RoomNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): { fetch(request: Request): Promise<Response> }
}

export class OrezRoomDO {
  #host: RoomHost
  #timer: ReturnType<typeof setInterval> | null = null
  // the room ticks while any socket is open, joined or not yet, so a hello
  // that arrives late is still answered and a silent socket meets its deadline.
  #sockets = 0

  constructor(_ctx: unknown, _env: unknown, limits?: Partial<RoomLimits>) {
    this.#host = createRoomHost({ now: () => Date.now(), limits })
  }

  // reached only through routeRoom, which checked the ticket and names its user.
  async fetch(request: Request) {
    if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket')
      return new Response('expected a websocket', { status: 426 })
    let user = ''
    try {
      user = decodeURIComponent(request.headers.get(ROOM_USER_HEADER) ?? '')
    } catch {}
    if (!user) return new Response('no room user', { status: 403 })
    const pair = new WebSocketPair()
    const client = pair[0]
    const server = pair[1]
    server.accept()
    const connection = this.#host.open(
      {
        send: (data) => server.send(data),
        // the room lets go as it closes, without waiting on a peer that may
        // never answer the close.
        close: (code, reason) => {
          server.close(code, reason)
          closed()
        },
      },
      user
    )
    this.#sockets++
    let open = true
    const closed = () => {
      if (!open) return
      open = false
      connection.close()
      if (--this.#sockets === 0 && this.#timer) {
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

// forwards a room upgrade whose ticket checks out to its object, turns any
// other request for a room away, and returns null for a path that is not a
// room's. only the name picks the object, so the ticket is checked first.
export async function routeRoom(request: Request, rooms: RoomNamespace, secret: string) {
  const admitted = await admitRoom(new URL(request.url), secret)
  if (admitted === null) return null
  if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket')
    return new Response('expected a websocket', { status: 426 })
  if (typeof admitted === 'number')
    return new Response('not admitted to this room', { status: admitted })
  // set, never taken from the caller: only a checked ticket names the user.
  const forwarded = new Request(request)
  forwarded.headers.set(ROOM_USER_HEADER, encodeURIComponent(admitted.user))
  return rooms.get(rooms.idFromName(admitted.room)).fetch(forwarded)
}
