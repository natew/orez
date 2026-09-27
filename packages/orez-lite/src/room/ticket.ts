/**
 * A room ticket is the app server's word that one user may join one room
 * until a moment it names. The app checks whoever asks (their session, their
 * access to what the room is about) and signs a ticket; every host checks the
 * signature before a socket joins, so rooms need no session lookup of their
 * own and admit nobody the app did not.
 *
 * `<expiry ms>.<base64url user>.<base64url HMAC-SHA256 of
 * "orez-room\n<room>\n<user>\n<expiry>">`, keyed by a secret only the app's
 * server and its room host hold. The room trusts the user it names, so a
 * member cannot pass for someone else and one user's members are counted.
 */

import { ROOM_PATH, ROOM_TICKET_PARAM } from './path.js'

// the client asks for a fresh one on every connect, so it only has to live
// until the socket opens; a leaked one (it rides in a URL) dies quickly.
const DEFAULT_TTL_MS = 60_000

const encoder = new TextEncoder()

const key = (secret: string, usage: 'sign' | 'verify') => {
  if (!secret) throw new Error('a room ticket needs a secret')
  return crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    [usage]
  )
}

const payload = (room: string, user: string, expires: number) =>
  encoder.encode(`orez-room\n${room}\n${user}\n${expires}`)

const toBase64Url = (bytes: Uint8Array) =>
  btoa(String.fromCharCode(...bytes))
    .replaceAll('+', '-')
    .replaceAll('/', '_')
    .replace(/=+$/, '')

function fromBase64Url(text: string): Uint8Array<ArrayBuffer> {
  const binary = atob(text.replaceAll('-', '+').replaceAll('_', '/'))
  return Uint8Array.from(binary, (char) => char.charCodeAt(0))
}

export async function signRoomTicket(
  secret: string,
  room: string,
  user: string,
  options: { ttlMs?: number; now?: number } = {}
) {
  const expires = (options.now ?? Date.now()) + (options.ttlMs ?? DEFAULT_TTL_MS)
  const signature = await crypto.subtle.sign(
    'HMAC',
    await key(secret, 'sign'),
    payload(room, user, expires)
  )
  return `${expires}.${toBase64Url(encoder.encode(user))}.${toBase64Url(new Uint8Array(signature))}`
}

// the user an unexpired ticket for this room was signed for with this secret,
// or null.
export async function verifyRoomTicket(
  secret: string,
  room: string,
  ticket: string | null | undefined,
  now = Date.now()
): Promise<string | null> {
  const parts = ticket?.split('.') ?? []
  if (parts.length !== 3) return null
  const expires = Number(parts[0])
  if (!/^\d+$/.test(parts[0]!) || !Number.isSafeInteger(expires) || expires <= now)
    return null
  let user: string
  let signature: Uint8Array<ArrayBuffer>
  try {
    user = new TextDecoder('utf-8', { fatal: true }).decode(fromBase64Url(parts[1]!))
    signature = fromBase64Url(parts[2]!)
  } catch {
    return null
  }
  if (!user) return null
  const valid = await crypto.subtle.verify(
    'HMAC',
    await key(secret, 'verify'),
    signature,
    payload(room, user, expires)
  )
  return valid ? user : null
}

// the room a request to `url` may join and who for, once the path names a
// room and the ticket checks out; the status to turn it away with; or null for
// a path that is not a room's.
export async function admitRoom(
  url: URL,
  secret: string,
  prefix = ROOM_PATH
): Promise<{ room: string; user: string } | 400 | 403 | null> {
  if (!url.pathname.startsWith(prefix)) return null
  let name: string
  try {
    name = decodeURIComponent(url.pathname.slice(prefix.length))
  } catch {
    return 400
  }
  if (!name || name.length > 256) return 400
  const user = await verifyRoomTicket(
    secret,
    name,
    url.searchParams.get(ROOM_TICKET_PARAM)
  )
  return user === null ? 403 : { room: name, user }
}
