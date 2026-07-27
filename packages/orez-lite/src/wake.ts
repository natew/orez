const TOKEN_VERSION = 'v1'
const SIGNATURE_CONTEXT = 'orez-wake-capability-v1:'
const MAX_TOKEN_LENGTH = 2048
const MAX_NAMESPACE_LENGTH = 200
const MAX_IDENTITY_LENGTH = 200

export type WakeCapability = {
  readonly namespace: string
  readonly identity: string
  readonly expiresAt: number
}

export type MintWakeCapabilityOptions = {
  readonly namespace: string
  readonly identity: string
  readonly ttlMs: number
}

export type VerifyWakeCapabilityOptions = {
  readonly namespace: string
  readonly ttlMs: number
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

function validateSecret(secret: string): void {
  if (!secret) throw new TypeError('wake capability secret must not be empty')
}

function validateNamespace(namespace: string): void {
  if (!namespace || namespace.length > MAX_NAMESPACE_LENGTH) {
    throw new TypeError('wake capability namespace is invalid')
  }
}

function validateIdentity(identity: string): void {
  if (!identity || identity.length > MAX_IDENTITY_LENGTH) {
    throw new TypeError('wake capability identity is invalid')
  }
}

function validateTtl(ttlMs: number): void {
  if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) {
    throw new TypeError('wake capability ttlMs must be a positive integer')
  }
}

function encodeBase64Url(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '')
}

function decodeBase64Url(value: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) throw new TypeError('invalid base64url')
  const base64 = value.replaceAll('-', '+').replaceAll('_', '/')
  const binary = atob(base64.padEnd(Math.ceil(base64.length / 4) * 4, '='))
  return Uint8Array.from(binary, (character) => character.charCodeAt(0))
}

async function sign(secret: string, payload: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  )
  return new Uint8Array(
    await crypto.subtle.sign('HMAC', key, encoder.encode(SIGNATURE_CONTEXT + payload))
  )
}

export function timingSafeEqual(left: string | Uint8Array, right: string | Uint8Array) {
  const leftBytes = typeof left === 'string' ? encoder.encode(left) : left
  const rightBytes = typeof right === 'string' ? encoder.encode(right) : right
  const length = Math.max(leftBytes.length, rightBytes.length)
  let difference = leftBytes.length ^ rightBytes.length
  for (let index = 0; index < length; index++) {
    difference |= (leftBytes[index] ?? 0) ^ (rightBytes[index] ?? 0)
  }
  return difference === 0
}

export function verifySharedSecretHeader(
  request: Request,
  secret: string,
  headerName = 'x-admin-key'
): boolean {
  const received = request.headers.get(headerName)
  return received !== null && secret.length > 0 && timingSafeEqual(received, secret)
}

export async function mintWakeCapability(
  secret: string,
  options: MintWakeCapabilityOptions
): Promise<{ readonly token: string; readonly expiresAt: number }> {
  validateSecret(secret)
  validateNamespace(options.namespace)
  validateIdentity(options.identity)
  validateTtl(options.ttlMs)

  const expiresAt = Date.now() + options.ttlMs
  const payload = encodeBase64Url(
    encoder.encode(
      JSON.stringify({
        namespace: options.namespace,
        identity: options.identity,
        expiresAt,
      } satisfies WakeCapability)
    )
  )
  const signature = encodeBase64Url(await sign(secret, payload))
  return { token: `${TOKEN_VERSION}.${payload}.${signature}`, expiresAt }
}

export async function verifyWakeCapability(
  secret: string,
  token: string | null | undefined,
  options: VerifyWakeCapabilityOptions
): Promise<WakeCapability | null> {
  validateSecret(secret)
  validateNamespace(options.namespace)
  validateTtl(options.ttlMs)
  if (!token || token.length > MAX_TOKEN_LENGTH) return null

  const parts = token.split('.')
  if (parts.length !== 3 || parts[0] !== TOKEN_VERSION) return null
  const payload = parts[1]
  const encodedSignature = parts[2]
  if (!payload || !encodedSignature) return null

  try {
    const signature = decodeBase64Url(encodedSignature)
    if (!timingSafeEqual(signature, await sign(secret, payload))) return null

    const value: unknown = JSON.parse(decoder.decode(decodeBase64Url(payload)))
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null
    const capability = value as Record<string, unknown>
    const now = Date.now()
    if (
      capability.namespace !== options.namespace ||
      typeof capability.identity !== 'string' ||
      !capability.identity ||
      capability.identity.length > MAX_IDENTITY_LENGTH ||
      typeof capability.expiresAt !== 'number' ||
      !Number.isSafeInteger(capability.expiresAt) ||
      capability.expiresAt <= now ||
      capability.expiresAt > now + options.ttlMs
    ) {
      return null
    }
    return {
      namespace: options.namespace,
      identity: capability.identity,
      expiresAt: capability.expiresAt,
    }
  } catch {
    return null
  }
}
