// The harness wake capability: a short-lived, namespace-scoped, HMAC-signed
// token naming the user it was issued to.
//
// Its own module because both sides need it and they run in different
// runtimes: the worker verifies it inside a Durable Object, and a harness lane
// mints it under Bun. harness-config imports 'cloudflare:workers' through the
// host, so a Node-side caller cannot reach in there for it.

const WAKE_TOKEN_TTL_MS = 60_000

type HarnessWakeTokenPayload = {
  namespace: string
  userID: string
  expiresAt: number
}

export async function mintHarnessWakeToken(
  namespace: string,
  userID: string,
  secret: string
): Promise<{ token: string; expiresAt: number }> {
  const expiresAt = Date.now() + WAKE_TOKEN_TTL_MS
  const payload = encodeBase64URL(
    new TextEncoder().encode(JSON.stringify({ namespace, userID, expiresAt }))
  )
  return {
    token: `${payload}.${await signWakeToken(payload, secret)}`,
    expiresAt,
  }
}

// Returns the token's userID rather than a bare yes, because the wake socket
// also carries field subscriptions and they are authorized against it. The
// token has always carried a userID; it used to be verified and discarded.
export async function verifyHarnessWakeToken(
  token: string,
  namespace: string,
  secret: string
): Promise<{ userID: string } | false> {
  try {
    const [payload, signature, extra] = token.split('.')
    if (!payload || !signature || extra) return false
    const key = await wakeTokenKey(secret, ['verify'])
    const valid = await crypto.subtle.verify(
      'HMAC',
      key,
      decodeBase64URL(signature),
      new TextEncoder().encode(payload)
    )
    if (!valid) return false
    const claims = JSON.parse(
      new TextDecoder().decode(decodeBase64URL(payload))
    ) as HarnessWakeTokenPayload
    const ok =
      claims.namespace === namespace &&
      typeof claims.userID === 'string' &&
      claims.userID.length > 0 &&
      Number.isFinite(claims.expiresAt) &&
      claims.expiresAt > Date.now()
    return ok ? { userID: claims.userID } : false
  } catch {
    return false
  }
}

async function signWakeToken(payload: string, secret: string): Promise<string> {
  const key = await wakeTokenKey(secret, ['sign'])
  const signature = await crypto.subtle.sign(
    'HMAC',
    key,
    new TextEncoder().encode(payload)
  )
  return encodeBase64URL(new Uint8Array(signature))
}

function wakeTokenKey(secret: string, usages: KeyUsage[]): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    usages
  )
}

function encodeBase64URL(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '')
}

function decodeBase64URL(value: string): ArrayBuffer {
  const base64 = value.replaceAll('-', '+').replaceAll('_', '/')
  const binary = atob(base64.padEnd(Math.ceil(base64.length / 4) * 4, '='))
  return Uint8Array.from(binary, (character) => character.charCodeAt(0))
    .buffer as ArrayBuffer
}
