import { Zero } from '@rocicorp/zero'

import { zeroHttpFixtureMutators, zeroHttpFixtureSchema } from './fixture-schema.js'
import { startZeroHttpServer, type Row } from './server.js'
import { installZeroHttpTransport } from './transport.js'

let storageID = 0

export type ZeroHttpHarness = Awaited<ReturnType<typeof startZeroHttpHarness>>

export async function startZeroHttpHarness(opts?: {
  seed?: { user?: Row[]; project?: Row[]; member?: Row[] }
}) {
  const server = await startZeroHttpServer({ seed: opts?.seed })
  const transport = installZeroHttpTransport({ origin: server.url })
  const clients: Array<{ close(): Promise<unknown> }> = []

  return {
    server,
    transport,
    createZero(userID: string) {
      const zero = new Zero({
        server: server.url,
        userID,
        auth: `token-${userID}`,
        schema: zeroHttpFixtureSchema,
        kvStore: 'mem' as const,
        storageKey: `zero-http-harness-${++storageID}`,
        mutators: zeroHttpFixtureMutators,
      })
      clients.push(zero)
      return zero
    },
    async close() {
      while (clients.length) await clients.pop()?.close()
      transport.uninstall()
      await server.close()
    },
  }
}

export function waitForComplete<T>(view: {
  addListener(listener: (data: T, resultType: string) => void): () => void
}): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error('timed out waiting for complete query')),
      5_000
    )
    let cleanup = () => {}
    cleanup = view.addListener((data, resultType) => {
      if (resultType !== 'complete') return
      clearTimeout(timeout)
      cleanup()
      resolve(JSON.parse(JSON.stringify(data)) as T)
    })
  })
}

export async function eventually(
  assertion: () => void | Promise<void>,
  timeout = 3_000
) {
  const started = Date.now()
  let lastError: unknown
  while (Date.now() - started < timeout) {
    try {
      await assertion()
      return
    } catch (error) {
      lastError = error
      await sleep(10)
    }
  }
  throw lastError
}

export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
