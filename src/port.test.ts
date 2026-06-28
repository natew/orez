import { createServer, type Server } from 'node:net'

import { afterEach, describe, expect, it } from 'vitest'

import { findPort, findPortBlock } from './port.js'

const servers: Server[] = []

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => resolve())
        })
    )
  )
})

describe('findPort', () => {
  it('returns the actual OS-assigned port for 0', async () => {
    const port = await findPort(0)

    expect(port).toBeGreaterThan(0)
  })
})

describe('findPortBlock', () => {
  it('skips a base whose adjacent zero-cache worker port is occupied', async () => {
    const base = await findPortBlock(0, 2, { host: '::' })
    servers.push(await listen(base + 1, '::'))

    const next = await findPortBlock(base, 2, { host: '::' })

    expect(next).not.toBe(base)
    expect(next).toBeGreaterThan(base)
  })
})

function listen(port: number, host: string): Promise<Server> {
  return new Promise((resolve, reject) => {
    const server = createServer()
    server.once('error', reject)
    server.listen(port, host, () => {
      server.off('error', reject)
      resolve(server)
    })
  })
}
