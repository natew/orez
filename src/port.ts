import { createServer, type Server } from 'node:net'

export interface FindPortOptions {
  host?: string
  maxAttempts?: number
}

export async function findPort(
  preferred: number,
  maxAttemptsOrOptions: number | FindPortOptions = 20
): Promise<number> {
  const options =
    typeof maxAttemptsOrOptions === 'number'
      ? { maxAttempts: maxAttemptsOrOptions }
      : maxAttemptsOrOptions
  return findPortBlock(preferred, 1, options)
}

export async function findPortBlock(
  preferred: number,
  count: number,
  options: FindPortOptions = {}
): Promise<number> {
  if (count < 1) throw new Error('findPortBlock count must be >= 1')

  const host = options.host ?? '127.0.0.1'
  const maxAttempts = options.maxAttempts ?? 20
  let port = preferred
  let attempt = 0

  while (attempt <= maxAttempts) {
    const servers: Server[] = []
    try {
      const first = await listen(port, host)
      servers.push(first.server)
      const base = first.port

      for (let offset = 1; offset < count; offset++) {
        servers.push((await listen(base + offset, host)).server)
      }

      await closeAll(servers)
      return base
    } catch (err) {
      await closeAll(servers)
      const code = (err as NodeJS.ErrnoException)?.code
      if (code !== 'EADDRINUSE' || attempt >= maxAttempts) throw err
      attempt++
      port = preferred === 0 ? 0 : port + 1
    }
  }

  throw new Error(`could not find ${count} free port(s) from ${preferred}`)
}

function listen(port: number, host: string): Promise<{ server: Server; port: number }> {
  return new Promise((resolve, reject) => {
    const server = createServer()
    server.unref()
    server.once('error', reject)
    server.listen(port, host, () => {
      server.off('error', reject)
      const address = server.address()
      if (!address || typeof address === 'string') {
        server.close(() => reject(new Error(`could not inspect port ${port}`)))
        return
      }
      resolve({ server, port: address.port })
    })
  })
}

function closeAll(servers: Server[]): Promise<void> {
  return Promise.all(
    servers.map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => resolve())
        })
    )
  ).then(() => {})
}
