import { createServer } from 'node:net'

export function findPort(preferred: number, maxAttempts = 20): Promise<number> {
  return new Promise((resolve, reject) => {
    let attempt = 0

    function tryPort(port: number) {
      const server = createServer()
      server.unref()
      server.on('error', (err: NodeJS.ErrnoException) => {
        if (err.code === 'EADDRINUSE' && attempt < maxAttempts) {
          attempt++
          tryPort(port + 1)
        } else {
          reject(err)
        }
      })
      server.listen(port, '127.0.0.1', () => {
        server.close(() => resolve(port))
      })
    }

    tryPort(preferred)
  })
}
