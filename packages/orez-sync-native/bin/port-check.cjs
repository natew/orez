'use strict'

const { execFileSync } = require('node:child_process')
const { createServer } = require('node:net')

function portHolder(port) {
  if (process.platform === 'win32') {
    const rows = execFileSync('netstat', ['-ano', '-p', 'tcp'], { encoding: 'utf8' })
    const row = rows.split('\n').find((line) => {
      const fields = line.trim().split(/\s+/)
      return fields[1]?.endsWith(`:${port}`) && fields[3] === 'LISTENING'
    })
    if (!row) return 'holder exited before inspection'
    const pid = row.trim().split(/\s+/).at(-1)
    const name = execFileSync('tasklist', ['/FI', `PID eq ${pid}`, '/FO', 'CSV', '/NH'], {
      encoding: 'utf8',
    }).trim()
    return `PID ${pid} (${name})`
  }
  const rows = execFileSync('lsof', ['-nP', `-iTCP:${port}`, '-sTCP:LISTEN', '-Fp'], {
    encoding: 'utf8',
  })
  return rows
    .trim()
    .split('\n')
    .filter((line) => line.startsWith('p'))
    .map((line) => {
      const pid = line.slice(1)
      // argv names the executable even when Linux's comm is a thread name.
      const command = execFileSync('ps', ['-p', pid, '-o', 'args='], {
        encoding: 'utf8',
      }).trim()
      return `PID ${pid} (${command})`
    })
    .join(', ')
}

async function assertPortAvailable(port, host = '127.0.0.1') {
  const server = createServer()
  await new Promise((resolve, reject) => {
    server.once('error', (error) => {
      if (error.code !== 'EADDRINUSE') return reject(error)
      let holder
      try {
        holder = portHolder(port)
      } catch (inspectionError) {
        holder = `could not inspect holder: ${inspectionError.message}`
      }
      reject(
        new Error(
          `native sync port ${host}:${port} is already in use by ${holder}. Stop that process or choose a different port offset.`
        )
      )
    })
    server.listen(port, host, () => server.close(resolve))
  })
}

module.exports = { assertPortAvailable }
