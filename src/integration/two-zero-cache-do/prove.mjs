import { mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const fixtureDir = import.meta.dirname
const wrangler = join(fixtureDir, 'node_modules/.bin/wrangler')
const stateDir = mkdtempSync(join(tmpdir(), 'orez-two-zero-cache-do-'))

const port = await new Promise((resolvePort, reject) => {
  const server = createServer()
  server.once('error', reject)
  server.listen(0, '127.0.0.1', () => {
    const address = server.address()
    if (!address || typeof address === 'string') {
      server.close()
      reject(new Error('failed to allocate a local port'))
      return
    }
    server.close(() => resolvePort(address.port))
  })
})

const child = Bun.spawn(
  [
    wrangler,
    'dev',
    '--local',
    '--port',
    String(port),
    '--persist-to',
    stateDir,
    '--no-show-interactive-dev-session',
  ],
  {
    cwd: fixtureDir,
    stdout: 'pipe',
    stderr: 'pipe',
  }
)

const output = []
const collect = async (stream) => {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    output.push(decoder.decode(value))
  }
}
const stdout = collect(child.stdout)
const stderr = collect(child.stderr)

try {
  const deadline = Date.now() + 30_000
  let ready = false
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health`)
      if (!response.ok) throw new Error(`health returned ${response.status}`)
      ready = true
      break
    } catch {
      await Bun.sleep(100)
    }
  }
  if (!ready) throw new Error('workerd did not become reachable within 30s')

  const response = await fetch(`http://127.0.0.1:${port}/prove`, {
    signal: AbortSignal.timeout(30_000),
  })
  const body = await response.text()
  if (!response.ok) {
    throw new Error(`two ZeroCacheDO proof failed: HTTP ${response.status} ${body}`)
  }
  const result = JSON.parse(body)
  if (result.ok !== true || result.probes?.length !== 2) {
    throw new Error(`unexpected proof response: ${body}`)
  }
  console.log('two ZeroCacheDO workerd proof passed', JSON.stringify(result))
} catch (error) {
  console.error(String(error))
  const log = output.join('').trim()
  if (log) console.error(log)
  process.exitCode = 1
} finally {
  child.kill('SIGTERM')
  await child.exited
  await Promise.all([stdout, stderr])
  rmSync(stateDir, { recursive: true, force: true })
}
