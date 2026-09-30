const assert = require('node:assert/strict')
const { spawn, execFileSync } = require('node:child_process')
const { once } = require('node:events')
const fs = require('node:fs')
const net = require('node:net')
const os = require('node:os')
const path = require('node:path')
const { test } = require('node:test')

const source = __dirname
const binary = path.resolve(source, '../../target/debug/sync-native')
const token = 'supervision-test-admin-token-32-bytes'

async function until(condition) {
  const deadline = Date.now() + 5000
  do {
    if (await condition()) return
    await new Promise((resolve) => setTimeout(resolve, 25))
  } while (Date.now() < deadline)
  assert.fail('condition did not become true within 5 seconds')
}

function running(pid) {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    if (error.code === 'ESRCH') return false
    throw error
  }
}

async function fixture(t, runtime = process.execPath) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'orez-supervision-'))
  const pids = new Set()
  t.after(() => {
    for (const pid of pids) {
      if (running(pid)) process.kill(pid, 'SIGKILL')
    }
    fs.rmSync(root, { recursive: true, force: true })
  })
  const launcher = path.join(root, 'bin/sync-native.cjs')
  fs.cpSync(path.join(source, 'bin'), path.dirname(launcher), { recursive: true })
  fs.writeFileSync(path.join(root, 'package.json'), JSON.stringify({ version: 'test' }))
  const platform = `orez-sync-native-${process.platform}-${process.arch}${process.platform === 'linux' ? '-gnu' : ''}`
  const platformRoot = path.join(root, 'node_modules', platform)
  fs.mkdirSync(path.join(platformRoot, 'bin'), { recursive: true })
  fs.writeFileSync(
    path.join(platformRoot, 'package.json'),
    JSON.stringify({ version: 'test' })
  )
  fs.symlinkSync(binary, path.join(platformRoot, 'bin/sync-native'))
  fs.writeFileSync(path.join(root, 'schema.json'), JSON.stringify({ tables: {} }))
  fs.writeFileSync(path.join(root, 'init.json'), '[]')
  const reservation = net.createServer()
  reservation.listen(0, '127.0.0.1')
  await once(reservation, 'listening')
  const port = reservation.address().port
  await new Promise((resolve) => reservation.close(resolve))
  const args = [
    'serve',
    '--schema',
    path.join(root, 'schema.json'),
    '--init-sql',
    path.join(root, 'init.json'),
    '--data-dir',
    root,
    '--port',
    String(port),
    '--admin-token-env',
    'TEST_ADMIN_TOKEN',
    '--auth-url',
    'http://127.0.0.1:3000/auth',
    '--wake-authorize-url',
    'http://127.0.0.1:3000/wake',
    '--query-transform-url',
    'http://127.0.0.1:3000/query',
  ]
  const parentScript = path.join(root, 'parent.cjs')
  fs.writeFileSync(
    parentScript,
    `const {spawn} = require('node:child_process'); const child = spawn(process.execPath, ${JSON.stringify([launcher, ...args])}, {stdio: 'inherit'}); process.send(child.pid); setInterval(() => {}, 1000)`
  )
  return {
    port,
    args,
    start() {
      const parent = spawn(runtime, [parentScript], {
        env: { ...process.env, TEST_ADMIN_TOKEN: token },
        stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
      })
      pids.add(parent.pid)
      let output = ''
      parent.stdout.on('data', (data) => {
        output += data
      })
      parent.stderr.on('data', (data) => {
        output += data
      })
      return { parent, output: () => output }
    },
    async ready(parent) {
      const [launcherPid] = await once(parent, 'message')
      pids.add(launcherPid)
      await until(async () => {
        try {
          const response = await fetch(`http://127.0.0.1:${port}/admin/health`, {
            headers: { 'x-admin-key': token },
            signal: AbortSignal.timeout(100),
          })
          return response.ok
        } catch {
          return false
        }
      })
      const nativePid = Number(
        execFileSync('pgrep', ['-P', String(launcherPid)], { encoding: 'utf8' }).trim()
      )
      assert.ok(nativePid > 0)
      pids.add(nativePid)
      return { launcherPid, nativePid }
    },
  }
}

for (const runtime of [process.execPath, 'bun']) {
  for (const signal of ['SIGTERM', 'SIGINT', 'SIGKILL']) {
    test(`native host dies when ${path.basename(runtime)} receives ${signal}, and the port can restart`, async (t) => {
      const f = await fixture(t, runtime)
      const first = f.start()
      const { launcherPid, nativePid } = await f.ready(first.parent)
      first.parent.kill(signal)
      await once(first.parent, 'exit')
      await until(() => !running(nativePid) && !running(launcherPid))
      const second = f.start()
      await f.ready(second.parent)
      second.parent.kill('SIGTERM')
    })
  }
}

for (const runtime of [process.execPath, 'bun']) {
  test(`native host dies even when the ${path.basename(runtime)} launcher receives SIGKILL`, async (t) => {
    const f = await fixture(t, runtime)
    const first = f.start()
    const { launcherPid, nativePid } = await f.ready(first.parent)
    process.kill(launcherPid, 'SIGKILL')
    await until(() => !running(nativePid))
    first.parent.kill('SIGTERM')
  })
}

test('occupied port fails immediately and names its holder without killing it', async (t) => {
  const f = await fixture(t)
  const holder = net.createServer()
  holder.listen(f.port, '127.0.0.1')
  await once(holder, 'listening')
  t.after(() => holder.close())
  const started = Date.now()
  const second = f.start()
  await until(() => second.output().includes('already in use'))
  assert.ok(Date.now() - started < 2000)
  assert.match(second.output(), new RegExp(`PID ${process.pid}\\b`))
  assert.match(second.output(), /node/)
  assert.doesNotMatch(second.output(), /panicked/)
  assert.equal(holder.listening, true)
  second.parent.kill('SIGTERM')
})

test('a direct native bind failure returns an error before reading configuration, without a panic', async (t) => {
  const f = await fixture(t)
  const holder = net.createServer()
  holder.listen(f.port, '127.0.0.1')
  await once(holder, 'listening')
  t.after(() => holder.close())
  const args = [...f.args]
  args[args.indexOf('--schema') + 1] = '/does-not-exist/schema.json'
  const child = spawn(binary, args, { stdio: ['ignore', 'pipe', 'pipe'] })
  let output = ''
  child.stderr.on('data', (data) => {
    output += data
  })
  const [code] = await once(child, 'close')
  assert.equal(code, 1)
  assert.match(output, new RegExp(`failed to bind native sync port 127.0.0.1:${f.port}`))
  assert.doesNotMatch(output, /panicked|schema/)
  assert.equal(holder.listening, true)
})
