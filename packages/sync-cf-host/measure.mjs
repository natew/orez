const percentile = (values, p) => {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)]
}

const round = (value) => Math.round(value * 1_000) / 1_000
const port = 9_500 + Math.floor(Math.random() * 300)
const spawnedAt = performance.now()
const server = Bun.spawn(['bunx', 'wrangler', 'dev', '--local', '--port', String(port)], {
  cwd: new URL('.', import.meta.url).pathname,
  stdout: 'ignore',
  stderr: 'inherit',
})
const baseURL = `http://127.0.0.1:${port}`

let readyAt
for (let attempt = 0; ; attempt++) {
  try {
    const response = await fetch(baseURL)
    if (response.ok) {
      readyAt = performance.now()
      break
    }
  } catch {}
  if (attempt >= 150) throw new Error('wrangler workerd did not become ready')
  await new Promise((resolve) => setTimeout(resolve, 100))
}

function workerdRssKiB() {
  const result = Bun.spawnSync(['ps', '-axo', 'pid=,ppid=,rss=,command='])
  const rows = new TextDecoder()
    .decode(result.stdout)
    .trim()
    .split('\n')
    .map((line) => {
      const match = line.trim().match(/^(\d+)\s+(\d+)\s+(\d+)\s+(.+)$/)
      return match
        ? { pid: Number(match[1]), ppid: Number(match[2]), rss: Number(match[3]), command: match[4] }
        : null
    })
    .filter(Boolean)
  const descendants = new Set([server.pid])
  for (let pass = 0; pass < 8; pass++) {
    for (const row of rows) if (descendants.has(row.ppid)) descendants.add(row.pid)
  }
  return rows
    .filter((row) => descendants.has(row.pid) && /(^|[/\s])workerd(?:\s|$)/.test(row.command))
    .reduce((total, row) => total + row.rss, 0)
}

const request = async (namespace, route, body) => {
  const response = await fetch(`${baseURL}/${namespace}${route}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!response.ok) throw new Error(`${route}: ${response.status} ${await response.text()}`)
  return response.json()
}

try {
  const baselineRssKiB = workerdRssKiB()
  const coldDoMs = []
  for (let index = 0; index < 30; index++) {
    const start = performance.now()
    await request(`cold-${crypto.randomUUID()}`, '/pull')
    coldDoMs.push(performance.now() - start)
  }
  const afterColdRssKiB = workerdRssKiB()

  const executionMs = []
  const wasmMs = []
  const sqlMs = []
  const namespace = `cpu-${crypto.randomUUID()}`
  for (let index = 0; index < 50; index++) {
    const result = await request(namespace, '/push/read-then-write', {
      mutationID: `measure-${index}`,
    })
    executionMs.push(result.timing.elapsedMs)
    wasmMs.push(result.timing.wasmMs)
    sqlMs.push(result.timing.sqlMs)
  }
  const afterLoadRssKiB = workerdRssKiB()

  console.log(
    JSON.stringify(
      {
        measuredAt: new Date().toISOString(),
        environment: {
          runtime: 'local workerd via wrangler dev',
          wrangler: '4.103.0',
          workerd: '1.20260617.1',
          platform: `${process.platform}-${process.arch}`,
        },
        coldStart: {
          wranglerSpawnToReadyMs: round(readyAt - spawnedAt),
          durableObjectSamples: coldDoMs.length,
          durableObjectP50Ms: round(percentile(coldDoMs, 0.5)),
          durableObjectP95Ms: round(percentile(coldDoMs, 0.95)),
        },
        transactionCpuProxy: {
          note: 'DO-side performance.now elapsed time; local workerd wall time, not billed production CPU',
          samples: executionMs.length,
          elapsedP50Ms: round(percentile(executionMs, 0.5)),
          elapsedP95Ms: round(percentile(executionMs, 0.95)),
          wasmBoundaryP50Ms: round(percentile(wasmMs, 0.5)),
          wasmBoundaryP95Ms: round(percentile(wasmMs, 0.95)),
          sqlP50Ms: round(percentile(sqlMs, 0.5)),
          sqlP95Ms: round(percentile(sqlMs, 0.95)),
        },
        memory: {
          note: 'resident set of the local workerd child process; isolate memory is not separately exposed',
          baselineMiB: round(baselineRssKiB / 1024),
          afterThirtyColdObjectsMiB: round(afterColdRssKiB / 1024),
          afterFiftyPushesMiB: round(afterLoadRssKiB / 1024),
          deltaMiB: round((afterLoadRssKiB - baselineRssKiB) / 1024),
        },
      },
      null,
      2,
    ),
  )
} finally {
  server.kill()
  await server.exited
}
