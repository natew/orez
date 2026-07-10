const percentile = (values, p) => {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)]
}

const round = (value) => Math.round(value * 1_000) / 1_000
const port = 9_500 + Math.floor(Math.random() * 300)
const spawnedAt = performance.now()
const server = Bun.spawn(
  [
    'bunx',
    'wrangler',
    'dev',
    '--config',
    'wrangler.toml',
    '--local',
    '--var',
    'ADMIN_KEY:measure-admin',
    '--port',
    String(port),
  ],
  {
    cwd: new URL('.', import.meta.url).pathname,
    stdout: 'ignore',
    stderr: 'inherit',
  }
)
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
        ? {
            pid: Number(match[1]),
            ppid: Number(match[2]),
            rss: Number(match[3]),
            command: match[4],
          }
        : null
    })
    .filter(Boolean)
  const descendants = new Set([server.pid])
  for (let pass = 0; pass < 8; pass++) {
    for (const row of rows) if (descendants.has(row.ppid)) descendants.add(row.pid)
  }
  return rows
    .filter(
      (row) => descendants.has(row.pid) && /(^|[/\s])workerd(?:\s|$)/.test(row.command)
    )
    .reduce((total, row) => total + row.rss, 0)
}

const request = async (namespace, route, body, admin = false) => {
  const response = await fetch(`${baseURL}/${namespace}${route}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {
      ...(admin
        ? { 'x-admin-key': 'measure-admin' }
        : { authorization: 'Bearer token-measure' }),
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!response.ok)
    throw new Error(`${route}: ${response.status} ${await response.text()}`)
  return response.json()
}

try {
  const baselineRssKiB = workerdRssKiB()
  const coldDoMs = []
  for (let index = 0; index < 30; index++) {
    const start = performance.now()
    await request(`cold-${crypto.randomUUID()}`, '/pull', {
      clientID: `cold-client-${index}`,
      clientGroupID: `cold-group-${index}`,
      cookie: null,
    })
    coldDoMs.push(performance.now() - start)
  }
  const afterColdRssKiB = workerdRssKiB()

  const acknowledgementMs = []
  const namespace = `cpu-${crypto.randomUUID()}`
  const statusAfterSeed = await request(namespace, '/admin/status', undefined, true)
  for (let index = 0; index < 50; index++) {
    const start = performance.now()
    await request(namespace, '/push', {
      clientGroupID: 'measure-group',
      pushVersion: 1,
      mutations: [
        {
          type: 'custom',
          clientID: 'measure-client',
          id: index + 1,
          name: 'project.rename',
          args: [{ id: 'p0', name: `measure-${index}` }],
        },
      ],
    })
    acknowledgementMs.push(performance.now() - start)
  }
  const statusAfterLoad = await request(namespace, '/admin/status', undefined, true)
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
          note: 'client-observed local workerd acknowledgement wall time; wake fan-out runs asynchronously after commit, not billed production CPU',
          samples: acknowledgementMs.length,
          acknowledgementP50Ms: round(percentile(acknowledgementMs, 0.5)),
          acknowledgementP95Ms: round(percentile(acknowledgementMs, 0.95)),
        },
        storage: {
          seededBytes: statusAfterSeed.databaseSizeBytes,
          afterFiftyPushesBytes: statusAfterLoad.databaseSizeBytes,
          deltaBytes:
            statusAfterLoad.databaseSizeBytes - statusAfterSeed.databaseSizeBytes,
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
      2
    )
  )
} finally {
  server.kill()
  await server.exited
}
