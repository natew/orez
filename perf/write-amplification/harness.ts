import { DoBackend } from '../../src/pg-proxy-do-backend.js'

type Measurement = { route: string; sql: string; rowsWritten: number }

const port = 9_800 + Math.floor(Math.random() * 100)
const persistencePath = `/tmp/orez-write-amplification-${crypto.randomUUID()}`
const server = Bun.spawn(
  [
    'bunx',
    'wrangler',
    'dev',
    '--config',
    'wrangler.toml',
    '--local',
    '--persist-to',
    persistencePath,
    '--port',
    String(port),
  ],
  {
    cwd: new URL('../../src/cf-do/', import.meta.url).pathname,
    stdout: 'ignore',
    stderr: 'inherit',
  }
)
const baseURL = `http://127.0.0.1:${port}`
const measurements: Measurement[] = []

const measuredFetch: typeof fetch = async (input, init) => {
  const headers = new Headers(init?.headers)
  headers.set('x-orez-measure-writes', '1')
  const response = await fetch(input, { ...init, headers })
  try {
    const body = (await response.clone().json()) as {
      writeMeasurements?: Array<{ sql: string; rowsWritten: number }>
    }
    const route = new URL(typeof input === 'string' ? input : input.url).pathname
    for (const entry of body.writeMeasurements ?? []) {
      measurements.push({ route, ...entry })
    }
  } catch {}
  return response
}

function literal(value: string): string {
  return `'${value.replaceAll("'", "''")}'`
}

function total(entries: Measurement[]): number {
  return entries.reduce((sum, entry) => sum + entry.rowsWritten, 0)
}

function summarize(entries: Measurement[]) {
  const byStatement = new Map<string, { calls: number; rowsWritten: number }>()
  for (const entry of entries) {
    if (entry.rowsWritten <= 0) continue
    const sql = entry.sql.replace(/\s+/g, ' ').trim()
    const key = sql
      .replace(/_orez_tx_[A-Za-z0-9_-]+/g, '_orez_tx_<id>')
      .replace(/VALUES \([^)]+\)/g, 'VALUES (...)')
    const current = byStatement.get(key) ?? { calls: 0, rowsWritten: 0 }
    current.calls++
    current.rowsWritten += entry.rowsWritten
    byStatement.set(key, current)
  }
  return {
    rowsWritten: total(entries),
    statements: [...byStatement]
      .map(([sql, value]) => ({ sql, ...value }))
      .sort((a, b) => b.rowsWritten - a.rowsWritten),
  }
}

async function phase(name: string, run: () => Promise<void>) {
  measurements.length = 0
  await run()
  return [name, summarize([...measurements])] as const
}

try {
  for (let attempt = 0; ; attempt++) {
    try {
      if ((await fetch(baseURL)).status < 500) break
    } catch {}
    if (attempt >= 200) throw new Error('workerd did not become ready')
    await Bun.sleep(100)
  }

  const backend = new DoBackend(baseURL, 'postgres', 'write-amplification-harness', {
    fetch: measuredFetch,
    txOwner: 'write-amplification-harness',
  })
  await backend.waitReady
  await backend.exec(`
    CREATE TABLE project (id TEXT PRIMARY KEY, name TEXT NOT NULL);
    CREATE TABLE task (
      id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
      title TEXT NOT NULL
    );
    CREATE INDEX task_project_id ON task(project_id);
    CREATE TABLE comment (
      id TEXT PRIMARY KEY,
      task_id TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
      body TEXT NOT NULL
    );
    CREATE INDEX comment_task_id ON comment(task_id);
    CREATE PUBLICATION zero_app FOR TABLE project, task, comment;
  `)

  await backend.exec("INSERT INTO project (id, name) VALUES ('p1', 'existing')")
  const taskValues = Array.from(
    { length: 40 },
    (_, task) => `(${literal(`t${task}`)}, 'p1', ${literal(`task ${task}`)})`
  )
  await backend.exec(
    `INSERT INTO task (id, project_id, title) VALUES ${taskValues.join(',')}`
  )
  const commentValues = Array.from({ length: 200 }, (_, comment) => {
    const task = comment % 40
    return `(${literal(`c${comment}`)}, ${literal(`t${task}`)}, ${literal(`comment ${comment}`)})`
  })
  await backend.exec(
    `INSERT INTO comment (id, task_id, body) VALUES ${commentValues.join(',')}`
  )

  const report = Object.fromEntries([
    await phase('cursorCounterProbe', async () => {
      const response = await measuredFetch(`${baseURL}/batch`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          statements: [
            "UPDATE project SET name = 'probe-1' WHERE id = 'p1'",
            "UPDATE project SET name = 'probe-2' WHERE id = 'p1'",
            "UPDATE project SET name = 'probe-3' WHERE id = 'p1'",
          ],
        }),
      })
      if (!response.ok) throw new Error(await response.text())
    }),
    await phase('singlePush', async () => {
      await backend.exec('BEGIN')
      await backend.exec("UPDATE project SET name = 'updated' WHERE id = 'p1'")
      await backend.exec('COMMIT')
    }),
    await phase('projectCreate', async () => {
      await backend.exec('BEGIN')
      await backend.exec("INSERT INTO project (id, name) VALUES ('p2', 'new project')")
      const values = Array.from(
        { length: 12 },
        (_, task) => `(${literal(`p2-t${task}`)}, 'p2', ${literal(`new task ${task}`)})`
      )
      await backend.exec(
        `INSERT INTO task (id, project_id, title) VALUES ${values.join(',')}`
      )
      await backend.exec('COMMIT')
      const { rows } = await backend.query(`
        SELECT watermark, row_data
        FROM _zero_changes
        ORDER BY watermark DESC
        LIMIT 13
      `)
      const promoted = rows.reverse().map((row) => ({
        watermark: Number(row.watermark),
        id: JSON.parse(String(row.row_data)).id,
      }))
      const expectedIDs = [
        'p2',
        ...Array.from({ length: 12 }, (_, task) => `p2-t${task}`),
      ]
      if (promoted.map((row) => row.id).join(',') !== expectedIDs.join(',')) {
        throw new Error(`bulk promotion reordered rows: ${JSON.stringify(promoted)}`)
      }
      if (
        promoted.some(
          (row, index) => index > 0 && row.watermark !== promoted[0]!.watermark + index
        )
      ) {
        throw new Error(
          `bulk promotion produced non-consecutive watermarks: ${JSON.stringify(promoted)}`
        )
      }
    }),
    await phase('rollbackCorrectness', async () => {
      const state = async () => {
        const { rows } = await backend.query(`
          SELECT
            (SELECT COUNT(*) FROM _zero_changes) AS change_count,
            (SELECT last_value FROM _zero_change_state WHERE id = 1) AS change_state,
            (SELECT last_value FROM _orez___zero_watermark WHERE dummy = 1) AS sequence_value,
            (SELECT is_called FROM _orez___zero_watermark WHERE dummy = 1) AS sequence_called
        `)
        return rows[0]
      }
      const before = await state()
      await backend.exec('BEGIN')
      await backend.exec("UPDATE project SET name = 'must roll back' WHERE id = 'p2'")
      await backend.exec('ROLLBACK')
      const after = await state()
      if (JSON.stringify(after) !== JSON.stringify(before)) {
        throw new Error(
          `rollback changed committed tracking state from ${JSON.stringify(before)} to ${JSON.stringify(after)}`
        )
      }
      const { rows } = await backend.query("SELECT name FROM project WHERE id = 'p2'")
      if (rows[0]?.name !== 'new project') {
        throw new Error(`rollback left project name ${String(rows[0]?.name)}`)
      }
      const { rows: pending } = await backend.query(
        'SELECT COUNT(*) AS count FROM _zero_pending_changes'
      )
      if (Number(pending[0]?.count) !== 0) {
        throw new Error(`rollback left ${String(pending[0]?.count)} pending changes`)
      }
    }),
    await phase('cascadeDelete', async () => {
      await backend.exec('BEGIN')
      await backend.exec("DELETE FROM project WHERE id = 'p1'")
      await backend.exec('COMMIT')
    }),
  ])

  console.log(
    JSON.stringify(
      {
        measuredAt: new Date().toISOString(),
        fixture: { projects: 1, tasks: 40, comments: 200, secondaryIndexes: 2 },
        report,
      },
      null,
      2
    )
  )
} finally {
  server.kill()
  await server.exited
  Bun.spawnSync(['rm', '-rf', persistencePath])
}
