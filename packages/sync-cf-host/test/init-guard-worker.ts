import { harnessConfig } from '../src/harness-config.js'
import { createSyncDurableObject, createSyncWorker } from '../src/index.js'

import type { SyncHostEnv, SyncSql } from '../src/index.js'

type Env = SyncHostEnv
const harness = harnessConfig<Env>()
const config = {
  ...harness,
  initialize(sql: SyncSql) {
    harness.initialize(sql)
    sql.exec('CREATE TABLE IF NOT EXISTS init_guard_test (mode TEXT)')
    sql.query('PRAGMA table_info("project")')
    sql.exec('CREATE INDEX IF NOT EXISTS init_guard_project_name ON project (name)')
    sql.exec('DROP INDEX IF EXISTS init_guard_missing')
    const mode = sql.query<{ mode: string }>('SELECT mode FROM init_guard_test')[0]?.mode
    if (mode === 'poison' || mode === 'repair') {
      sql.exec('DROP TABLE task')
      sql.exec(`CREATE TABLE task AS SELECT 'bad' AS id, 'p0' AS projectId,
        'title' AS title, 'garbage' AS rank, 0 AS done, NULL AS meta, NULL AS dueAt`)
      if (mode === 'repair') sql.exec('UPDATE task SET rank = 1')
    }
  },
}
export class SyncDurableObject extends createSyncDurableObject(config) {}
export default createSyncWorker(config)
