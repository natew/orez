/**
 * orez/change-tracking — standalone entrypoint for installing and reading
 * the `_orez` change tracking schema against a postgres-compatible database.
 *
 * this module intentionally has no transitive dependency on pglite-manager
 * or any other server-only orez module. it talks to the database through
 * the minimal `ChangeTrackingDb` structural interface (`exec` + `query`),
 * so it can be imported anywhere a sql executor exists — embedded PGlite,
 * a worker proxy, tests, or an external host.
 *
 * usage:
 *   import { installChangeTracking } from 'orez/change-tracking'
 *   await installChangeTracking(db)
 */

export {
  installChangeTracking,
  installTriggersOnShardTables,
  resetShardSchemaCache,
  getChangesSince,
  getCurrentWatermark,
  purgeConsumedChanges,
} from './replication/change-tracker.js'

export type {
  ChangeRecord,
  ChangeTrackingDb,
} from './replication/change-tracker.js'
