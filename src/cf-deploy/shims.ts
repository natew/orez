import type { CfDeployConfig } from './config.js'

/**
 * Retired Zero-on-Cloudflare source builders.
 *
 * Kept as named compatibility exports only while Soot and Chat move their
 * control-plane workers to Orez Lite. New deployments must use the Rust/SQLite
 * builder below.
 */
export {
  buildAppShimSource,
  buildDataShimSource,
  buildUserShimSource,
  type ShimBuildOptions,
} from './legacy-zero-shims.js'

export type RustSyncUserShimOptions = {
  feedTables: Readonly<Record<string, readonly string[]>>
}

/**
 * Build the small Cloudflare entrypoint for a single-worker Orez Lite app.
 *
 * The behavior lives in `lite-worker.ts` as ordinary typechecked code. This
 * generator exists only because Cloudflare requires statically visible imports
 * and named Durable Object / WorkerEntrypoint exports.
 */
export function buildRustSyncUserShimSource(
  cfg: CfDeployConfig,
  options: RustSyncUserShimOptions
): string {
  const migrationRunner = `run${cfg.prefixPascal}CloudflareMigrations`
  return `import { WorkerEntrypoint } from 'cloudflare:workers'
import { ZeroDO as OrezZeroSqlDO, createApplicationSqlClient } from 'orez/cf-do'
import { createOrezLiteWorkerRuntime } from 'orez/cf-deploy/lite-worker'
import { installZeroSqlWriteCircuitBreaker } from 'orez/worker/zero-sql-write-circuit'
import { ${migrationRunner} } from './orez-migrations.js'

const runtime = createOrezLiteWorkerRuntime({
  prefix: ${JSON.stringify(cfg.prefix)},
  feedTables: ${JSON.stringify(options.feedTables)},
  createApplicationSqlClient,
  runMigrations: ${migrationRunner},
  loadApp: () => import('./one-app.js').then((module) => module.default),
})

export class ZeroSqlDO extends OrezZeroSqlDO {
  constructor(ctx, env) {
    super(ctx, env)
    installZeroSqlWriteCircuitBreaker(ctx.storage.sql, {
      table: ${JSON.stringify(`_${cfg.prefix}_write_circuit`)},
      logPrefix: ${JSON.stringify(`[${cfg.prefix}]`)},
      rowsPerWindow: 200_000,
      hardRowsPerWindow: 1_000_000,
    })
  }
}

export { ZeroSqlDO as ZeroDO }

export class OrezDataFeed extends WorkerEntrypoint {
  fetch(request) {
    return runtime.dataFeed(request, this.env)
  }
}

export default {
  fetch(request, env, ctx) {
    return runtime.fetch(request, env, ctx)
  },
}
`
}
