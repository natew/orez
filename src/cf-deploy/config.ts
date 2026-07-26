// CfDeployConfig — the single neutralization seam for the shared Cloudflare/Orez
// deploy integration. every consumer-specific token in the worker shims is
// derived from `prefix`:
//
//   prefix 'contrast' ->  __contrast_*  /__contrast_*  x-contrast-*  _contrast_*  [contrast]  zero_contrast
//                     contrast-do-backend  contrast-backup-v2  contrast-ns (cookie)
//   prefixPascal 'Contrast' -> runContrastCloudflareMigrations, ContrastCloudflarePgPool
//
// orez's own internal tokens (orez-data.local, __orez_signal_replication,
// _orez_pg_metadata, …) are NOT derived from prefix — they are shared across all
// consumers and stay fixed.

export interface CfDeployConfig {
  /**
   * lowercase per-deploy token prefix. drives every `__<prefix>_*` global,
   * `/__<prefix>_*` internal path, `x-<prefix>-*` header, `_<prefix>_*` table,
   * `[<prefix>]` log tag, `<prefix>-do-backend` alias, `<prefix>-backup-v2`
   * format, and the `zero_<prefix>` publication.
   */
  prefix: string
  /**
   * PascalCase variant of `prefix` for identifier names
   * (`run<Prefix>CloudflareMigrations`, `<Prefix>CloudflarePgPool`).
   */
  prefixPascal: string
  /**
   * Consumer-declared import specifiers to attach as CompiledWasm worker modules.
   * Each is resolved (honoring the package's `exports`), copied next to the worker
   * output, and its import rewritten to a relative external specifier — so
   * wrangler's CompiledWasm rule turns it into a `WebAssembly.Module` at runtime
   * (workerd forbids compiling wasm from bytes). The matching `import` must appear
   * in a worker module the esbuild step bundles (e.g. the consumer's app shim).
   * Keeps this package app-neutral: no dependency names are baked in here.
   */
  compiledWasmModules?: readonly string[]
  /**
   * Extra fetches the data worker's minute cron (`* * * * *`) fires at the app
   * worker over the `APP` service binding, after the data-tier warm ping. Each
   * emits a guarded `POST <path>` carrying the value of `env.<secretEnvVar>` in the
   * `x-cron-secret` header. The synthetic request also carries a host because One's
   * request URL parser requires it inside a service binding. For consumers whose
   * job/flow runner lives in the app worker and needs a periodic tick (workerd has no
   * long-running process). Keeps this package app-neutral: no consumer paths are
   * baked into the template.
   */
  minuteCronAppForwards?: readonly { path: string; secretEnvVar: string }[]
  /**
   * Optional consumer push handler for `/api/zero/push` inside the data worker.
   * When set, ZeroCacheDO's apiFetch handles pushes without re-entering the app
   * worker; pulls still route through the app worker. The module is imported
   * lazily so consumer code evaluates only after the data worker has installed
   * its request env globals.
   */
  dataWorkerZeroPush?: {
    module: string
    exportName: string
  }
}

/** build a CfDeployConfig from a lowercase prefix, deriving the PascalCase form. */
export function cfDeployConfig(
  prefix: string,
  options?: Pick<
    CfDeployConfig,
    'compiledWasmModules' | 'minuteCronAppForwards' | 'dataWorkerZeroPush'
  >
): CfDeployConfig {
  if (!/^[a-z][a-z0-9]*$/.test(prefix)) {
    throw new Error(
      `cfDeployConfig: prefix must be a lowercase identifier, got ${JSON.stringify(prefix)}`
    )
  }
  return {
    prefix,
    prefixPascal: prefix.charAt(0).toUpperCase() + prefix.slice(1),
    ...options,
  }
}
