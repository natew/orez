export type CloudflareConfig = {
  /** Stable lowercase identifier used to namespace Orez-owned tables and globals. */
  name: string
  /** Package imports that workerd should receive as compiled WebAssembly modules. */
  compiledWasmModules?: readonly string[]
}

/**
 * Define the app-neutral configuration shared by Orez Cloudflare build and
 * migration primitives.
 */
export function defineCloudflareConfig(
  name: string,
  options: Omit<CloudflareConfig, 'name'> = {}
): CloudflareConfig {
  if (!/^[a-z][a-z0-9]*$/.test(name)) {
    throw new TypeError(
      `Cloudflare config name must be a lowercase identifier, got ${JSON.stringify(name)}`
    )
  }
  return { name, ...options }
}
