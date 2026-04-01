import { existsSync } from 'node:fs'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

import type { OrezConfig } from './config.js'

const CONFIG_FILES = ['orez.config.ts', 'orez.config.js', 'orez.config.mjs']

export async function loadConfigFile(cwd = process.cwd()): Promise<OrezConfig> {
  for (const name of CONFIG_FILES) {
    const filePath = resolve(cwd, name)
    if (!existsSync(filePath)) continue

    try {
      const mod = await import(pathToFileURL(filePath).href)
      const config: OrezConfig = mod.default ?? mod
      return config
    } catch (err) {
      throw new Error(
        `failed to load ${name}: ${err instanceof Error ? err.message : err}`
      )
    }
  }

  return {}
}

/**
 * resolve OrezConfig aliases and convert to the shape expected by
 * startZeroLite() + CLI extras (s3, s3Port, disableAdmin).
 *
 * CLI args are passed as overrides — they take precedence over config file values.
 */
export function resolveOrezConfig(
  fileConfig: OrezConfig,
  cliOverrides: Partial<OrezConfig> = {}
): OrezConfig {
  // merge: file < cli (undefined cli values don't override)
  const merged: OrezConfig = { ...fileConfig }
  for (const [k, v] of Object.entries(cliOverrides)) {
    if (v !== undefined) {
      ;(merged as Record<string, unknown>)[k] = v
    }
  }
  return merged
}
