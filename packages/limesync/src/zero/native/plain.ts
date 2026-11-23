import {
  openNativeZero,
  type NativeZeroEngine,
  type OpenNativeZeroOptions,
} from './open.js'

import type { Mutators } from './types.js'

export * from './clientExports.js'

function isZeroEngine(value: unknown): value is NativeZeroEngine {
  return (
    value != null &&
    typeof value === 'object' &&
    'open' in value &&
    typeof value.open === 'function' &&
    'execute' in value &&
    typeof value.execute === 'function' &&
    'close' in value &&
    typeof value.close === 'function'
  )
}

export async function createNativeZero<M extends Mutators>(
  options: OpenNativeZeroOptions<M>
) {
  const proxy: unknown = Reflect.get(globalThis, 'NitroModulesProxy')
  if (
    !proxy ||
    typeof proxy !== 'object' ||
    !('createHybridObject' in proxy) ||
    typeof proxy.createHybridObject !== 'function'
  ) {
    throw new Error('Lime Sync native Zero requires a linked Nitro runtime')
  }
  const engine: unknown = proxy.createHybridObject('OneZero')
  if (!isZeroEngine(engine))
    throw new Error('Lime Sync native Zero has an invalid driver')
  return openNativeZero(options, engine)
}
