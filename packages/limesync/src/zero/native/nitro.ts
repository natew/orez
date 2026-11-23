import { NitroModules } from 'react-native-nitro-modules'

import { openNativeZero, type OpenNativeZeroOptions } from './open.js'

import type { OneZero } from './OneZero.nitro.js'
import type { Mutators } from './types.js'

export async function createNativeZero<M extends Mutators>(
  options: OpenNativeZeroOptions<M>
) {
  return openNativeZero(options, NitroModules.createHybridObject<OneZero>('OneZero'))
}
