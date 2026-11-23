import { NativeZero, type NativeZeroOptions } from './client.js'

import type { Engine, Mutators, Schema } from './types.js'

export type OpenNativeZeroOptions<M extends Mutators> = Omit<
  NativeZeroOptions<M>,
  'engine'
> & {
  name: string
  clientID?: string
}

export type NativeZeroEngine = Engine & {
  open(name: string, schema: Schema, clientID: string): Promise<void>
}

export async function openNativeZero<M extends Mutators>(
  options: OpenNativeZeroOptions<M>,
  engine: NativeZeroEngine
) {
  const client = new NativeZero({ ...options, engine })
  try {
    await engine.open(options.name, options.schema, options.clientID ?? '')
    await client.ready()
    return client
  } catch (error) {
    try {
      await client.close()
    } catch {
      try {
        await engine.close()
      } catch {}
    }
    throw error
  }
}
