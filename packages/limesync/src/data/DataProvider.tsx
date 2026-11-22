import { createZeroClient, type CreateZeroClientOptions } from './createZeroClient.js'

import type { Schema } from '../zero/zero.js'
import type { GenericModels } from './types.js'
import type { ComponentProps } from 'react'

export function createDataApp<S extends Schema, M extends GenericModels>(
  options: CreateZeroClientOptions<S, M>
) {
  let client: ReturnType<typeof createZeroClient<S, M>> | undefined
  return {
    ...options,
    get client() {
      return (client ??= createZeroClient(options))
    },
  }
}

export type DataApp<S extends Schema, M extends GenericModels> = ReturnType<
  typeof createDataApp<S, M>
>

export function DataProvider<S extends Schema, M extends GenericModels>({
  app,
  ...props
}: ComponentProps<DataApp<S, M>['client']['ProvideZero']> & { app: DataApp<S, M> }) {
  const { ProvideZero } = app.client
  return <ProvideZero {...props} />
}
