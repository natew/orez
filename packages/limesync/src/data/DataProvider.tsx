import { createZeroClient, type CreateZeroClientOptions } from './createZeroClient.js'

import type { EmptyLocalSchema, Schema } from '../zero/zero.js'
import type { GenericModels } from './types.js'
import type { ComponentProps } from 'react'

export function createDataApp<
  S extends Schema,
  M extends GenericModels,
  L extends Schema = EmptyLocalSchema,
>(options: CreateZeroClientOptions<S, M, L>) {
  let client: ReturnType<typeof createZeroClient<S, M, L>> | undefined
  return {
    ...options,
    get client() {
      return (client ??= createZeroClient(options))
    },
  }
}

export type DataApp<
  S extends Schema,
  M extends GenericModels,
  L extends Schema = EmptyLocalSchema,
> = ReturnType<typeof createDataApp<S, M, L>>

export function DataProvider<
  S extends Schema,
  M extends GenericModels,
  L extends Schema = EmptyLocalSchema,
>({
  app,
  ...props
}: ComponentProps<DataApp<S, M, L>['client']['ProvideZero']> & {
  app: DataApp<S, M, L>
}) {
  const { ProvideZero } = app.client
  return <ProvideZero {...props} />
}
