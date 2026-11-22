declare module 'limesync:app' {
  export const schema: import('limesync/data').Schema
  export const models: import('limesync/data').GenericModels
  export const groupedQueries: import('limesync/data').GroupedQueries
  export const app: import('limesync/data').DataApp<typeof schema, typeof models>
}

declare module 'limesync:app/server' {
  export const app: import('limesync/data/server').DataServerApp<
    import('limesync/data').Schema,
    import('limesync/data').GenericModels
  >
  export const queries: typeof app.queries
  export const mutationValidators: typeof app.mutationValidators
}
