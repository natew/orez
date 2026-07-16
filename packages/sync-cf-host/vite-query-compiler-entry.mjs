import { createQueryCompiler } from 'orez-sync-cf-host/query-compiler'

const compile = createQueryCompiler({
  tables: {
    account: {
      name: 'account',
      serverName: 'accounts',
      columns: { id: { type: 'string' } },
      primaryKey: ['id'],
    },
  },
})

export const compiledSql = compile(
  { table: 'account' },
  { singular: false, relationships: {} }
).root.sql
