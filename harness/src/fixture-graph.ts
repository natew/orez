// PURE schema graph mirroring fixture.ts (columns + kinds, relationships +
// cardinality). Zero-free so spec-shrink.ts and spec-corpus.ts share it without
// importing @rocicorp/zero, and so the frozen sweep generator grammar lives in
// ONE place. Trivially kept in sync: unknown names/kinds are rejected by the
// corpus parser, which mutant-tests every family.
export type Kind = 'id' | 'string' | 'number' | 'boolean' | 'json' | 'nullableNumber'
export type Card = 'one' | 'many'

export const COLUMN_KIND: Record<string, Record<string, Kind>> = {
  user: { id: 'id', name: 'string' },
  project: { id: 'id', ownerId: 'id', name: 'string' },
  member: { id: 'id', projectId: 'id', userId: 'id' },
  task: {
    id: 'id',
    projectId: 'id',
    title: 'string',
    rank: 'number',
    done: 'boolean',
    meta: 'json',
    dueAt: 'nullableNumber',
  },
}

// table -> relationship name -> { child table, cardinality }
export const RELATIONSHIPS: Record<
  string,
  Record<string, { child: string; card: Card }>
> = {
  user: {},
  project: {
    members: { child: 'member', card: 'many' },
    tasks: { child: 'task', card: 'many' },
  },
  member: {
    user: { child: 'user', card: 'one' },
    project: { child: 'project', card: 'one' },
  },
  task: { project: { child: 'project', card: 'one' } },
}

export const columnsOf = (table: string): string[] =>
  Object.keys(COLUMN_KIND[table] ?? {})
export const relOf = (table: string, rel: string) => RELATIONSHIPS[table]?.[rel]
