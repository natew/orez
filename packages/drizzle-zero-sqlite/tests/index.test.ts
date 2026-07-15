import { string as zeroString } from '@rocicorp/zero'
import { defineRelations } from 'drizzle-orm'
import { pgTable, text as pgText } from 'drizzle-orm/pg-core'
import {
  blob,
  integer,
  primaryKey,
  real,
  sqliteTable,
  text,
} from 'drizzle-orm/sqlite-core'
import { describe, expect, test } from 'vitest'

import { drizzleZeroConfig } from '../src/index'

const users = sqliteTable('user_records', {
  id: text().primaryKey(),
  displayName: text('display_name').notNull(),
  active: integer({ mode: 'boolean' }).notNull().default(true),
  createdAt: integer('created_at', { mode: 'timestamp_ms' }).notNull(),
  metadata: text({ mode: 'json' }).$type<{ theme: string }>(),
  score: real(),
  avatar: blob(),
})

const posts = sqliteTable('posts', {
  id: text().primaryKey(),
  authorId: text('author_id').notNull(),
  title: text().notNull(),
})

const groups = sqliteTable('groups', {
  id: text().primaryKey(),
  name: text().notNull(),
})

const memberships = sqliteTable(
  'memberships',
  {
    userId: text('user_id').notNull(),
    groupId: text('group_id').notNull(),
  },
  (table) => [primaryKey({ columns: [table.userId, table.groupId] })]
)

const relations = defineRelations({ users, posts, groups, memberships }, (r) => ({
  users: {
    posts: r.many.posts({
      from: r.users.id,
      to: r.posts.authorId,
    }),
    groups: r.many.groups({
      from: r.users.id.through(r.memberships.userId),
      to: r.groups.id.through(r.memberships.groupId),
    }),
  },
  posts: {
    author: r.one.users({
      from: r.posts.authorId,
      to: r.users.id,
    }),
  },
}))

describe('drizzleZeroConfig', () => {
  test('maps SQLite tables, columns, defaults, and composite primary keys', () => {
    const schema = drizzleZeroConfig(
      { users, posts, groups, memberships, relations },
      {
        tables: {
          users: {
            id: true,
            displayName: true,
            active: true,
            createdAt: true,
            metadata: true,
            score: true,
          },
          posts: true,
          groups: true,
          memberships: true,
        },
        suppressDefaultsWarning: true,
      }
    )

    expect(schema.tables.users).toMatchObject({
      name: 'users',
      serverName: 'user_records',
      primaryKey: ['id'],
      columns: {
        id: { type: 'string', optional: false },
        displayName: { type: 'string', optional: false, serverName: 'display_name' },
        active: { type: 'boolean', optional: true },
        createdAt: { type: 'number', optional: false, serverName: 'created_at' },
        metadata: { type: 'json', optional: true },
        score: { type: 'number', optional: true },
      },
    })
    expect(schema.tables.users.columns).not.toHaveProperty('avatar')
    expect(schema.tables.memberships.primaryKey).toEqual(['userId', 'groupId'])
  })

  test('preserves direct and through relations using schema export keys', () => {
    const schema = drizzleZeroConfig(
      { users, posts, groups, memberships, relations },
      {
        tables: {
          users: {
            id: true,
            displayName: true,
            active: true,
            createdAt: true,
            metadata: true,
            score: true,
          },
          posts: true,
          groups: true,
          memberships: true,
        },
        suppressDefaultsWarning: true,
      }
    )

    expect(schema.relationships.users!.posts).toEqual([
      {
        sourceField: ['id'],
        destField: ['authorId'],
        destSchema: 'posts',
        cardinality: 'many',
      },
    ])
    expect(schema.relationships.users!.groups).toEqual([
      {
        sourceField: ['id'],
        destField: ['userId'],
        destSchema: 'memberships',
        cardinality: 'many',
      },
      {
        sourceField: ['groupId'],
        destField: ['id'],
        destSchema: 'groups',
        cardinality: 'many',
      },
    ])
  })

  test('rejects a non-SQLite table', () => {
    const legacy = pgTable('legacy', {
      id: pgText().primaryKey(),
    })

    expect(() => drizzleZeroConfig({ legacy })).toThrow(
      'Only SQLite tables are supported'
    )
  })

  test('maps SQLite blob JSON and explicit Zero column overrides', () => {
    const preferences = sqliteTable('preference_records', {
      id: text().primaryKey(),
      payload: blob({ mode: 'json' }).$type<{ theme: string }>(),
      raw: blob(),
    })
    const schema = drizzleZeroConfig(
      { preferences },
      {
        tables: {
          preferences: {
            payload: true,
            raw: zeroString(),
          },
        },
        suppressDefaultsWarning: true,
      }
    )

    expect(schema.tables.preferences).toMatchObject({
      name: 'preferences',
      serverName: 'preference_records',
      columns: {
        id: { type: 'string', optional: false },
        payload: { type: 'json', optional: true },
        raw: { type: 'string', optional: false },
      },
    })
  })
})
