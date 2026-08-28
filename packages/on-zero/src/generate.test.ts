import {
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync as writeFile,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { runInNewContext } from 'node:vm'

import * as zero from '@rocicorp/zero'
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import {
  deriveDataMembership,
  generate,
  generateDrizzleSchemaFile,
  generateDrizzleSchemaInputFile,
} from './generate'

const testDir = join(tmpdir(), 'on-zero-test-' + Date.now())

function writeFileSync(path: string, content: string) {
  mkdirSync(dirname(path), { recursive: true })
  writeFile(path, content)
}

beforeEach(() => {
  mkdirSync(testDir, { recursive: true })
})

afterEach(() => {
  vi.restoreAllMocks()
  rmSync(testDir, { recursive: true, force: true })
})

describe('generate', () => {
  test('runs after command when files change', async () => {
    writeFileSync(
      join(testDir, 'post/mutations.ts'),
      `export const schema = table('post', { id: string() })`
    )

    // use a command that creates a marker file
    const markerFile = join(testDir, 'after-ran')
    const result = await generate({
      dir: testDir,
      silent: true,
      after: `touch ${markerFile}`,
    })

    expect(result.filesChanged).toBeGreaterThan(0)
    expect(existsSync(markerFile)).toBe(true)
  })

  test('does not regenerate when nothing changed', async () => {
    writeFileSync(
      join(testDir, 'post/mutations.ts'),
      `export const schema = table('post', { id: string() })`
    )

    const first = await generate({ dir: testDir, silent: true })
    expect(first.filesChanged).toBeGreaterThan(0)

    const second = await generate({ dir: testDir, silent: true })
    expect(second.filesChanged).toBe(0)
  })

  test('force regenerates without source changes', async () => {
    writeFileSync(
      join(testDir, 'post/mutations.ts'),
      `export const schema = table('post', { id: string() })`
    )
    writeFileSync(
      join(testDir, 'post/queries.ts'),
      `export const allPosts = () => zero.query.post`
    )
    await generate({ dir: testDir, silent: true })
    const syncedQueriesPath = join(testDir, 'generated/syncedQueries.ts')
    rmSync(syncedQueriesPath)

    await generate({ dir: testDir, silent: true, force: true })

    expect(existsSync(syncedQueriesPath)).toBe(true)
  })
})

describe('instance layout', () => {
  const dataDir = () => join(testDir, 'src/data')

  test('derives query membership from the returned query root', async () => {
    writeFileSync(
      join(dataDir(), 'category/mutations.ts'),
      `export const schema = table('category').columns({ id: string() })`
    )
    writeFileSync(
      join(dataDir(), 'dashboard/queries.ts'),
      `export const monthSummary = () => zql.category.related('expenses')`
    )
    writeFileSync(
      join(testDir, 'src/database/relations.ts'),
      `export const relations = defineRelations(schema, (r) => ({
        category: { expenses: r.many.expense({}) },
      }))`
    )

    await expect(deriveDataMembership({ dir: dataDir() })).resolves.toEqual({
      instances: {
        default: {
          tables: ['category'],
          syncTables: ['category', 'expense'],
          supportTables: [],
          scope: null,
        },
      },
      allTables: ['category', 'expense'],
    })
  })

  test('derives single-file namespaces from data exports', async () => {
    writeFileSync(
      join(dataDir(), 'server.ts'),
      `export const serverRows = () => zql.server`
    )
    writeFileSync(
      join(dataDir(), 'types.ts'),
      `export const formatRow = (value: string) => value`
    )
    writeFileSync(
      join(dataDir(), 'auth.ts'),
      `export function authId(auth: { id: string }) { return auth.id }`
    )

    await expect(deriveDataMembership({ dir: dataDir() })).resolves.toEqual({
      instances: {
        default: {
          tables: ['server'],
          syncTables: ['server'],
          supportTables: [],
          scope: null,
        },
      },
      allTables: ['server'],
    })
  })

  test('warns once and ignores an unparseable non-data file', async () => {
    writeFileSync(join(dataDir(), 'post.ts'), `export const posts = () => zql.post`)
    writeFileSync(join(dataDir(), 'types.ts'), `export type Broken = {`)
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})

    await expect(deriveDataMembership({ dir: dataDir() })).resolves.toMatchObject({
      allTables: ['post'],
    })
    expect(warn).toHaveBeenCalledOnce()
    expect(warn).toHaveBeenCalledWith(
      '[on-zero] ignoring data/types.ts: no recognized data exports'
    )
    warn.mockRestore()
  })

  test('derives fileless support tables through mutation helpers', async () => {
    writeFileSync(
      join(dataDir(), 'post.ts'),
      `
import { writeAudit } from './helpers/writeAudit'
export const posts = () => zql.post
export const mutate = mutations('post', { save: async (ctx) => writeAudit(ctx.tx) })
`
    )
    writeFileSync(
      join(dataDir(), 'helpers/writeAudit.ts'),
      `
import { readSettings } from './readSettings'
export async function writeAudit(tx: Transaction) {
  await tx.mutate.audit.insert({ id: 'audit' })
  await tx.mutate.post.update({ id: 'post' })
  await tx.mutate[tableName].insert({ id: 'dynamic' })
  return readSettings(tx)
}
`
    )
    writeFileSync(
      join(dataDir(), 'helpers/readSettings.ts'),
      `export const readSettings = (tx: Transaction) => tx.query.settings`
    )

    await expect(deriveDataMembership({ dir: dataDir() })).resolves.toEqual({
      instances: {
        default: {
          tables: ['post'],
          syncTables: ['post'],
          supportTables: ['audit', 'settings'],
          scope: null,
        },
      },
      allTables: ['audit', 'post', 'settings'],
    })
  })

  test('derives mutation membership from the targeted table', async () => {
    writeFileSync(
      join(dataDir(), 'mint/mutations.ts'),
      `export const mutate = mutations('expense', permissions, { save: async () => {} })`
    )

    await expect(deriveDataMembership({ dir: dataDir() })).resolves.toEqual({
      instances: {
        default: {
          tables: ['expense'],
          syncTables: ['expense'],
          supportTables: [],
          scope: null,
        },
      },
      allTables: ['expense'],
    })
  })

  test('derives aggregate source and target membership', async () => {
    writeFileSync(
      join(dataDir(), 'expense/mutations.ts'),
      `export const mutate = mutations('expense', permissions, { save: async () => {} })`
    )
    writeFileSync(
      join(dataDir(), 'expense/aggregates.ts'),
      `export const aggregates = {
        dashboardSummary: {
          source: 'expense',
          target: 'dashboardSummary',
          mode: 'materialized',
          groupBy: { userId: 'userId' },
          columns: { expenseCount: count() },
        },
      } satisfies AggregateDefinitions<typeof schema>`
    )

    await expect(deriveDataMembership({ dir: dataDir() })).resolves.toEqual({
      instances: {
        default: {
          tables: ['dashboardSummary', 'expense'],
          syncTables: ['dashboardSummary', 'expense'],
          supportTables: [],
          scope: null,
        },
      },
      allTables: ['dashboardSummary', 'expense'],
    })
  })

  test('includes a fileless support table in every instance that uses it', async () => {
    writeFileSync(
      join(dataDir(), 'control/account.ts'),
      `export const mutate = mutations('account', { save: async (ctx) => ctx.tx.mutate.audit.insert({}) })`
    )
    writeFileSync(
      join(dataDir(), 'on-zero.config.ts'),
      `export default defineConfig({ instances: { control: {}, project: { scope: 'projectId' } } })`
    )
    writeFileSync(
      join(dataDir(), 'project/message.ts'),
      `
export const schema = table('message').columns({ id: string(), projectId: string() })
export const mutate = mutations(schema, permission, {
  save: async (ctx) => ctx.tx.mutate.audit.insert({}),
})
`
    )

    await expect(deriveDataMembership({ dir: dataDir() })).resolves.toMatchObject({
      instances: {
        control: { supportTables: ['audit'] },
        project: { supportTables: ['audit'] },
      },
      allTables: ['account', 'audit', 'message'],
    })
  })

  test('keeps a configured default instance at the data root', async () => {
    writeFileSync(
      join(dataDir(), 'on-zero.config.ts'),
      `export default defineConfig({ instances: { default: { dir: '.', supportTables: ['accountGithubOrgLink', 'accountRepo', 'usageLedger'] }, project: { scope: 'projectId' } } })`
    )
    writeFileSync(
      join(dataDir(), 'account.ts'),
      `export const accounts = () => zql.account`
    )
    writeFileSync(
      join(dataDir(), 'project/message.ts'),
      `export const schema = table('message').columns({ id: string(), projectId: string() })`
    )

    await expect(deriveDataMembership({ dir: dataDir() })).resolves.toEqual({
      instances: {
        default: {
          tables: ['account'],
          syncTables: ['account'],
          supportTables: ['accountGithubOrgLink', 'accountRepo', 'usageLedger'],
          scope: null,
        },
        project: {
          tables: ['message'],
          syncTables: ['message'],
          supportTables: [],
          scope: 'projectId',
        },
      },
      allTables: [
        'account',
        'accountGithubOrgLink',
        'accountRepo',
        'message',
        'usageLedger',
      ],
    })
  })

  test('emits only relations whose source and target are derived members', async () => {
    writeFileSync(join(dataDir(), 'post.ts'), `export const posts = () => zql.post`)
    writeFileSync(
      join(dataDir(), 'comment.ts'),
      `export const comments = () => zql.comment`
    )
    writeFileSync(
      join(testDir, 'src/database/relations.ts'),
      `
export const relations = defineRelations(schema, (r) => ({
  post: {
    comments: r.many.comment({}),
    privateNotes: r.many.privateNote({}),
  },
  comment: { post: r.one.post({}), author: r.one.privateUser({}) },
  privateNote: { post: r.one.post({}) },
}))
`
    )

    const generated = await generateDrizzleSchemaInputFile({
      dir: dataDir(),
      schemaImportPath: '../../database/schema',
    })
    const runnable = generated
      .replace(`import { defineRelations } from 'drizzle-orm'`, '')
      .replace(`import * as schema from "../../database/schema"`, '')
      .replace(/export \{[^\n]+\} from [^\n]+/, '')
      .replace('export const relations =', 'globalThis.relations =')
    const context = {
      schema: { comment: {}, post: {} },
      defineRelations: (_schema: unknown, factory: (relations: unknown) => unknown) =>
        factory({
          one: new Proxy({}, { get: (_target, table) => () => ({ table }) }),
          many: new Proxy({}, { get: (_target, table) => () => ({ table }) }),
        }),
    } as { relations?: unknown }

    runInNewContext(runnable, context)

    expect(context.relations).toEqual({
      comment: { post: { table: 'post' } },
      post: { comments: { table: 'comment' } },
    })
  })

  test('rejects a relation that crosses instance ownership', async () => {
    writeFileSync(
      join(dataDir(), 'control/userPublic.ts'),
      `export const users = () => zql.userPublic`
    )
    writeFileSync(
      join(dataDir(), 'on-zero.config.ts'),
      `export default defineConfig({ instances: { control: {}, project: { scope: 'projectId' } } })`
    )
    writeFileSync(
      join(dataDir(), 'project/message.ts'),
      `
export const schema = table('message').columns({ id: string(), projectId: string() })
export const messages = () => zql.message.related('author')
`
    )
    writeFileSync(
      join(testDir, 'src/database/relations.ts'),
      `export const relations = defineRelations(schema, (r) => ({ message: { author: r.one.userPublic({}) } }))`
    )

    await expect(generate({ dir: dataDir(), silent: true })).rejects.toThrow(
      /message\.messages.*instance 'project'.*userPublic.*instance 'control'/
    )
  })

  test('rejects a scoped sync table without the scope column', async () => {
    writeFileSync(
      join(dataDir(), 'on-zero.config.ts'),
      `export default defineConfig({ instances: { project: { scope: 'projectId' } } })`
    )
    writeFileSync(
      join(dataDir(), 'project/message.ts'),
      `export const schema = table('message').columns({ id: string() })`
    )

    await expect(generate({ dir: dataDir(), silent: true })).rejects.toThrow(
      /table 'message'.*instance 'project'.*scope column 'projectId'/
    )
  })

  test('rejects duplicate namespaces across instances', async () => {
    writeFileSync(
      join(dataDir(), 'control/message.ts'),
      `export const messages = () => zql.message`
    )
    writeFileSync(
      join(dataDir(), 'on-zero.config.ts'),
      `export default defineConfig({ instances: { control: {}, project: { scope: 'projectId' } } })`
    )
    writeFileSync(
      join(dataDir(), 'project/message.ts'),
      `export const schema = table('message').columns({ projectId: string() })`
    )

    await expect(generate({ dir: dataDir(), silent: true })).rejects.toThrow(
      /namespace 'message'.*instances 'control' and 'project'/
    )
  })

  test('rejects missing configured instance directories', async () => {
    writeFileSync(
      join(dataDir(), 'on-zero.config.ts'),
      `export default defineConfig({ instances: { project: { dir: './missing' } } })`
    )

    await expect(deriveDataMembership({ dir: dataDir() })).rejects.toThrow(
      /instance 'project' directory does not exist.*missing/
    )
  })

  test('rejects two instances that resolve to the same directory', async () => {
    writeFileSync(
      join(dataDir(), 'on-zero.config.ts'),
      `export default defineConfig({ instances: { control: { dir: './shared' }, project: { dir: './shared' } } })`
    )
    writeFileSync(
      join(dataDir(), 'shared/account.ts'),
      `export const accounts = () => zql.account`
    )

    await expect(deriveDataMembership({ dir: dataDir() })).rejects.toThrow(
      /instances 'control' and 'project' resolve to the same directory/
    )
  })

  test('rejects root namespaces when instances are configured', async () => {
    writeFileSync(
      join(dataDir(), 'on-zero.config.ts'),
      `export default defineConfig({ instances: { project: {} } })`
    )
    writeFileSync(
      join(dataDir(), 'project/message.ts'),
      `export const rows = () => zql.message`
    )
    writeFileSync(join(dataDir(), 'account.ts'), `export const rows = () => zql.account`)

    await expect(deriveDataMembership({ dir: dataDir() })).rejects.toThrow(
      /data namespace .*account\.ts.*outside every instance directory/
    )
  })

  test('rejects removed instance.ts configuration', async () => {
    writeFileSync(join(dataDir(), 'post.ts'), `export const posts = () => zql.post`)
    writeFileSync(
      join(dataDir(), 'project/instance.ts'),
      `export default defineInstance({ scope: 'projectId' })`
    )

    await expect(deriveDataMembership({ dir: dataDir() })).rejects.toThrow(
      /uses removed instance\.ts configuration/
    )
  })

  test('rejects the removed top-level layout', async () => {
    writeFileSync(
      join(dataDir(), 'queries/message.ts'),
      `export const messages = () => zql.message`
    )

    await expect(generate({ dir: dataDir(), silent: true })).rejects.toThrow(
      /removed top-level queries\/ layout/
    )
  })

  test('rejects dynamically named relations', async () => {
    writeFileSync(
      join(dataDir(), 'message.ts'),
      `export const messages = (relation: string) => zql.message.related(relation)`
    )

    await expect(generate({ dir: dataDir(), silent: true })).rejects.toThrow(
      /related\(\) without a string literal.*statically derivable/
    )
  })
})

describe('mutations', () => {
  test('populates mutationCount and caching works', async () => {
    writeFileSync(
      join(testDir, 'item/mutations.ts'),
      `
import { table, string } from 'on-zero'
import { mutations, serverWhere } from 'on-zero'

export const schema = table('item').columns({
  id: string(),
  name: string(),
}).primaryKey('id')

const perm = serverWhere('item', () => true)

export const mutate = mutations(schema, perm, {
  rename: async ({ tx }, { id, name }: { id: string; name: string }) => {
    await tx.mutate.item.update({ id, name })
  },
})
`
    )

    const first = await generate({ dir: testDir, silent: true })
    expect(first.mutationCount).toBeGreaterThan(0)

    const second = await generate({ dir: testDir, silent: true })
    expect(second.filesChanged).toBe(0)
    expect(second.mutationCount).toBe(first.mutationCount)
  })
})
