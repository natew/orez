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
  test('generates models.ts, types.ts, tables.ts from model files', async () => {
    writeFileSync(
      join(testDir, 'post/mutations.ts'),
      `
import { table, string, boolean } from 'on-zero'

export const schema = table('post', {
  id: string(),
  title: string(),
  published: boolean(),
})
`
    )

    writeFileSync(
      join(testDir, 'comment/mutations.ts'),
      `
import { table, string } from 'on-zero'

export const schema = table('comment', {
  id: string(),
  postId: string(),
  body: string(),
})
`
    )

    writeFileSync(
      join(testDir, 'post/post.test.ts'),
      `throw new Error('test files must not be generated as models')`
    )
    writeFileSync(
      join(testDir, 'comment/comment.spec.ts'),
      `throw new Error('spec files must not be generated as queries')`
    )

    const result = await generate({ dir: testDir, silent: true })

    expect(result.modelCount).toBe(2)
    expect(result.schemaCount).toBe(2)
    expect(result.filesChanged).toBeGreaterThan(0)

    // check generated files exist
    expect(existsSync(join(testDir, 'generated/models.ts'))).toBe(true)
    expect(existsSync(join(testDir, 'generated/types.ts'))).toBe(true)
    expect(existsSync(join(testDir, 'generated/tables.ts'))).toBe(true)

    // check models.ts content
    const modelsContent = readFileSync(join(testDir, 'generated/models.ts'), 'utf-8')
    expect(modelsContent).toContain("import * as comment from '../comment/mutations'")
    expect(modelsContent).toContain("import * as post from '../post/mutations'")
    expect(modelsContent).not.toContain('post.test')
    expect(modelsContent).toContain('export const models = {')

    // check types.ts content
    const typesContent = readFileSync(join(testDir, 'generated/types.ts'), 'utf-8')
    expect(typesContent).toContain(
      'export type Post = TableInsertRow<typeof schema.post>'
    )
    expect(typesContent).toContain(
      'export type Comment = TableInsertRow<typeof schema.comment>'
    )

    // check tables.ts content
    const tablesContent = readFileSync(join(testDir, 'generated/tables.ts'), 'utf-8')
    expect(tablesContent).toContain("export { schema as post } from '../post/mutations'")
    expect(tablesContent).toContain(
      "export { schema as comment } from '../comment/mutations'"
    )
  })

  test('generates query validators from query files', async () => {
    // need at least one model
    writeFileSync(
      join(testDir, 'post/mutations.ts'),
      `export const schema = table('post', { id: string() })`
    )

    writeFileSync(
      join(testDir, 'post/queries.ts'),
      `
import { zero } from '../zero'

export const allPosts = () => zero.query.post

export const postById = ({ id }: { id: string }) => zero.query.post.where('id', id)

export const postsByAuthor = ({ authorId, limit }: { authorId: string; limit?: number }) =>
  zero.query.post.where('authorId', authorId).limit(limit ?? 10)
`
    )

    const result = await generate({ dir: testDir, silent: true })

    expect(result.queryCount).toBe(3)

    // check query files exist
    expect(existsSync(join(testDir, 'generated/groupedQueries.ts'))).toBe(true)
    expect(existsSync(join(testDir, 'generated/syncedQueries.ts'))).toBe(true)

    // check groupedQueries.ts
    const groupedContent = readFileSync(
      join(testDir, 'generated/groupedQueries.ts'),
      'utf-8'
    )
    expect(groupedContent).toContain("import * as postSource from '../post/queries'")
    expect(groupedContent).toContain('postById: postSource.postById')

    // check syncedQueries.ts has validators
    const syncedContent = readFileSync(
      join(testDir, 'generated/syncedQueries.ts'),
      'utf-8'
    )
    expect(syncedContent).toContain('allPosts: defineQuery')
    expect(syncedContent).toContain('postById: defineQuery')
    expect(syncedContent).toContain('postsByAuthor: defineQuery')
    expect(syncedContent).toContain('v.object')
  })

  test('skips permission exports in queries', async () => {
    writeFileSync(
      join(testDir, 'post/mutations.ts'),
      `export const schema = table('post', { id: string() })`
    )

    writeFileSync(
      join(testDir, 'post/queries.ts'),
      `
export const permission = () => ({ canRead: true })
export const allPosts = () => zero.query.post
`
    )

    const result = await generate({ dir: testDir, silent: true })

    expect(result.queryCount).toBe(1)

    const syncedContent = readFileSync(
      join(testDir, 'generated/syncedQueries.ts'),
      'utf-8'
    )
    expect(syncedContent).toContain('allPosts')
    expect(syncedContent).not.toContain('permission:')
  })

  test('aliases user import without changing the model key', async () => {
    writeFileSync(
      join(testDir, 'user/mutations.ts'),
      `export const schema = table('user', { id: string(), name: string() })`
    )

    await generate({ dir: testDir, silent: true })

    const modelsContent = readFileSync(join(testDir, 'generated/models.ts'), 'utf-8')
    expect(modelsContent).toContain("import * as userPublic from '../user/mutations'")
    expect(modelsContent).toContain('user: userPublic,')
    expect(modelsContent).not.toContain('\n  userPublic,')

    const typesContent = readFileSync(join(testDir, 'generated/types.ts'), 'utf-8')
    expect(typesContent).toContain('typeof schema.userPublic')
  })

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

  test('discovers file and folder namespaces and emits related sync closure', async () => {
    writeFileSync(
      join(dataDir(), 'control/reaction.ts'),
      `export const allReactions = () => zql.reaction`
    )
    writeFileSync(
      join(dataDir(), 'on-zero.config.ts'),
      `export default defineConfig({ instances: { control: {}, project: { scope: 'projectId' } } })`
    )
    writeFileSync(
      join(dataDir(), 'project/message/queries.ts'),
      `
const unused = () => zql.message.related('notARealRelation')
const withComments = () => zql.message.related('comments', (q) => q.related('author'))
export const messages = () => withComments()
`
    )
    writeFileSync(
      join(dataDir(), 'project/message/mutations.ts'),
      `export const schema = table('message').columns({ id: string(), projectId: string() })`
    )
    writeFileSync(
      join(dataDir(), 'project/message/helpers.ts'),
      `export const privateHelper = () => 'ignored'`
    )
    writeFileSync(
      join(testDir, 'src/database/relations.ts'),
      `
export const relations = defineRelations(schema, (r) => ({
  message: { comments: r.many.comment({}) },
  comment: { author: r.one.userPublic({}) },
}))
`
    )
    writeFileSync(
      join(testDir, 'src/database/schema.ts'),
      `
export const comment = sqliteTable('comment', { id: text(), projectId: text() })
export const userPublic = sqliteTable('user_public', { id: text(), projectId: text() })
`
    )

    const membership = await deriveDataMembership({ dir: dataDir() })
    expect(membership.allTables).toEqual(['comment', 'message', 'reaction', 'userPublic'])
    expect(membership.instances.project).toEqual({
      tables: ['message'],
      syncTables: ['comment', 'message', 'userPublic'],
      supportTables: [],
      scope: 'projectId',
    })

    await generate({ dir: dataDir(), silent: true })

    const grouped = readFileSync(join(dataDir(), 'generated/groupedQueries.ts'), 'utf8')
    expect(grouped).toContain("from '../control/reaction'")
    expect(grouped).toContain("from '../project/message/queries'")
    expect(grouped).not.toContain('privateHelper')

    const manifest = readFileSync(join(dataDir(), 'generated/instances.ts'), 'utf8')
    expect(manifest).toContain('control: {')
    expect(manifest).toContain('project: {')
    expect(manifest).toContain('tables: ["message"]')
    expect(manifest).toContain('syncTables: ["comment","message","userPublic"]')
    expect(manifest).toContain(
      `defaultVisibility: (value: string) => ({ column: "projectId", value })`
    )
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

  test('resolves configured instance directories relative to the config file', async () => {
    writeFileSync(
      join(dataDir(), 'on-zero.config.ts'),
      `export default defineConfig({ instances: { control: { dir: './planes/control-data', supportTables: ['audit'] }, project: { dir: '../project-data', scope: 'projectId' } } })`
    )
    writeFileSync(
      join(dataDir(), 'planes/control-data/account.ts'),
      `export const accounts = () => zql.account`
    )
    writeFileSync(
      join(testDir, 'src/project-data/message.ts'),
      `export const schema = table('message').columns({ id: string(), projectId: string() })`
    )

    await expect(
      deriveDataMembership({
        dir: dataDir(),
        config: join(dataDir(), 'on-zero.config.ts'),
      })
    ).resolves.toEqual({
      instances: {
        control: {
          tables: ['account'],
          syncTables: ['account'],
          supportTables: ['audit'],
          scope: null,
        },
        project: {
          tables: ['message'],
          syncTables: ['message'],
          supportTables: [],
          scope: 'projectId',
        },
      },
      allTables: ['account', 'audit', 'message'],
    })

    await expect(
      generate({
        dir: dataDir(),
        config: join(dataDir(), 'on-zero.config.ts'),
        silent: true,
      })
    ).resolves.toMatchObject({ modelCount: 2, schemaCount: 1 })
    expect(readFileSync(join(dataDir(), 'generated/models.ts'), 'utf8')).toContain(
      "from '../../project-data/message'"
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
  test('generates validators for inline mutation param types', async () => {
    writeFileSync(
      join(testDir, 'post/mutations.ts'),
      `
import { table, string } from 'on-zero'
import { mutations, serverWhere } from 'on-zero'

export const schema = table('post').columns({
  id: string(),
  title: string(),
}).primaryKey('id')

const perm = serverWhere('post', () => true)

export const mutate = mutations(schema, perm, {
  archive: async ({ tx }, { id, reason }: { id: string; reason: string }) => {
    await tx.mutate.post.update({ id, archived: true })
  },
})
`
    )

    const result = await generate({ dir: testDir, silent: true })

    expect(result.mutationCount).toBeGreaterThan(0)
    expect(existsSync(join(testDir, 'generated/syncedMutations.ts'))).toBe(true)

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    expect(content).toContain('archive')
    expect(content).toContain('v.object')
    expect(content).toContain('v.string()')
  })

  test('generates CRUD validators from schema columns', async () => {
    writeFileSync(
      join(testDir, 'task/mutations.ts'),
      `
import { table, string, number, boolean } from 'on-zero'
import { mutations, serverWhere } from 'on-zero'

export const schema = table('task').columns({
  id: string(),
  title: string(),
  priority: number(),
  done: boolean(),
  note: string().optional(),
}).primaryKey('id')

const perm = serverWhere('task', () => true)

export const mutate = mutations(schema, perm)
`
    )

    const result = await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')

    // insert: all columns present
    expect(content).toContain('insert:')
    expect(content).toMatch(/insert:.*v\.object/)

    // update: id required, rest optional
    expect(content).toContain('update:')

    // delete: only PK
    expect(content).toContain('delete:')
  })

  test('treats models without export const mutate as empty mutations', async () => {
    writeFileSync(
      join(testDir, 'readonly/mutations.ts'),
      `
import { table, string } from 'on-zero'

export const schema = table('readonly').columns({
  id: string(),
  name: string(),
}).primaryKey('id')
`
    )

    const result = await generate({ dir: testDir, silent: true })
    expect(result.mutationCount).toBe(0)

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    expect(content).toContain('readonly: {')
  })

  test('extracts custom mutations from bare mutations({})', async () => {
    writeFileSync(
      join(testDir, 'admin/mutations.ts'),
      `
import { mutations } from 'on-zero'

export const mutate = mutations({
  reset: async ({ tx }, { targetId }: { targetId: string }) => {
    await tx.mutate.admin.delete({ id: targetId })
  },
})
`
    )

    const result = await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    expect(content).toContain('reset')
    expect(content).toContain('v.string()')
  })

  test('generates validators for string-model multiline mutation params', async () => {
    writeFileSync(
      join(testDir, 'agentEvent/mutations.ts'),
      `
import { mutations, serverWhere } from 'on-zero'

const perm = serverWhere('agentEvent', () => true)

export const mutate = mutations('agentEvent', perm, {
  claimReviewLease: async (
    { tx },
    props: {
      id: string
      projectId: string
      agentId: string
      userId: string
      taskId: string
      reviewKey: string
      runnerId: string
      createdAt: number
    },
  ) => {
    await tx.mutate.agentEvent.insert(props)
  },
})
`
    )

    await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    expect(content).toContain('claimReviewLease')
    expect(content).toContain('projectId: v.string()')
    expect(content).toContain('createdAt: v.number()')
    expect(content).not.toContain('claimReviewLease: v.object({})')
  })

  test('generates validators for object intersections', async () => {
    writeFileSync(
      join(testDir, 'agent/mutations.ts'),
      `
import { mutations, serverWhere } from 'on-zero'

const perm = serverWhere('agent', () => true)

export const mutate = mutations('agent', perm, {
  update: async (
    { tx },
    props: {
      id: string
    } & {
      status?: string
      currentTaskId?: string
      [key: string]: unknown
    },
  ) => {
    await tx.mutate.agent.update(props)
  },
})
`
    )

    await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    expect(content).toContain('update')
    expect(content).toContain('id: v.string()')
    expect(content).toContain('status: v.optional(v.string())')
    expect(content).toContain('currentTaskId: v.optional(v.string())')
    expect(content).not.toContain('update: v.object({})')
    expect(content).not.toContain('[key')
  })

  test('uses v.unknown for untyped mutation payloads', async () => {
    writeFileSync(
      join(testDir, 'project/mutations.ts'),
      `
import { mutations, serverWhere } from 'on-zero'

const perm = serverWhere('project', () => true)

export const mutate = mutations('project', perm, {
  insert: async ({ tx }, project) => {
    await tx.mutate.project.insert(project)
  },
})
`
    )

    await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    expect(content).toContain('insert: v.unknown()')
    expect(content).not.toContain('insert: v.void_()')
  })

  test('handles mutations with only context param (void)', async () => {
    writeFileSync(
      join(testDir, 'user/mutations.ts'),
      `
import { table, string } from 'on-zero'
import { mutations, serverWhere } from 'on-zero'

export const schema = table('user').columns({
  id: string(),
  name: string(),
}).primaryKey('id')

const perm = serverWhere('user', () => true)

export const mutate = mutations(schema, perm, {
  finishOnboarding: async ({ tx }) => {
    // no second param
  },
})
`
    )

    const result = await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    // should not crash, void mutations get no validator
    expect(content).toContain('finishOnboarding')
  })

  test('handles primitive param type', async () => {
    writeFileSync(
      join(testDir, 'user/mutations.ts'),
      `
import { table, string } from 'on-zero'
import { mutations, serverWhere } from 'on-zero'

export const schema = table('user').columns({
  id: string(),
  name: string(),
}).primaryKey('id')

const perm = serverWhere('user', () => true)

export const mutate = mutations(schema, perm, {
  completeSignup: async ({ tx }, userId: string) => {
    await tx.mutate.user.update({ id: userId })
  },
})
`
    )

    const result = await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    expect(content).toContain('completeSignup')
    expect(content).toContain('v.string()')
  })

  test('handles array param type', async () => {
    writeFileSync(
      join(testDir, 'batch/mutations.ts'),
      `
import { mutations } from 'on-zero'

export const mutate = mutations({
  bulkDelete: async ({ tx }, ids: Array<{ id: string }>) => {
    for (const { id } of ids) await tx.mutate.batch.delete({ id })
  },
})
`
    )

    const result = await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    expect(content).toContain('bulkDelete')
    expect(content).toContain('v.array')
  })

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

describe('type resolution', () => {
  test('resolves imported type references for mutations', async () => {
    // types file that the model imports from
    writeFileSync(
      join(testDir, 'post/types.ts'),
      `
export type ArchiveParams = {
  id: string
  reason: string
  archived: boolean
}
`
    )

    writeFileSync(
      join(testDir, 'post/mutations.ts'),
      `
import { table, string, boolean } from 'on-zero'
import { mutations } from 'on-zero'
import type { ArchiveParams } from './types'

export const mutate = mutations({
  archive: async ({ tx }, params: ArchiveParams) => {
    await tx.mutate.post.update(params)
  },
})
`
    )

    const result = await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    // the imported ArchiveParams type should be resolved to its fields
    expect(content).toContain('v.string()')
    expect(content).toContain('v.boolean()')
    expect(content).toContain('reason')
    expect(content).toContain('archived')
  })

  test('resolves utility types like Pick', async () => {
    writeFileSync(
      join(testDir, 'item/types.ts'),
      `
export type Item = {
  id: string
  name: string
  description: string
  count: number
}
`
    )

    writeFileSync(
      join(testDir, 'item/mutations.ts'),
      `
import { table, string, number } from 'on-zero'
import { mutations, serverWhere } from 'on-zero'
import type { Item } from './types'

export const schema = table('item').columns({
  id: string(),
  name: string(),
  description: string(),
  count: number(),
}).primaryKey('id')

const perm = serverWhere('item', () => true)

export const mutate = mutations(schema, perm, {
  rename: async ({ tx }, updates: Pick<Item, 'id' | 'name'>) => {
    await tx.mutate.item.update(updates)
  },
})
`
    )

    const result = await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    // Pick should resolve to only id and name
    expect(content).toContain('id')
    expect(content).toContain('name')
    // description and count should NOT be in the rename validator
    expect(content).not.toMatch(/rename:[\s\S]*description/)
    expect(content).not.toMatch(/rename:[\s\S]*count/)
  })

  test('resolves instantiated Zero row fields through Partial and Omit', async () => {
    symlinkSync(
      join(import.meta.dirname, '../node_modules'),
      join(testDir, 'node_modules'),
      'dir'
    )
    writeFileSync(
      join(testDir, 'item/types.ts'),
      `
import type { Row } from '@rocicorp/zero'
import { drizzleZeroConfig } from 'drizzle-zero-sqlite'
import { sqliteTable, text } from 'drizzle-orm/sqlite-core'

const item = sqliteTable('item', {
  id: text().primaryKey(),
  name: text().notNull(),
  description: text().notNull(),
})
const schema = drizzleZeroConfig({ item })

export type Item = Row<(typeof schema)['tables']['item']>
`
    )

    writeFileSync(
      join(testDir, 'item/mutations.ts'),
      `
import { mutations } from 'on-zero'
import type { Item } from './types'

type ItemUpdate = { id: string } & Partial<Omit<Item, 'id'>>

export const mutate = mutations('item', {
  update: async ({ tx }, props: ItemUpdate) => {
    await tx.mutate.item.update(props)
  },
})
`
    )

    await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    expect(content).toContain('name: v.optional(v.string())')
    expect(content).toContain('description: v.optional(v.string())')
    expect(content).not.toContain('name: v.optional(v.unknown())')
    expect(content).not.toContain('description: v.optional(v.unknown())')
  })

  test('skips symbol-keyed properties when resolving imported mutation param types', async () => {
    writeFileSync(
      join(testDir, 'item/types.ts'),
      `
export type WeirdParams = {
  id: string
  [Symbol.iterator]?: () => Iterator<string>
}
`
    )

    writeFileSync(
      join(testDir, 'item/mutations.ts'),
      `
import { mutations } from 'on-zero'
import type { WeirdParams } from './types'

export const mutate = mutations({
  run: async ({ tx }, params: WeirdParams) => {
    await tx.mutate.item.delete({ id: params.id })
  },
})
`
    )

    await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedMutations.ts'), 'utf-8')
    expect(content).toContain('run')
    expect(content).toContain('id: v.string()')
    expect(content).not.toContain('__@iterator')
  })

  test('resolves imported types in query params', async () => {
    writeFileSync(
      join(testDir, 'post/mutations.ts'),
      `export const schema = table('post', { id: string() })`
    )

    writeFileSync(
      join(testDir, 'post/types.ts'),
      `
export type PostFilter = {
  authorId: string
  published: boolean
}
`
    )

    writeFileSync(
      join(testDir, 'post/queries.ts'),
      `
import type { PostFilter } from './types'

export const filteredPosts = (filter: PostFilter) => zero.query.post
`
    )

    const result = await generate({ dir: testDir, silent: true })

    const content = readFileSync(join(testDir, 'generated/syncedQueries.ts'), 'utf-8')
    expect(content).toContain('filteredPosts')
    expect(content).toContain('v.object')
    expect(content).toContain('authorId')
    expect(content).toContain('v.boolean()')
  })
})

describe('generateDrizzleSchemaFile', () => {
  test('preserves table server names and executable two-hop relationships', () => {
    const schema = {
      tables: {
        users: {
          name: 'users',
          serverName: 'user_records',
          primaryKey: ['id'],
          columns: {
            id: { type: 'string', optional: false, customType: null },
          },
        },
        groups: {
          name: 'groups',
          primaryKey: ['id'],
          columns: {
            id: { type: 'string', optional: false, customType: null },
          },
        },
        memberships: {
          name: 'memberships',
          primaryKey: ['userId', 'groupId'],
          columns: {
            userId: { type: 'string', optional: false, customType: null },
            groupId: { type: 'string', optional: false, customType: null },
          },
        },
      },
      relationships: {
        users: {
          groups: [
            {
              sourceField: ['id'],
              destField: ['userId'],
              destSchema: 'memberships',
              cardinality: 'many' as const,
            },
            {
              sourceField: ['groupId'],
              destField: ['id'],
              destSchema: 'groups',
              cardinality: 'many' as const,
            },
          ],
        },
      },
    }
    const source = generateDrizzleSchemaFile(schema)
    const executableSource = source
      .replace(
        "import { boolean, createSchema, json, number, relationships, string, table } from '@rocicorp/zero'",
        ''
      )
      .replace('export const schema =', 'globalThis.generatedSchema =')
    const context: Record<string, unknown> = { ...zero }

    runInNewContext(executableSource, context)

    expect(source).toContain('const usersTable = table("users").from("user_records")')
    expect(context.generatedSchema).toMatchObject({
      tables: {
        users: {
          serverName: 'user_records',
        },
      },
      relationships: {
        users: {
          groups: schema.relationships.users.groups,
        },
      },
    })
  })
})
