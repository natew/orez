import { runInNewContext } from 'node:vm'

import { afterEach, describe, expect, test, vi } from 'vitest'

import { generateLite } from './generate-lite'

import type { LiteParseFn, LiteParsedFile } from './generate-lite'

// minimal hand-rolled "parser" backed by a lookup table. the real caller
// (e.g. a browser worker) will plug in acorn+acorn-typescript here; for the
// test we just return pre-baked lite ast shapes keyed by file path, which
// keeps the test focused on generate-lite's wiring rather than ast walking.
function makeParse(table: Record<string, LiteParsedFile>): LiteParseFn {
  return (_src, path) => {
    const entry = table[path]
    if (!entry) {
      throw new Error(`no lite ast fixture for ${path}`)
    }
    return entry
  }
}

const DIR = '/proj/src/data'

afterEach(() => {
  vi.restoreAllMocks()
})

describe('generateLite', () => {
  test('emits models.ts, syncedMutations.ts, and README.md from inline types', () => {
    const files: Record<string, string> = {
      [`${DIR}/todo.ts`]: '// fake source, parser returns fixture',
      [`${DIR}/user.ts`]: '// fake source, parser returns fixture',
    }

    const fixtures: Record<string, LiteParsedFile> = {
      [`${DIR}/todo.ts`]: {
        mutations: [
          {
            modelName: 'todo',
            handlers: [
              {
                name: 'toggle',
                paramTypeText: '{ id: string; isActive: boolean }',
              },
              {
                name: 'rename',
                paramTypeText: '{ id: string; title: string }',
              },
            ],
            schema: null,
          },
        ],
        queries: [],
      },
      [`${DIR}/user.ts`]: {
        mutations: [
          {
            modelName: 'user',
            handlers: [
              {
                name: 'finishOnboarding',
                // null = no second param / void
                paramTypeText: null,
              },
            ],
            schema: null,
          },
        ],
        queries: [],
      },
    }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse(fixtures),
    })

    // output file set
    expect(Object.keys(result.files).sort()).toEqual([
      'README.md',
      'groupedQueries.ts',
      'instances.ts',
      'models.ts',
      'syncedMutations.ts',
      'syncedQueries.ts',
    ])

    // models.ts: aliases the user import without changing the model key
    const models = result.files['models.ts']!
    expect(models).toContain("import * as todo from '../todo'")
    expect(models).toContain("import * as userPublic from '../user'")
    expect(models).toContain('user: userPublic,')
    expect(models).not.toContain('\n  userPublic,')
    expect(models).toContain('export const models = {')

    // syncedMutations.ts: inline types resolve to v.object validators
    const syncedMutations = result.files['syncedMutations.ts']!
    expect(syncedMutations).toContain('toggle:')
    expect(syncedMutations).toContain('v.object({')
    expect(syncedMutations).toContain('id: v.string()')
    expect(syncedMutations).toContain('isActive: v.boolean()')
    expect(syncedMutations).toContain('rename:')
    expect(syncedMutations).toContain('title: v.string()')
    // null param → v.void_()
    expect(syncedMutations).toContain('finishOnboarding: v.void_()')

    expect(result.modelCount).toBe(2)
    expect(result.schemaCount).toBe(0)
    expect(result.mutationCount).toBe(3)
  })

  test('parses newline-separated mutation object types', () => {
    const files: Record<string, string> = {
      [`${DIR}/agentEvent.ts`]: '// fake',
      [`${DIR}/deployment.ts`]: '// fake',
    }

    const fixtures: Record<string, LiteParsedFile> = {
      [`${DIR}/agentEvent.ts`]: {
        mutations: [
          {
            modelName: 'agentEvent',
            handlers: [
              {
                name: 'claimReviewLease',
                paramTypeText: `{
                  id: string
                  projectId: string
                  createdAt: number
                }`,
              },
            ],
            schema: null,
          },
        ],
        queries: [],
      },
      [`${DIR}/deployment.ts`]: {
        mutations: [
          {
            modelName: 'deployment',
            handlers: [
              {
                name: 'deploy',
                paramTypeText: `{
                  id: string
                  platform?: string // 'web' | 'ios'
                  buildTier?: string // 'dev' | 'testflight' | 'production'
                }`,
              },
            ],
            schema: null,
          },
        ],
        queries: [],
      },
    }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse(fixtures),
    })

    const synced = result.files['syncedMutations.ts']!
    expect(synced).toContain('claimReviewLease:')
    expect(synced).toContain('projectId: v.string()')
    expect(synced).toContain('createdAt: v.number()')
    expect(synced).toContain('deploy:')
    expect(synced).toContain('platform: v.optional(v.string())')
    expect(synced).toContain('buildTier: v.optional(v.string())')
    expect(synced).not.toContain('claimReviewLease: v.object({})')
    expect(synced).not.toContain('deploy: v.object({})')
  })

  test('parses object intersections and skips index signatures', () => {
    const files: Record<string, string> = {
      [`${DIR}/agent.ts`]: '// fake',
    }

    const fixtures: Record<string, LiteParsedFile> = {
      [`${DIR}/agent.ts`]: {
        mutations: [
          {
            modelName: 'agent',
            handlers: [
              {
                name: 'update',
                paramTypeText: `{
                  id: string
                } & {
                  status?: string
                  currentTaskId?: string
                  [key: string]: unknown
                }`,
              },
            ],
            schema: null,
          },
        ],
        queries: [],
      },
    }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse(fixtures),
    })

    const synced = result.files['syncedMutations.ts']!
    expect(synced).toContain('update:')
    expect(synced).toContain('id: v.string()')
    expect(synced).toContain('status: v.optional(v.string())')
    expect(synced).toContain('currentTaskId: v.optional(v.string())')
    expect(synced).not.toContain('[key')
    expect(synced).not.toContain('update: v.object({})')
  })

  test('falls back to v.unknown() for type references', () => {
    const files: Record<string, string> = {
      [`${DIR}/post.ts`]: `export const allPosts = () => zql.post`,
    }

    const fixtures: Record<string, LiteParsedFile> = {
      [`${DIR}/post.ts`]: {
        mutations: [
          {
            modelName: 'post',
            handlers: [
              {
                name: 'archive',
                // bare type reference — parseTypeString returns null, so
                // generate-lite should fall back to v.unknown() rather than
                // attempting cross-file type resolution.
                paramTypeText: 'ArchiveParams',
              },
              {
                name: 'publish',
                // primitive, should resolve
                paramTypeText: 'string',
              },
            ],
            schema: null,
          },
        ],
        queries: [],
      },
    }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse(fixtures),
    })

    const synced = result.files['syncedMutations.ts']!
    expect(synced).toContain('archive: v.unknown()')
    expect(synced).toContain('publish: v.string()')
  })

  test('uses v.unknown() for explicitly unknown mutation params', () => {
    const files: Record<string, string> = {
      [`${DIR}/project.ts`]: '// fake',
    }

    const fixtures: Record<string, LiteParsedFile> = {
      [`${DIR}/project.ts`]: {
        mutations: [
          {
            modelName: 'project',
            handlers: [{ name: 'insert', paramTypeText: 'unknown' }],
            schema: null,
          },
        ],
        queries: [],
      },
    }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse(fixtures),
    })

    expect(result.files['syncedMutations.ts']!).toContain('insert: v.unknown()')
  })

  test('emits query files with v.unknown() fallback for references', () => {
    const files: Record<string, string> = {
      [`${DIR}/post.ts`]: '// fake',
    }

    const fixtures: Record<string, LiteParsedFile> = {
      [`${DIR}/post.ts`]: {
        mutations: [],
        queries: [
          // no-arg query → void
          { name: 'allPosts', paramTypeText: null },
          // inline object → real validator
          { name: 'postById', paramTypeText: '{ id: string }' },
          // primitive
          { name: 'byAuthorId', paramTypeText: 'string' },
          // optional nullable inline object
          {
            name: 'paged',
            paramTypeText:
              '{ pageSize: number; cursor?: { id: string; createdAt: number } | null }',
          },
          // type reference → fallback
          { name: 'filtered', paramTypeText: 'PostFilter' },
          // permission should be skipped
          { name: 'permission', paramTypeText: null },
        ],
      },
    }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse(fixtures),
    })

    // expect both query files
    expect(result.files['groupedQueries.ts']).toBeDefined()
    expect(result.files['syncedQueries.ts']).toBeDefined()

    const grouped = result.files['groupedQueries.ts']!
    expect(grouped).toContain("import * as postSource from '../post'")
    expect(grouped).toContain('postById: postSource.postById')

    const synced = result.files['syncedQueries.ts']!
    expect(synced).toContain('allPosts: defineQuery(() => Queries.post.allPosts())')
    expect(synced).toContain('postById: defineQuery(')
    expect(synced).toContain('id: v.string()')
    expect(synced).toContain('byAuthorId: defineQuery(')
    // primitive string param shows up as v.string() validator
    expect(synced).toMatch(/byAuthorId: defineQuery\(\s*v\.string\(\)/)
    expect(synced).toContain('paged: defineQuery(')
    expect(synced).toContain('cursor: v.optional(v.nullable(v.object({')
    // type reference fallback
    expect(synced).toContain('filtered: defineQuery(')
    expect(synced).toMatch(/filtered: defineQuery\(\s*v\.unknown\(\)/)
    // permission export skipped
    expect(synced).not.toContain('permission: defineQuery')

    expect(result.queryCount).toBe(5) // permission excluded
  })

  test('emits types.ts and tables.ts when a model declares a schema inline', () => {
    const files: Record<string, string> = {
      [`${DIR}/task.ts`]: '// fake',
    }

    const fixtures: Record<string, LiteParsedFile> = {
      [`${DIR}/task.ts`]: {
        mutations: [
          {
            modelName: 'task',
            handlers: [],
            schema: {
              tableName: 'task',
              primaryKeys: ['id'],
              columns: [
                { name: 'id', builderText: 'string()' },
                { name: 'title', builderText: 'string()' },
                { name: 'priority', builderText: 'number()' },
                { name: 'done', builderText: 'boolean()' },
                { name: 'note', builderText: 'string().optional()' },
              ],
            },
          },
        ],
        queries: [],
      },
    }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse(fixtures),
    })

    expect(result.schemaCount).toBe(1)
    expect(result.files['types.ts']).toBeDefined()
    expect(result.files['tables.ts']).toBeDefined()

    const types = result.files['types.ts']!
    expect(types).toContain('export type Task = TableInsertRow<typeof schema.task>')

    const tables = result.files['tables.ts']!
    expect(tables).toContain("export { schema as task } from '../task'")

    // crud validators emitted in syncedMutations.ts
    const synced = result.files['syncedMutations.ts']!
    expect(synced).toContain('insert:')
    expect(synced).toContain('update:')
    expect(synced).toContain('delete:')
    // crud count is 3
    expect(result.mutationCount).toBe(3)
  })

  test('ignores private files inside a namespace folder', () => {
    const files: Record<string, string> = {
      [`${DIR}/post/mutations.ts`]: '// fake',
      [`${DIR}/post/README.md`]: 'not a model',
      [`${DIR}/post/helpers/util.ts`]: 'private helper, ignored',
      [`${DIR}/post/post.d.ts`]: 'declaration file, ignored',
      [`${DIR}/post/post.test.ts`]: 'test file, ignored',
      [`${DIR}/post/post.spec.ts`]: 'spec file, ignored',
    }

    const fixtures: Record<string, LiteParsedFile> = {
      [`${DIR}/post/mutations.ts`]: {
        mutations: [{ modelName: 'post', handlers: [], schema: null }],
        queries: [],
      },
    }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse(fixtures),
    })

    expect(result.modelCount).toBe(1)
    const models = result.files['models.ts']!
    expect(models).toContain("import * as post from '../post/mutations'")
    expect(models).not.toContain('util')
    expect(models).not.toContain('README')
    expect(models).not.toContain('post.test')
    expect(models).not.toContain('post.spec')
  })

  test('supports a split namespace folder', () => {
    const files: Record<string, string> = {
      [`${DIR}/post/mutations.ts`]: '// fake',
    }

    const fixtures: Record<string, LiteParsedFile> = {
      [`${DIR}/post/mutations.ts`]: {
        mutations: [
          {
            modelName: 'post',
            handlers: [{ name: 'publish', paramTypeText: '{ id: string }' }],
            schema: null,
          },
        ],
        queries: [],
      },
    }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse(fixtures),
    })

    const models = result.files['models.ts']!
    expect(models).toContain("from '../post/mutations'")
  })

  test('derives single-file namespaces from data exports', () => {
    const files = {
      [`${DIR}/server.ts`]: `export const serverRows = () => zql.server`,
      [`${DIR}/types.ts`]: `export const formatRow = (value: string) => value`,
    }
    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse({
        [`${DIR}/server.ts`]: {
          mutations: [],
          queries: [{ name: 'serverRows', paramTypeText: null }],
        },
        [`${DIR}/types.ts`]: {
          mutations: [],
          queries: [],
        },
      }),
    })

    expect(result.modelCount).toBe(1)
    expect(result.queryCount).toBe(1)
  })

  test('warns once and ignores an unparseable non-data file', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const result = generateLite({
      files: {
        [`${DIR}/server.ts`]: `export const serverRows = () => zql.server`,
        [`${DIR}/types.ts`]: `export type Broken = {`,
      },
      dir: DIR,
      parse: (source, path) => {
        if (path.endsWith('/types.ts')) throw new Error('parse failed')
        return {
          mutations: [],
          queries: source.includes('zql.server')
            ? [{ name: 'serverRows', paramTypeText: null }]
            : [],
        }
      },
    })

    expect(result.modelCount).toBe(1)
    expect(warn).toHaveBeenCalledOnce()
    expect(warn).toHaveBeenCalledWith(
      '[on-zero] ignoring data/types.ts: no recognized data exports'
    )
    warn.mockRestore()
  })

  test('rejects the removed top-level layout', () => {
    expect(() =>
      generateLite({
        files: { [`${DIR}/queries/post.ts`]: '// fake' },
        dir: DIR,
        parse: makeParse({}),
      })
    ).toThrow(/removed top-level queries\/ layout/)
  })

  test('derives scoped related-table closure from lite metadata', () => {
    const files = {
      [`${DIR}/project/instance.ts`]: `export default defineInstance({ scope: 'projectId' })`,
      [`${DIR}/project/message/queries.ts`]: `export const messages = () => zql.message.related('comments')`,
      [`${DIR}/project/message/mutations.ts`]: '// fake mutation',
      ['/proj/src/database/relations.ts']: '// fake relations',
      ['/proj/src/database/schema.ts']: '// fake tables',
    }
    const empty = { mutations: [], queries: [] }
    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse({
        [`${DIR}/project/message/queries.ts`]: {
          ...empty,
          queries: [
            {
              name: 'messages',
              paramTypeText: null,
              relatedPaths: [['comments', 'author']],
            },
          ],
        },
        [`${DIR}/project/message/mutations.ts`]: {
          ...empty,
          mutations: [
            {
              modelName: 'message',
              handlers: [],
              schema: {
                tableName: 'message',
                primaryKeys: ['id'],
                columns: [
                  { name: 'id', builderText: 'string()' },
                  { name: 'projectId', builderText: 'string()' },
                ],
              },
            },
          ],
        },
        '/proj/src/database/relations.ts': {
          ...empty,
          relations: [
            { sourceTable: 'message', name: 'comments', targetTable: 'comment' },
            { sourceTable: 'comment', name: 'author', targetTable: 'userPublic' },
          ],
        },
        '/proj/src/database/schema.ts': {
          ...empty,
          tables: [
            { name: 'comment', columns: ['id', 'projectId'] },
            { name: 'userPublic', columns: ['id', 'projectId'] },
          ],
        },
      }),
    })

    expect(result.files['instances.ts']).toContain(
      'syncTables: ["comment","message","userPublic"]'
    )
  })

  test('derives fileless support tables through parsed mutation helpers', () => {
    const files = {
      [`${DIR}/post.ts`]: '// namespace',
      [`${DIR}/helpers/writeAudit.ts`]: '// helper',
      [`${DIR}/helpers/readSettings.ts`]: '// nested helper',
    }
    const empty = { mutations: [], queries: [] }
    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse({
        [`${DIR}/post.ts`]: {
          ...empty,
          mutations: [{ modelName: 'post', handlers: [], schema: null }],
          imports: ['./helpers/writeAudit'],
          supportTables: ['post'],
        },
        [`${DIR}/helpers/writeAudit.ts`]: {
          ...empty,
          imports: ['./readSettings'],
          supportTables: ['audit'],
        },
        [`${DIR}/helpers/readSettings.ts`]: {
          ...empty,
          supportTables: ['settings'],
        },
      }),
    })
    const runnable = result.files['instances.ts']!.replace(
      "import { schema } from './schema'",
      ''
    )
      .replace("import * as groupedQueries from './groupedQueries'", '')
      .replace("import { models } from './models'", '')
      .replace('export const instances =', 'globalThis.instances =')
      .replace(/: string/g, '')
      .replace(' as const', '')
    const context = {
      groupedQueries: {},
      models: { post: {} },
      schema: {},
    } as { instances?: Record<string, { supportTables: string[] }> }

    runInNewContext(runnable, context)

    expect(context.instances?.default?.supportTables).toEqual(['audit', 'settings'])
  })

  test('includes a fileless support table in every lite instance that uses it', () => {
    const files = {
      [`${DIR}/account.ts`]: '// control namespace',
      [`${DIR}/project/instance.ts`]: `export default defineInstance({ scope: 'projectId' })`,
      [`${DIR}/project/message.ts`]: '// project namespace',
      ['/proj/src/database/schema.ts']: '// table columns',
    }
    const empty = { mutations: [], queries: [] }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse({
        [`${DIR}/account.ts`]: {
          ...empty,
          mutations: [{ modelName: 'account', handlers: [], schema: null }],
          supportTables: ['audit'],
        },
        [`${DIR}/project/message.ts`]: {
          ...empty,
          mutations: [
            {
              modelName: 'message',
              handlers: [],
              schema: {
                tableName: 'message',
                primaryKeys: ['id'],
                columns: [
                  { name: 'id', builderText: 'string()' },
                  { name: 'projectId', builderText: 'string()' },
                ],
              },
            },
          ],
          supportTables: ['audit'],
        },
        '/proj/src/database/schema.ts': {
          ...empty,
          tables: [{ name: 'message', columns: ['id', 'projectId'] }],
        },
      }),
    })
    const runnable = result.files['instances.ts']!.replace(
      "import { schema } from './schema'",
      ''
    )
      .replace("import * as groupedQueries from './groupedQueries'", '')
      .replace("import { models } from './models'", '')
      .replace('export const instances =', 'globalThis.instances =')
      .replace(/: string/g, '')
      .replace(' as const', '')
    const context = {
      groupedQueries: {},
      models: { account: {}, message: {} },
      schema: {},
    } as { instances?: Record<string, { supportTables: string[] }> }

    runInNewContext(runnable, context)

    expect(context.instances?.default?.supportTables).toEqual(['audit'])
    expect(context.instances?.project?.supportTables).toEqual(['audit'])
  })
})
