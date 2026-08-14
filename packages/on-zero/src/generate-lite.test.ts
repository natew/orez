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
          queries: [
            {
              name: 'serverRows',
              rootTable: 'server',
              paramTypeText: null,
              relatedPaths: [],
            },
          ],
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
            ? [
                {
                  name: 'serverRows',
                  rootTable: 'server',
                  paramTypeText: null,
                  relatedPaths: [],
                },
              ]
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

  test('ignores related syntax mentioned only in a query-file comment', () => {
    const files = {
      [`${DIR}/dashboard/queries.ts`]: `// monthSummary moved to category/queries.ts because it uses .related()`,
    }
    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse({
        [`${DIR}/dashboard/queries.ts`]: { mutations: [], queries: [] },
      }),
    })

    expect(result.queryCount).toBe(0)
    expect(result.instances[0]?.syncTables).toEqual([])
  })

  test('compiles every namespace aggregates.ts into one generated set', () => {
    const files = {
      [`${DIR}/post/queries.ts`]: '// queries',
      [`${DIR}/post/aggregates.ts`]: '// declarations only',
      [`${DIR}/order/aggregates.ts`]: '// declarations only',
    }
    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse({
        [`${DIR}/post/queries.ts`]: { mutations: [], queries: [] },
      }),
    })

    const generated = result.files['aggregates.ts']
    expect(generated).toBeDefined()
    // a folder holding only aggregates.ts is still a namespace, and the
    // aggregate module is never parsed — it is imported, not read.
    expect(generated).toContain(
      `import { aggregates as orderAggregates } from '../order/aggregates'`
    )
    expect(generated).toContain(
      `import { aggregates as postAggregates } from '../post/aggregates'`
    )
    expect(generated).toContain(
      'mergeAggregateDefinitions(orderAggregates, postAggregates)'
    )
  })

  test('omits the generated aggregate set when no namespace declares one', () => {
    const files = { [`${DIR}/post/queries.ts`]: '// queries' }
    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse({ [`${DIR}/post/queries.ts`]: { mutations: [], queries: [] } }),
    })

    expect(result.files['aggregates.ts']).toBeUndefined()
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

  test('derives mutation membership from the targeted table', () => {
    const path = `${DIR}/mint/mutations.ts`
    const result = generateLite({
      files: { [path]: '// namespace' },
      dir: DIR,
      parse: makeParse({
        [path]: {
          mutations: [{ modelName: 'expense', handlers: [], schema: null }],
          queries: [],
        },
      }),
    })

    expect(result.instances[0]?.syncTables).toEqual(['expense'])
  })

  test('includes a fileless support table in every lite instance that uses it', () => {
    const files = {
      [`${DIR}/on-zero.config.ts`]: `export default defineConfig({ instances: { control: { dir: './planes/control', supportTables: ['manual'] }, project: { scope: 'projectId' } } })`,
      [`${DIR}/planes/control/account.ts`]: '// control namespace',
      [`${DIR}/project/message.ts`]: '// project namespace',
      ['/proj/src/database/schema.ts']: '// table columns',
    }
    const empty = { mutations: [], queries: [] }

    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse({
        [`${DIR}/on-zero.config.ts`]: {
          ...empty,
          dataConfig: {
            instances: {
              control: { dir: './planes/control', supportTables: ['manual'] },
              project: { scope: 'projectId' },
            },
          },
        },
        [`${DIR}/planes/control/account.ts`]: {
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

    expect(context.instances?.control?.supportTables).toEqual(['audit', 'manual'])
    expect(context.instances?.project?.supportTables).toEqual(['audit'])
  })

  test('keeps a configured default lite instance at the data root', () => {
    const files = {
      [`${DIR}/on-zero.config.ts`]: `export default defineConfig({ instances: { default: { dir: '.', supportTables: ['accountRepo', 'usageLedger'] }, project: { scope: 'projectId' } } })`,
      [`${DIR}/account.ts`]: '// control namespace',
      [`${DIR}/project/message.ts`]: '// project namespace',
    }
    const empty = { mutations: [], queries: [] }
    const result = generateLite({
      files,
      dir: DIR,
      parse: makeParse({
        [`${DIR}/on-zero.config.ts`]: {
          ...empty,
          dataConfig: {
            instances: {
              default: { dir: '.', supportTables: ['accountRepo', 'usageLedger'] },
              project: { scope: 'projectId' },
            },
          },
        },
        [`${DIR}/account.ts`]: {
          ...empty,
          queries: [
            {
              name: 'accounts',
              rootTable: 'account',
              paramTypeText: null,
              relatedPaths: [],
            },
          ],
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
      models: { message: {} },
      schema: {},
    } as { instances?: Record<string, { supportTables: string[] }> }

    runInNewContext(runnable, context)

    expect(context.instances?.default?.supportTables).toEqual([
      'accountRepo',
      'usageLedger',
    ])
    expect(context.instances?.project?.supportTables).toEqual([])
  })

  test('rejects missing configured directories and instance.ts remnants', () => {
    const empty = { mutations: [], queries: [] }
    expect(() =>
      generateLite({
        files: {
          [`${DIR}/on-zero.config.ts`]: `export default defineConfig({ instances: { project: {} } })`,
        },
        dir: DIR,
        parse: makeParse({
          [`${DIR}/on-zero.config.ts`]: {
            ...empty,
            dataConfig: { instances: { project: {} } },
          },
        }),
      })
    ).toThrow(/instance 'project' directory does not exist/)

    expect(() =>
      generateLite({
        files: {
          [`${DIR}/on-zero.config.ts`]: `export default defineConfig({ instances: { control: { dir: './shared' }, project: { dir: './shared' } } })`,
          [`${DIR}/shared/account.ts`]: '// namespace',
        },
        dir: DIR,
        parse: makeParse({
          [`${DIR}/on-zero.config.ts`]: {
            ...empty,
            dataConfig: {
              instances: {
                control: { dir: './shared' },
                project: { dir: './shared' },
              },
            },
          },
        }),
      })
    ).toThrow(/instances 'control' and 'project' resolve to the same directory/)

    expect(() =>
      generateLite({
        files: {
          [`${DIR}/post.ts`]: '// namespace',
          [`${DIR}/project/instance.ts`]: `export default defineInstance({})`,
        },
        dir: DIR,
        parse: makeParse({
          [`${DIR}/post.ts`]: {
            ...empty,
            mutations: [{ modelName: 'post', handlers: [], schema: null }],
          },
        }),
      })
    ).toThrow(/uses removed instance\.ts configuration/)
  })
})
