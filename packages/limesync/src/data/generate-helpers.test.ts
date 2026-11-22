import { runInNewContext } from 'node:vm'

import * as v from 'valibot'
import { describe, expect, test } from 'vitest'

import {
  generateGroupedQueriesFile,
  generateModelsFile,
  generateSyncedMutationsFile,
  generateTablesFile,
} from './generate-helpers.js'

// the emitted files are modules, so every identifier they declare has to be
// unique. these cases all come from namespace names that are individually legal
// but previously aliased onto each other, producing output that did not compile.
function declaredIdentifiers(source: string): string[] {
  const names: string[] = []
  for (const line of source.split('\n')) {
    const imported = line.match(/^import \* as ([$\w]+) from /)
    if (imported) names.push(imported[1]!)
    const declared = line.match(/^export const ([$\w]+) = /)
    if (declared) names.push(declared[1]!)
    const reexported = line.match(/^export \{ \w+ as ([$\w]+) \} from /)
    if (reexported) names.push(reexported[1]!)
  }
  return names
}

function expectNoDuplicates(source: string) {
  const names = declaredIdentifiers(source)
  const duplicates = names.filter((name, index) => names.indexOf(name) !== index)
  expect(duplicates).toEqual([])
  return names
}

describe('generated module identifiers', () => {
  test('table re-exports keep distinct names when user and userPublic both exist', () => {
    const source = generateTablesFile([
      { name: 'user', importPath: '../user' },
      { name: 'userPublic', importPath: '../userPublic' },
    ] as Parameters<typeof generateTablesFile>[0])

    expectNoDuplicates(source)
  })
})

describe('generated mutation validators', () => {
  test('indents multiline custom validators inside their mutation entries', () => {
    const source = generateSyncedMutationsFile([
      {
        modelName: 'post',
        hasCRUD: true,
        columns: {
          id: { type: 'string', optional: false, customType: undefined },
        },
        primaryKeys: ['id'],
        custom: [
          {
            name: 'update',
            paramType: '{ id: string }',
            valibotCode: 'v.object({\n    id: v.string(),\n  })',
          },
          {
            name: 'publish',
            paramType: '{ id: string }',
            valibotCode: 'v.object({\n    id: v.string(),\n  })',
          },
        ],
      },
    ])

    expect(source).toContain(`    update: v.object({
      id: v.string(),
    }),`)
    expect(source).toContain(`    publish: v.object({
      id: v.string(),
    }),`)
  })

  test('emits an upsert validator alongside the other generated crud slots', () => {
    const source = generateSyncedMutationsFile([
      {
        modelName: 'post',
        hasCRUD: true,
        columns: {
          id: { type: 'string', optional: false, customType: undefined },
          title: { type: 'string', optional: true, customType: undefined },
        },
        primaryKeys: ['id'],
        custom: [],
      },
    ])

    // upsert takes the insert payload shape: every column, optionality as declared
    expect(source).toContain(`    upsert: v.object({
    id: v.string(),
    title: v.optional(v.nullable(v.string())),
  }),`)
    expect(source).toContain('    insert: v.object({')
    expect(source).toContain('    update: v.object({')
    expect(source).toContain('    delete: v.object({')
  })

  test('keeps a custom upsert override instead of dropping it', () => {
    const source = generateSyncedMutationsFile([
      {
        modelName: 'post',
        hasCRUD: true,
        columns: {
          id: { type: 'string', optional: false, customType: undefined },
        },
        primaryKeys: ['id'],
        custom: [
          {
            name: 'upsert',
            paramType: '{ id: string; draft: boolean }',
            valibotCode: 'v.object({\n    id: v.string(),\n    draft: v.boolean(),\n  })',
          },
        ],
      },
    ])

    expect(source).toContain(`    upsert: v.object({
      id: v.string(),
      draft: v.boolean(),
    }),`)
    // the override replaces the generated slot rather than appearing twice
    expect(source.match(/upsert:/g)).toHaveLength(1)
  })

  test('honors a custom unknown payload even when table columns are available', () => {
    const source = generateSyncedMutationsFile([
      {
        modelName: 'post',
        hasCRUD: true,
        columns: { id: { type: 'string', optional: false, customType: undefined } },
        primaryKeys: ['id'],
        custom: [{ name: 'upsert', paramType: 'unknown', valibotCode: '' }],
      },
    ])
    const validators = runInNewContext(
      source
        .replace("import * as v from 'valibot'", '')
        .replace('export const mutationValidators', 'const mutationValidators') +
        '\nmutationValidators',
      { v }
    )
    expect(v.parse(validators.post.upsert, { customInput: true })).toEqual({
      customInput: true,
    })
    expect(() => v.parse(validators.post.insert, { customInput: true })).toThrow()
  })

  test('emits only authored handlers when the model opted out of crud', () => {
    const source = generateSyncedMutationsFile([
      {
        modelName: 'appNotification',
        hasCRUD: false,
        columns: {
          id: { type: 'string', optional: false, customType: undefined },
        },
        primaryKeys: ['id'],
        custom: [
          {
            name: 'markRead',
            paramType: '{ id: string }',
            valibotCode: 'v.object({\n    id: v.string(),\n  })',
          },
        ],
      },
    ])

    expect(source).toContain('    markRead: v.object({')
    for (const slot of ['insert', 'update', 'delete', 'upsert']) {
      expect(source).not.toContain(`    ${slot}:`)
    }
  })

  test('keeps an authored crud-named handler when there are no schema columns', () => {
    const source = generateSyncedMutationsFile([
      {
        modelName: 'post',
        hasCRUD: true,
        columns: {},
        primaryKeys: [],
        custom: [
          {
            name: 'insert',
            paramType: '{ id: string }',
            valibotCode: 'v.object({\n    id: v.string(),\n  })',
          },
        ],
      },
    ])

    expect(source).toContain(`    insert: v.object({
      id: v.string(),
    }),`)
  })
})
