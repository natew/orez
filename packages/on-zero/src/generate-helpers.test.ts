import { describe, expect, test } from 'vitest'

import {
  generateGroupedQueriesFile,
  generateModelsFile,
  generateSyncedMutationsFile,
  generateTablesFile,
} from './generate-helpers'

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
})
