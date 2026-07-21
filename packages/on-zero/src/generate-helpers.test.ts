import { describe, expect, test } from 'vitest'

import {
  generateGroupedQueriesFile,
  generateModelsFile,
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
  test('grouped queries keep distinct aliases when one namespace is another plus Source', () => {
    const source = generateGroupedQueriesFile([
      { name: 'appById', sourceFile: 'app', importPath: '../app/queries' },
      {
        name: 'appSourceByAppId',
        sourceFile: 'appSource',
        importPath: '../appSource/queries',
      },
    ])

    expectNoDuplicates(source)
    // each namespace still exports its own queries, reading from its own module
    const appExport = source.match(/export const app = \{([^}]*)\}/)![1]!
    const appSourceExport = source.match(/export const appSource = \{([^}]*)\}/)![1]!
    expect(appExport).toContain('appById')
    expect(appSourceExport).toContain('appSourceByAppId')
    expect(appExport).not.toContain('appSourceByAppId')
  })

  test('models keep distinct aliases when user and userPublic both exist', () => {
    const source = generateModelsFile([
      { name: 'user', importPath: '../user' },
      { name: 'userPublic', importPath: '../userPublic' },
    ] as Parameters<typeof generateModelsFile>[0])

    expectNoDuplicates(source)
    // both namespaces survive as distinct keys bound to their own module
    const modelsObject = source.match(/export const models = \{([\s\S]*)\}/)![1]!
    expect(modelsObject).toMatch(/\buser:/)
    expect(modelsObject).toMatch(/\buserPublic\b/)
  })

  test('table re-exports keep distinct names when user and userPublic both exist', () => {
    const source = generateTablesFile([
      { name: 'user', importPath: '../user' },
      { name: 'userPublic', importPath: '../userPublic' },
    ] as Parameters<typeof generateTablesFile>[0])

    expectNoDuplicates(source)
  })

  test('a namespace with no collision keeps its plain alias', () => {
    const source = generateGroupedQueriesFile([
      { name: 'todoById', sourceFile: 'todo', importPath: '../todo/queries' },
    ])

    expect(source).toContain(`import * as todoSource from '../todo/queries'`)
  })
})
