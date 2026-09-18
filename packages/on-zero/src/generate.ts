import { createHash } from 'node:crypto'
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { basename, dirname, resolve } from 'node:path'

import * as ts from 'typescript/unstable/ast'
import {
  ObjectFlags,
  SymbolFlags,
  TypeFlags,
  type Project,
  type Symbol,
  type Type,
} from 'typescript/unstable/async'

import {
  CRUD_MUTATION_NAMES,
  formatObjectKey,
  generateAggregatesFile,
  generateGroupedQueriesFile,
  generateInstancesFile,
  generateModelsFile,
  generateReadmeFile,
  generateSyncedMutationsFile,
  generateSyncedQueriesFile,
  generateTablesFile,
  generateTypesFile,
  parseColumnType,
  parseTypeString,
  renderDrizzleZeroSqliteSchemaModule,
  shouldSkipObjectKey,
} from './generate-helpers'
import { discoverDataLayout, namespaceImportPath } from './generate-layout'
import {
  collectTypeScriptSourcePaths,
  createNativeTypeScriptProject,
  type NativeTypeScriptProject,
} from './native-typescript'

import type { ExtractedMutation, ModelMutations, SchemaColumn } from './generate-helpers'
import type { DataLayout } from './generate-layout'

const hash = (s: string) => createHash('sha256').update(s).digest('hex')
const GENERATOR_CACHE_VERSION = '7'

const isGeneratorSourceFile = (name: string) =>
  name.endsWith('.ts') &&
  !name.endsWith('.d.ts') &&
  !name.endsWith('.test.ts') &&
  !name.endsWith('.spec.ts')

// hash every .ts input the generator reads (all of baseDir except the generated
// output dir + node_modules) so a dev-boot regen can be skipped when nothing
// changed. the expensive part of generate() — loading the typescript compiler
// and building a TS program per query/model for type resolution — runs every
// boot otherwise, even when the output is already current (the configureServer
// watcher re-runs it on real edits, so the boot-time pass is pure redundancy).
function hashInputTree(sourceRoots: string[], generatedDir: string): string {
  const parts: string[] = []
  const seen = new Set<string>()
  const walk = (dir: string) => {
    if (!existsSync(dir)) return
    const entries = readdirSync(dir, { withFileTypes: true }).sort((a, b) =>
      a.name < b.name ? -1 : 1
    )
    for (const entry of entries) {
      const full = resolve(dir, entry.name)
      if (entry.isDirectory()) {
        if (
          entry.name === 'node_modules' ||
          entry.name === 'generated' ||
          full === generatedDir
        )
          continue
        walk(full)
      } else if (entry.isFile() && isGeneratorSourceFile(entry.name)) {
        if (seen.has(full)) continue
        seen.add(full)
        parts.push(`${full}\0${readFileSync(full, 'utf-8')}`)
      }
    }
  }
  for (const root of sourceRoots) walk(root)
  return hash(parts.join('\0'))
}

let generateCache: Record<string, string> = {}
let generateCachePath = ''

function getCacheDir() {
  let dir = process.cwd()
  while (dir !== '/') {
    const nm = resolve(dir, 'node_modules')
    if (existsSync(nm)) {
      const cacheDir = resolve(nm, '.on-zero')
      if (!existsSync(cacheDir)) {
        mkdirSync(cacheDir, { recursive: true })
      }
      return cacheDir
    }
    dir = resolve(dir, '..')
  }
  return null
}

function loadCache() {
  const cacheDir = getCacheDir()
  if (!cacheDir) return
  generateCachePath = resolve(cacheDir, 'generate-cache.json')
  try {
    generateCache = JSON.parse(readFileSync(generateCachePath, 'utf-8'))
  } catch {
    generateCache = {}
  }
}

function saveCache() {
  if (generateCachePath) {
    writeFileSync(generateCachePath, JSON.stringify(generateCache) + '\n', 'utf-8')
  }
}

function writeFileIfChanged(filePath: string, content: string): boolean {
  const contentHash = hash(content)

  // compare against the bytes actually on disk, never against the cache. the
  // cache records what this generator last WROTE to a path, and a checkout,
  // merge, revert or hand edit replaces the file without touching it, so a
  // cache hit does not mean the file matches. trusting it skipped the write
  // and left stale generated output that the next build silently consumed.
  let onDisk: string | null = null
  try {
    onDisk = readFileSync(filePath, 'utf-8')
  } catch {}

  if (onDisk !== null && hash(onDisk) === contentHash) {
    generateCache[filePath] = contentHash
    return false
  }

  writeFileSync(filePath, content, 'utf-8')
  generateCache[filePath] = contentHash
  return true
}

// file-content emitters and valibot helpers are imported from ./generate-helpers
// so they can be shared with the browser-safe generate-lite entry point.

function createTypeResolver(project: Project) {
  return {
    project,
    async resolveType(node: ts.TypeNode): Promise<Type | null> {
      try {
        return (await project.checker.getTypeFromTypeNode(node)) ?? null
      } catch {
        return null
      }
    },
    async typeToValibot(type: Type): Promise<string> {
      return tsTypeToValibot(project, type)
    },
  }
}

// find a specific exported arrow function's Nth parameter type in a checker-owned source file
async function resolveParamType(
  resolver: ReturnType<typeof createTypeResolver>,
  sourceFile: ts.SourceFile,
  exportName: string,
  paramIndex: number
): Promise<Type | null> {
  let typeNode: ts.TypeNode | null = null

  sourceFile.forEachChild((node) => {
    if (typeNode) return
    if (!ts.isVariableStatement(node)) return
    if (!node.modifiers?.some((m) => m.kind === ts.SyntaxKind.ExportKeyword)) return

    const decl = node.declarationList.declarations[0]
    if (!decl || !ts.isVariableDeclaration(decl)) return
    if (decl.name.getText(sourceFile) !== exportName) return

    if (decl.initializer && ts.isArrowFunction(decl.initializer)) {
      const param = decl.initializer.parameters[paramIndex]
      if (param?.type) {
        typeNode = param.type
      }
    }
  })

  return typeNode ? resolver.resolveType(typeNode) : null
}

// positional shape of a `mutations(...)` call, matching the runtime overloads:
//   mutations(handlers)
//   mutations(table, permissions)
//   mutations(table, permissions, handlers)
//   mutations(table, permissions, handlers, { crud: false })
// arg 1 is always permissions and arg 3 is always options, so neither may ever
// be read as the handlers object.
function readMutationsCall(call: ts.CallExpression): {
  hasTable: boolean
  crud: boolean
  handlersArg: ts.ObjectLiteralExpression | null
} {
  const args = call.arguments

  if (args.length < 2) {
    const only = args[0]
    return {
      hasTable: false,
      crud: false,
      handlersArg: only && ts.isObjectLiteralExpression(only) ? only : null,
    }
  }

  const handlers = args[2]
  const options = args[3]

  let crud = true
  if (options && ts.isObjectLiteralExpression(options)) {
    for (const prop of options.properties) {
      if (!ts.isPropertyAssignment(prop)) continue
      if (
        !(ts.isIdentifier(prop.name) || ts.isStringLiteral(prop.name)) ||
        prop.name.text !== 'crud'
      )
        continue
      // only a literal opt-out removes the generated slots. a computed value
      // keeps them, since extra validators are inert but missing ones are not.
      if (prop.initializer.kind === ts.SyntaxKind.FalseKeyword) crud = false
    }
  }

  return {
    hasTable: true,
    crud,
    handlersArg: handlers && ts.isObjectLiteralExpression(handlers) ? handlers : null,
  }
}

// find mutation handler param types in a resolver-owned source file
// walks `export const mutate = mutations(..., { handlerName: async (ctx, param: Type) => ... })`
async function resolveMutationParamTypes(
  resolver: ReturnType<typeof createTypeResolver>,
  sourceFile: ts.SourceFile
): Promise<Map<string, Type>> {
  const nodes = new Map<string, ts.TypeNode>()

  sourceFile.forEachChild((node) => {
    if (!ts.isVariableStatement(node)) return
    if (!node.modifiers?.some((m) => m.kind === ts.SyntaxKind.ExportKeyword)) return

    const decl = node.declarationList.declarations[0]
    if (!decl || !ts.isVariableDeclaration(decl)) return
    if (decl.name.getText(sourceFile) !== 'mutate') return

    if (!decl.initializer || !ts.isCallExpression(decl.initializer)) return

    const { handlersArg } = readMutationsCall(decl.initializer)
    if (!handlersArg) return

    for (const prop of handlersArg.properties) {
      if (!ts.isPropertyAssignment(prop) && !ts.isMethodDeclaration(prop)) continue
      const name = prop.name?.getText(sourceFile)
      if (!name) continue

      let params: ts.NodeArray<ts.ParameterDeclaration> | null = null
      if (ts.isPropertyAssignment(prop)) {
        const init = prop.initializer
        if (ts.isArrowFunction(init) || ts.isFunctionExpression(init)) {
          params = init.parameters
        }
      } else if (ts.isMethodDeclaration(prop)) {
        params = prop.parameters
      }

      if (!params || params.length < 2) continue
      const typeNode = params[1]!.type
      if (!typeNode) continue

      nodes.set(name, typeNode)
    }
  })

  const resolved = new Map<string, Type>()
  for (const [name, node] of nodes) {
    const expanded = await resolver.resolveType(node)
    if (expanded) resolved.set(name, expanded)
  }
  return resolved
}

function extractMutationsFromModel(
  sourceFile: ts.SourceFile,
  content: string,
  fileName: string,
  silent: boolean,
  typeToValibot: (typeString: string) => string | null,
  resolvedValibot?: Map<string, string>
): ModelMutations | null {
  let mutateNode: ts.CallExpression | null = null

  // find `export const mutate = mutations(...)`
  sourceFile.forEachChild((node) => {
    if (!ts.isVariableStatement(node)) return
    if (!node.modifiers?.some((m) => m.kind === ts.SyntaxKind.ExportKeyword)) return
    const decl = node.declarationList.declarations[0]
    if (!decl || !ts.isVariableDeclaration(decl)) return
    if (decl.name.getText(sourceFile) !== 'mutate') return
    if (decl.initializer && ts.isCallExpression(decl.initializer)) {
      mutateNode = decl.initializer
    }
  })

  if (!mutateNode) {
    return {
      modelName: basename(fileName, '.ts'),
      hasCRUD: false,
      columns: {},
      primaryKeys: [],
      custom: [],
    }
  }

  const call = mutateNode as ts.CallExpression

  // string-named and table-builder registrations behave identically at runtime:
  // any 2+ arg call registers permissions and generates crud unless it opted out
  const { hasTable, crud, handlersArg } = readMutationsCall(call)
  const hasCRUD = hasTable && crud

  // extract schema columns for CRUD generation
  const columns: Record<string, SchemaColumn> = {}
  const primaryKeys: string[] = []

  if (hasCRUD) {
    // parse schema columns from file content
    extractSchemaColumns(sourceFile, columns, primaryKeys)
  }

  // extract custom mutation param types
  const custom: ExtractedMutation[] = []

  if (handlersArg) {
    for (const prop of handlersArg.properties) {
      if (!ts.isPropertyAssignment(prop) && !ts.isMethodDeclaration(prop)) continue

      const name = prop.name?.getText(sourceFile)
      if (!name) continue

      // find the arrow function or method
      let params: ts.NodeArray<ts.ParameterDeclaration> | null = null

      if (ts.isPropertyAssignment(prop)) {
        const init = prop.initializer
        if (ts.isArrowFunction(init) || ts.isFunctionExpression(init)) {
          params = init.parameters
        }
      } else if (ts.isMethodDeclaration(prop)) {
        params = prop.parameters
      }

      if (!params) continue

      // second param is the mutation data (first is ctx)
      if (params.length < 2) {
        custom.push({ name, paramType: 'void', valibotCode: '' })
        continue
      }

      const secondParam = params[1]!
      const paramType = secondParam.type?.getText(sourceFile) || 'unknown'

      if (paramType === 'unknown') {
        custom.push({ name, paramType: 'unknown', valibotCode: '' })
        continue
      }

      let valibotCode = typeToValibot(paramType)

      // if direct parse failed (unresolved reference), use checker-resolved types
      if (!valibotCode && resolvedValibot) {
        valibotCode = resolvedValibot.get(name) ?? null
      }

      custom.push({
        name,
        paramType,
        valibotCode: valibotCode || '',
      })
    }
  }

  return {
    modelName: '',
    hasCRUD,
    columns,
    primaryKeys,
    custom,
  }
}

function extractSchemaColumns(
  sourceFile: ts.SourceFile,
  columns: Record<string, SchemaColumn>,
  primaryKeys: string[]
) {
  // walk AST to find table(...).columns({...}).primaryKey(...)
  function visit(node: ts.Node) {
    if (ts.isCallExpression(node)) {
      const text = node.expression.getText(sourceFile)

      // look for .primaryKey('id') or .primaryKey('id', 'otherId')
      if (text.endsWith('.primaryKey')) {
        for (const arg of node.arguments) {
          if (ts.isStringLiteral(arg)) {
            primaryKeys.push(arg.text)
          }
        }
      }

      // look for .columns({...})
      if (text.endsWith('.columns') && node.arguments.length === 1) {
        const obj = node.arguments[0]!
        if (ts.isObjectLiteralExpression(obj)) {
          for (const prop of obj.properties) {
            if (!ts.isPropertyAssignment(prop)) continue
            const colName = prop.name?.getText(sourceFile)
            if (!colName) continue

            const initText = prop.initializer.getText(sourceFile)
            const colType = parseColumnType(initText)
            columns[colName] = colType
          }
        }
      }
    }
    node.forEachChild(visit)
  }
  visit(sourceFile)
}

// convert a ts.Type to valibot code by walking the type checker AST
async function tsTypeToValibot(
  project: Project,
  type: Type,
  seen = new Set<number>()
): Promise<string> {
  const checker = project.checker
  // prevent infinite recursion on circular types
  // only track structured types (objects, intersections) — not primitives/unions
  const flags = type.flags
  if (flags & (TypeFlags.Object | TypeFlags.Intersection)) {
    if (seen.has(type.id)) return 'v.unknown()'
    seen.add(type.id)
  }

  const recurse = (value: Type) => tsTypeToValibot(project, value, seen)

  // primitives
  if (flags & TypeFlags.String) return 'v.string()'
  if (flags & TypeFlags.Number) return 'v.number()'
  if (flags & TypeFlags.Boolean) return 'v.boolean()'
  if (flags & TypeFlags.Void || flags & TypeFlags.Undefined) return 'v.void_()'
  if (flags & TypeFlags.Null) return 'v.null_()'
  if (flags & TypeFlags.Any || flags & TypeFlags.Unknown) return 'v.unknown()'
  if (flags & TypeFlags.Never) return 'v.never()'
  if (flags & TypeFlags.TemplateLiteral) return 'v.string()'
  if (flags & TypeFlags.Object && (await checker.isArrayLikeType(type))) {
    const typeArgs = type.isTypeReference() ? await checker.getTypeArguments(type) : []
    const indexInfo = (await checker.getIndexInfosOfType(type)).find((info) =>
      Boolean(info.keyType.flags & TypeFlags.Number)
    )
    const elementType = typeArgs.length === 1 ? typeArgs[0] : indexInfo?.valueType
    return `v.array(${elementType ? await recurse(elementType) : 'v.unknown()'})`
  }

  // string/number/boolean literals
  if (type.isStringLiteralType()) {
    return `v.literal(${JSON.stringify(type.value)})`
  }
  if (type.isNumberLiteralType()) {
    return `v.literal(${type.value})`
  }
  if (type.isBooleanLiteralType()) {
    return `v.literal(${type.value})`
  }

  // union
  if (type.isUnionType()) {
    const members = await type.getTypes()
    const hasNull = members.some((member) => member.flags & TypeFlags.Null)
    const hasUndefined = members.some(
      (member) => member.flags & (TypeFlags.Undefined | TypeFlags.Void)
    )
    const rest = members.filter(
      (member) =>
        !(member.flags & (TypeFlags.Null | TypeFlags.Undefined | TypeFlags.Void))
    )
    if (
      rest.length === 2 &&
      rest.every((member) => member.flags & TypeFlags.BooleanLiteral)
    ) {
      let inner = 'v.boolean()'
      if (hasNull) inner = `v.nullable(${inner})`
      if (hasUndefined) inner = `v.optional(${inner})`
      return inner
    }

    if (rest.length === 0) return 'v.unknown()'

    let inner =
      rest.length === 1
        ? await recurse(rest[0]!)
        : `v.union([${(await Promise.all(rest.map(recurse))).join(', ')}])`

    if (hasNull) inner = `v.nullable(${inner})`
    if (hasUndefined) inner = `v.optional(${inner})`
    return inner
  }

  const resolveSymbolType = async (property: Symbol) => {
    const symbolType = await checker.getTypeOfSymbol(property)
    if (symbolType && !symbolType.isErrorType()) return symbolType
    const declaration = property.valueDeclaration ?? property.declarations[0]
    const node = declaration ? await declaration.resolve(project) : undefined
    if (node) return checker.getTypeOfSymbolAtLocation(property, node)
    return checker.getDeclaredTypeOfSymbol(property)
  }

  const objectToValibot = async (props: readonly Symbol[]) => {
    if (props.length === 0) return 'v.object({})'
    const entries: string[] = []
    for (const prop of props) {
      const name = prop.name
      if (shouldSkipObjectKey(name)) continue
      const propType = await resolveSymbolType(prop)
      const isOptional = Boolean(prop.flags & SymbolFlags.Optional)
      let val = await recurse(propType)
      if (isOptional && !val.startsWith('v.optional(')) {
        val = `v.optional(${val})`
      }
      entries.push(`${formatObjectKey(name)}: ${val}`)
    }
    if (entries.length === 0) return 'v.object({})'
    return `v.object({\n    ${entries.join(',\n    ')},\n  })`
  }

  // intersection - use checker's merged properties directly
  if (type.isIntersectionType()) {
    return objectToValibot(await checker.getPropertiesOfType(type))
  }

  if (type.isTupleType()) {
    const typeArgs = await checker.getTypeArguments(type)
    return `v.tuple([${(await Promise.all(typeArgs.map(recurse))).join(', ')}])`
  }

  if (type.isObjectType()) {
    if (type.objectFlags & ObjectFlags.Reference && type.isTypeReference()) {
      const symbol = await type.getSymbol()
      const typeArgs = await checker.getTypeArguments(type)
      if (
        (symbol?.name === 'Array' || symbol?.name === 'ReadonlyArray') &&
        typeArgs.length === 1
      ) {
        return `v.array(${await recurse(typeArgs[0]!)})`
      }
    }
    const props = await checker.getPropertiesOfType(type)
    if (props.length > 0) return objectToValibot(props)
  }

  // index signature / Record type
  for (const indexInfo of await checker.getIndexInfosOfType(type)) {
    if (indexInfo.keyType.flags & TypeFlags.String) {
      return `v.record(v.string(), ${await recurse(indexInfo.valueType)})`
    }
    if (indexInfo.keyType.flags & TypeFlags.Number) {
      return `v.record(v.number(), ${await recurse(indexInfo.valueType)})`
    }
  }

  return 'v.unknown()'
}

type SchemaTable = {
  name: string
  serverName?: string
  columns: Record<string, SchemaColumn>
  primaryKey: readonly string[]
}

type SchemaRelationHop = {
  sourceField: string[]
  destField: string[]
  destSchema: string
  cardinality: 'one' | 'many'
}

type DrizzleZeroSchema = {
  tables: Record<string, SchemaTable>
  relationships: Record<string, Record<string, SchemaRelationHop[]>>
}

function serializeColumn(col: SchemaColumn): string {
  const parts: string[] = []
  parts.push(`type: '${col.type}'`)
  parts.push(`optional: ${col.optional}`)
  parts.push(
    `customType: null as unknown as ${col.type === 'json' ? 'ReadonlyJSONValue' : col.type}`
  )
  if (col.serverName) {
    parts.push(`serverName: '${col.serverName}'`)
  }
  return `{ ${parts.join(', ')} }`
}

function serializeColumnBuilder(col: SchemaColumn): string {
  const zeroType =
    col.type === 'string'
      ? 'string'
      : col.type === 'number'
        ? 'number'
        : col.type === 'boolean'
          ? 'boolean'
          : 'json'
  let expr = `${zeroType}()`
  if (col.serverName) {
    expr += `.from('${col.serverName}')`
  }
  if (col.optional) {
    expr += '.optional()'
  }
  return expr
}

/**
 * generate a typed schema.ts from drizzle-zero output.
 * produces a file using table()/createSchema()/relationships() from @rocicorp/zero
 * so the full type system works (no `relationships: any`).
 *
 */
export function generateDrizzleSchemaFile(schema: DrizzleZeroSchema): string {
  const lines: string[] = [
    `// auto-generated by: on-zero generate (from drizzle schema)`,
    `import { boolean, createSchema, json, number, relationships, string, table } from '@rocicorp/zero'`,
    ``,
  ]

  const tableNames = Object.keys(schema.tables).sort()

  // emit table consts using Zero's builder API
  for (const tableName of tableNames) {
    const t = schema.tables[tableName]!
    const colEntries = Object.entries(t.columns)
      .map(([colName, col]) => `    ${colName}: ${serializeColumnBuilder(col)},`)
      .join('\n')
    const pkArgs = t.primaryKey.map((k) => `'${k}'`).join(', ')

    const tableSource = t.serverName
      ? `table(${JSON.stringify(t.name)}).from(${JSON.stringify(t.serverName)})`
      : `table(${JSON.stringify(t.name)})`
    lines.push(`const ${tableName}Table = ${tableSource}`)
    lines.push(`  .columns({`)
    lines.push(colEntries)
    lines.push(`  })`)
    lines.push(`  .primaryKey(${pkArgs})`)
    lines.push(``)
  }

  // emit relationship consts
  const relTableNames = Object.keys(schema.relationships).sort()
  for (const tableName of relTableNames) {
    const rels = schema.relationships[tableName]!
    const relEntries = Object.entries(rels)
    if (relEntries.length === 0) continue

    const relBody = relEntries
      .map(([relName, hops]) => {
        // each relationship is an array of hops (usually 1, 2 for many-to-many)
        if (hops.length === 1) {
          const hop = hops[0]!
          const fn = hop.cardinality === 'one' ? 'one' : 'many'
          const sf = hop.sourceField.map((f) => `'${f}'`).join(', ')
          const df = hop.destField.map((f) => `'${f}'`).join(', ')
          return `    ${relName}: ${fn}({\n      sourceField: [${sf}],\n      destSchema: ${hop.destSchema}Table,\n      destField: [${df}],\n    })`
        }
        // many-to-many (2 hops)
        if (hops.length !== 2) {
          throw new Error(
            `Relationship ${tableName}.${relName} must have one or two hops`
          )
        }
        const cardinality = hops[0]!.cardinality
        if (hops.some((hop) => hop.cardinality !== cardinality)) {
          throw new Error(`Relationship ${tableName}.${relName} must use one cardinality`)
        }
        const fn = cardinality === 'one' ? 'one' : 'many'
        const hopCode = hops
          .map((hop) => {
            const sf = hop.sourceField.map((f) => `'${f}'`).join(', ')
            const df = hop.destField.map((f) => `'${f}'`).join(', ')
            return `{ sourceField: [${sf}], destSchema: ${hop.destSchema}Table, destField: [${df}] }`
          })
          .join(', ')
        return `    ${relName}: ${fn}(${hopCode})`
      })
      .join(',\n')

    lines.push(
      `const ${tableName}Relationships = relationships(${tableName}Table, ({ one, many }) => ({`
    )
    lines.push(relBody)
    lines.push(`}))`)
    lines.push(``)
  }

  // emit createSchema
  const tableList = tableNames.map((n) => `  ${n}Table,`).join('\n')
  const relList = relTableNames
    .filter((n) => Object.keys(schema.relationships[n]!).length > 0)
    .map((n) => `  ${n}Relationships,`)
    .join('\n')

  lines.push(`export const schema = createSchema({`)
  lines.push(`  tables: [`)
  lines.push(tableList)
  lines.push(`  ],`)
  lines.push(`  relationships: [`)
  lines.push(relList)
  lines.push(`  ],`)
  lines.push(`})`)
  lines.push(``)

  return lines.join('\n')
}

export interface GenerateOptions {
  /** base data directory */
  dir: string
  /** explicit on-zero.config.ts path; auto-discovered in `dir` when omitted */
  config?: string
  /** run after generation */
  after?: string
  /** suppress output */
  silent?: boolean
  /** ignore the generation cache */
  force?: boolean
}

export interface WatchOptions extends GenerateOptions {
  /** debounce delay in ms */
  debounce?: number
}

export interface GenerateResult {
  filesChanged: number
  modelCount: number
  schemaCount: number
  queryCount: number
  mutationCount: number
}

export type DataMembership = {
  instances: Record<
    string,
    {
      tables: string[]
      syncTables: string[]
      supportTables: string[]
      scope: string | null
    }
  >
  allTables: string[]
}

function dataMembershipFromLayout(layout: DataLayout): DataMembership {
  return {
    instances: Object.fromEntries(
      layout.instances.map((instance) => [
        instance.name,
        {
          tables: [...instance.tables],
          syncTables: [...instance.syncTables],
          supportTables: [...instance.supportTables],
          scope: instance.scope,
        },
      ])
    ),
    allTables: [
      ...new Set(
        layout.instances.flatMap((instance) => [
          ...instance.syncTables,
          ...instance.supportTables,
        ])
      ),
    ].sort(),
  }
}

async function loadGeneratorProject(baseDir: string): Promise<NativeTypeScriptProject> {
  const sourcePaths = collectTypeScriptSourcePaths([
    baseDir,
    resolve(dirname(baseDir), 'database'),
  ])
  return createNativeTypeScriptProject(baseDir, sourcePaths)
}

export async function deriveDataMembership(options: {
  dir: string
  config?: string
}): Promise<DataMembership> {
  const baseDir = resolve(options.dir)
  const project = await loadGeneratorProject(baseDir)
  try {
    const layout = discoverDataLayout(
      project,
      baseDir,
      options.config ? resolve(options.config) : undefined
    )
    return dataMembershipFromLayout(layout)
  } finally {
    await project.close()
  }
}

export async function generateDrizzleSchemaInputFile(options: {
  dir: string
  schemaImportPath: string
  config?: string
}): Promise<string> {
  const baseDir = resolve(options.dir)
  const project = await loadGeneratorProject(baseDir)
  try {
    return generateDrizzleSchemaInputFileWithProject(options, project)
  } finally {
    await project.close()
  }
}

async function generateDrizzleSchemaInputFileWithProject(
  options: { dir: string; schemaImportPath: string; config?: string },
  project: NativeTypeScriptProject
): Promise<string> {
  const baseDir = resolve(options.dir)
  const layout = discoverDataLayout(
    project,
    baseDir,
    options.config ? resolve(options.config) : undefined
  )
  const tableNames = dataMembershipFromLayout(layout).allTables
  const relationsPath =
    layout.metadataPaths.find(
      (path) =>
        basename(path) === 'relations.ts' &&
        dirname(path) === resolve(dirname(baseDir), 'database')
    ) ?? layout.metadataPaths.find((path) => basename(path) === 'relations.ts')
  const relationEntries: string[] = []

  if (relationsPath) {
    const source = project.sourceFile(relationsPath)
    const included = new Set(tableNames)
    const visit = (node: ts.Node) => {
      if (
        !ts.isCallExpression(node) ||
        node.expression.getText(source) !== 'defineRelations'
      ) {
        node.forEachChild(visit)
        return
      }
      const factory = node.arguments[1]
      if (
        !factory ||
        (!ts.isArrowFunction(factory) && !ts.isFunctionExpression(factory))
      ) {
        return
      }
      const body = ts.isParenthesizedExpression(factory.body)
        ? factory.body.expression
        : factory.body
      if (!ts.isObjectLiteralExpression(body)) return

      for (const tableProperty of body.properties) {
        if (
          !ts.isPropertyAssignment(tableProperty) ||
          !ts.isObjectLiteralExpression(tableProperty.initializer)
        ) {
          continue
        }
        const table = tableProperty.name.getText(source).replace(/^['"]|['"]$/g, '')
        if (!included.has(table)) continue
        const relations: string[] = []
        for (const relationProperty of tableProperty.initializer.properties) {
          if (
            !ts.isPropertyAssignment(relationProperty) ||
            !ts.isCallExpression(relationProperty.initializer) ||
            !ts.isPropertyAccessExpression(relationProperty.initializer.expression)
          ) {
            continue
          }
          const target = relationProperty.initializer.expression.name.text
          if (included.has(target)) relations.push(relationProperty.getText(source))
        }
        relationEntries.push(
          `  ${tableProperty.name.getText(source)}: {${relations.length ? `\n${relations.map((relation) => `    ${relation},`).join('\n')}\n  ` : ''}},`
        )
      }
    }
    visit(source)
  }

  const schemaImportPath = JSON.stringify(options.schemaImportPath)
  return [
    '// auto-generated from the on-zero data layout',
    `import { defineRelations } from 'drizzle-orm'`,
    `import * as schema from ${schemaImportPath}`,
    '',
    `export { ${tableNames.join(', ')} } from ${schemaImportPath}`,
    `export const relations = defineRelations(schema, (r) => ({`,
    ...relationEntries,
    `}))`,
    '',
  ].join('\n')
}

export async function generate(options: GenerateOptions): Promise<GenerateResult> {
  const project = await loadGeneratorProject(resolve(options.dir))
  try {
    return await generateWithProject(options, project)
  } finally {
    await project.close()
  }
}

async function generateWithProject(
  options: GenerateOptions,
  project: NativeTypeScriptProject
): Promise<GenerateResult> {
  const { dir, after, silent, force, config } = options
  const baseDir = resolve(dir)
  const generatedDir = resolve(baseDir, 'generated')

  if (!existsSync(generatedDir)) {
    mkdirSync(generatedDir, { recursive: true })
  }

  loadCache()

  // the layout pass is intentionally first: config, filenames, and related()
  // calls determine schema membership before any type program exists.
  const layout = discoverDataLayout(
    project,
    baseDir,
    config ? resolve(config) : undefined
  )
  const metadataHash = hash(
    layout.metadataPaths
      .map((path) => `${path}\0${readFileSync(path, 'utf8')}`)
      .join('\0')
  )

  // input-freshness gate: if nothing under baseDir changed since the last
  // COMPLETED generate (the input hash is only stored at the end, after
  // saveCache), the outputs are already current — skip the typescript-program
  // build entirely and return the cached counts. the configureServer watcher
  // still re-runs generate on real model/query edits.
  const databaseSchemaPath = resolve(dirname(baseDir), 'database/schema.ts')
  const needsSqliteZeroSchema = existsSync(databaseSchemaPath)
  const inputHash = hash(
    `${hashInputTree(layout.sourceRoots, generatedDir)}\0${metadataHash}`
  )
  if (
    !force &&
    generateCache.__generatorVersion === GENERATOR_CACHE_VERSION &&
    generateCache.__inputHash === inputHash &&
    // the input hash deliberately skips the generated directory, so it cannot
    // notice that the outputs themselves moved. existence is not correctness
    // either: a checkout, merge or revert leaves every file present and stale.
    // confirm the bytes on disk still match what the cache says it wrote there
    // before skipping the build, or the fast path serves someone else's output.
    //
    // a consumer that formats generated output after this runs never hits the
    // fast path again, because the cache holds the bytes written here and disk
    // holds the formatted ones. soot does exactly that, `on-zero generate &&
    // generate-instance-tables && oxfmt src/data/generated/`, and the number is
    // already taken: 2.50s median before, 3.69s after, four warm runs each in
    // one checkout with only the pin swapped, so about 1.2 seconds or 47
    // percent on this step. the full three-command script went 2.65s to 3.14s
    // with a spread that swallows most of it.
    //
    // the shape matters more than the number. this is not a cache that hits
    // less often, it is a cache that never hits, so the cost does not degrade
    // gracefully and grows with the generated output rather than staying at
    // 1.2s. correct and slower beats fast and stale, which is why it is written
    // this way, but if generate time ever becomes the complaint, record the
    // hash after the consumer's formatter runs rather than weakening this.
    Object.entries(generateCache).every(([cachedPath, cachedHash]) => {
      if (!cachedPath.startsWith(generatedDir)) return true
      try {
        return hash(readFileSync(cachedPath, 'utf-8')) === cachedHash
      } catch {
        return false
      }
    }) &&
    existsSync(resolve(generatedDir, 'models.ts')) &&
    (!needsSqliteZeroSchema || existsSync(resolve(generatedDir, 'schema.ts')))
  ) {
    let counts: Partial<GenerateResult> = {}
    try {
      counts = JSON.parse(generateCache.__counts || '{}')
    } catch {}
    return {
      filesChanged: 0,
      modelCount: counts.modelCount ?? 0,
      schemaCount: counts.schemaCount ?? 0,
      queryCount: counts.queryCount ?? 0,
      mutationCount: counts.mutationCount ?? 0,
    }
  }

  const modelNamespaces = layout.namespaces.filter(
    (namespace): namespace is typeof namespace & { modelPath: string } =>
      namespace.modelPath !== null
  )
  const filesWithSchema = modelNamespaces.filter((namespace) =>
    readFileSync(namespace.modelPath, 'utf-8').includes('export const schema = table(')
  )
  const modelModules = modelNamespaces.map((namespace) => ({
    name: namespace.name,
    importPath: namespaceImportPath(baseDir, namespace.modelPath),
  }))
  const schemaModules = filesWithSchema.map((namespace) => ({
    name: namespace.name,
    importPath: namespaceImportPath(baseDir, namespace.modelPath),
  }))
  const writeResults = [
    writeFileIfChanged(
      resolve(generatedDir, 'models.ts'),
      generateModelsFile(modelModules)
    ),
    // only generate types.ts and tables.ts when model files define schemas.
    // when using drizzle-zero CLI for schema generation, these files are
    // managed externally and should not be overwritten.
    ...(filesWithSchema.length > 0
      ? [
          writeFileIfChanged(
            resolve(generatedDir, 'types.ts'),
            generateTypesFile(schemaModules.map((module) => module.name))
          ),
          writeFileIfChanged(
            resolve(generatedDir, 'tables.ts'),
            generateTablesFile(schemaModules)
          ),
        ]
      : []),
    writeFileIfChanged(resolve(generatedDir, 'README.md'), generateReadmeFile()),
  ]

  let filesChanged = writeResults.filter(Boolean).length
  if (needsSqliteZeroSchema) {
    const membership = dataMembershipFromLayout(layout)
    const drizzleSchema = await generateDrizzleSchemaInputFileWithProject(
      {
        dir: baseDir,
        schemaImportPath: '../../database/schema',
        config,
      },
      project
    )
    const sqliteSchema = renderDrizzleZeroSqliteSchemaModule({
      importPath: './drizzleSchema',
      tableNames: membership.allTables,
    })
    if (writeFileIfChanged(resolve(generatedDir, 'drizzleSchema.ts'), drizzleSchema)) {
      filesChanged++
    }
    if (writeFileIfChanged(resolve(generatedDir, 'schema.ts'), sqliteSchema)) {
      filesChanged++
    }
  }
  let queryCount = 0
  let mutationCount = 0

  // lightweight string-based parser for inline type annotations from source text
  // handles simple cases: primitives, inline objects, arrays
  // returns null for type references that need the checker to resolve
  const typeToValibot = (paramType: string): string | null => {
    try {
      return parseTypeString(paramType.trim())
    } catch {
      return null
    }
  }

  const allQueries: Array<{
    name: string
    params: string
    valibotCode: string
    sourceFile: string
    importPath: string
  }> = []

  for (const namespace of layout.namespaces.filter(
    (namespace): namespace is typeof namespace & { queryPath: string } =>
      namespace.queryPath !== null
  )) {
    const filePath = namespace.queryPath

    try {
      const sourceFile = project.sourceFile(filePath)
      const candidates: Array<{
        name: string
        paramType: string
        typeNode: ts.TypeNode | null
      }> = []

      sourceFile.forEachChild((node) => {
        if (ts.isVariableStatement(node)) {
          const exportModifier = node.modifiers?.find(
            (m) => m.kind === ts.SyntaxKind.ExportKeyword
          )
          if (!exportModifier) return

          const declaration = node.declarationList.declarations[0]
          if (!declaration || !ts.isVariableDeclaration(declaration)) return

          const name = declaration.name.getText(sourceFile)
          if (['mutate', 'permission', 'schema', 'where'].includes(name)) return

          if (declaration.initializer && ts.isArrowFunction(declaration.initializer)) {
            const params = declaration.initializer.parameters
            let paramType = 'void'

            if (params.length > 0) {
              const param = params[0]!
              paramType = param.type?.getText(sourceFile) || 'unknown'
            }
            candidates.push({ name, paramType, typeNode: params[0]?.type ?? null })
          }
        }
      })

      const resolver = createTypeResolver(project.projectForFile(filePath))
      for (const candidate of candidates) {
        let valibotCode = typeToValibot(candidate.paramType)
        if (!valibotCode && candidate.typeNode) {
          const resolvedType = await resolveParamType(
            resolver,
            sourceFile,
            candidate.name,
            0
          )
          if (resolvedType) valibotCode = await resolver.typeToValibot(resolvedType)
        }
        if (valibotCode) {
          allQueries.push({
            name: candidate.name,
            params: candidate.paramType,
            valibotCode,
            sourceFile: namespace.name,
            importPath: namespaceImportPath(baseDir, filePath),
          })
        } else if (!silent && candidate.paramType !== 'void') {
          console.error(
            `✗ ${candidate.name}: could not resolve type "${candidate.paramType}"`
          )
        }
      }
    } catch (err) {
      if (!silent) console.error(`Error processing ${filePath}:`, err)
    }
  }

  queryCount = allQueries.length

  const groupedChanged = writeFileIfChanged(
    resolve(generatedDir, 'groupedQueries.ts'),
    generateGroupedQueriesFile(allQueries)
  )
  const syncedChanged = writeFileIfChanged(
    resolve(generatedDir, 'syncedQueries.ts'),
    generateSyncedQueriesFile(allQueries)
  )

  if (groupedChanged) filesChanged++
  if (syncedChanged) filesChanged++

  const instancesChanged = writeFileIfChanged(
    resolve(generatedDir, 'instances.ts'),
    generateInstancesFile(
      layout.instances.map((instance) => ({
        name: instance.name,
        scope: instance.scope,
        queryNames: instance.namespaces
          .map((namespace) => namespace.name)
          .filter((name) => allQueries.some((query) => query.sourceFile === name)),
        modelNames: instance.namespaces
          .filter((namespace) => namespace.modelPath)
          .map((namespace) => namespace.name),
        tables: instance.tables,
        syncTables: instance.syncTables,
        supportTables: instance.supportTables,
      }))
    )
  )
  if (instancesChanged) filesChanged++

  const aggregateNamespaces = layout.namespaces.filter(
    (namespace): namespace is typeof namespace & { aggregatePath: string } =>
      namespace.aggregatePath !== null
  )
  if (aggregateNamespaces.length > 0) {
    const aggregatesChanged = writeFileIfChanged(
      resolve(generatedDir, 'aggregates.ts'),
      generateAggregatesFile(
        aggregateNamespaces.map((namespace) => ({
          name: namespace.name,
          importPath: namespaceImportPath(baseDir, namespace.aggregatePath),
        }))
      )
    )
    if (aggregatesChanged) filesChanged++
  }

  // generate mutation validators from model files
  const allModelMutations: ModelMutations[] = []

  // first pass: extract mutations, note which have unresolved types
  const unresolvedModels: Array<{ baseName: string; filePath: string }> = []

  for (const namespace of modelNamespaces) {
    const filePath = namespace.modelPath
    const fileBaseName = namespace.name

    try {
      const content = readFileSync(filePath, 'utf-8')

      const sourceFile = project.sourceFile(filePath)
      const result = extractMutationsFromModel(
        sourceFile,
        content,
        filePath,
        !!silent,
        typeToValibot
      )

      if (result) {
        result.modelName = fileBaseName
        allModelMutations.push(result)

        // check if any custom mutations have unresolved types
        const hasUnresolved = result.custom.some(
          (m) => m.paramType !== 'void' && m.paramType !== 'unknown' && !m.valibotCode
        )
        if (hasUnresolved) {
          unresolvedModels.push({ baseName: fileBaseName, filePath })
        }
      }
    } catch (err) {
      if (!silent) console.error(`Error extracting mutations from ${filePath}:`, err)
    }
  }

  // second pass: resolve imported types using TypeChecker
  if (unresolvedModels.length > 0) {
    for (const { baseName, filePath } of unresolvedModels) {
      const sourceFile = project.sourceFile(filePath)
      const modelResolver = createTypeResolver(project.projectForFile(filePath))
      const resolvedTypes = await resolveMutationParamTypes(modelResolver, sourceFile)
      if (resolvedTypes.size === 0) continue
      const resolvedValibot = new Map<string, string>()
      for (const [name, type] of resolvedTypes) {
        resolvedValibot.set(name, await modelResolver.typeToValibot(type))
      }

      // re-extract with resolved types
      const content = readFileSync(filePath, 'utf-8')
      const result = extractMutationsFromModel(
        sourceFile,
        content,
        filePath,
        !!silent,
        typeToValibot,
        resolvedValibot
      )

      if (result) {
        result.modelName = baseName
        // replace the old entry
        const idx = allModelMutations.findIndex((m) => m.modelName === baseName)
        if (idx >= 0) allModelMutations[idx] = result
      }
    }
  }

  // count total mutations (CRUD + custom)
  for (const model of allModelMutations) {
    if (model.hasCRUD) mutationCount += CRUD_MUTATION_NAMES.length
    mutationCount += model.custom.filter(
      (m) => !model.hasCRUD || !CRUD_MUTATION_NAMES.some((name) => name === m.name)
    ).length
  }

  if (allModelMutations.length > 0) {
    const mutationsChanged = writeFileIfChanged(
      resolve(generatedDir, 'syncedMutations.ts'),
      generateSyncedMutationsFile(allModelMutations)
    )
    if (mutationsChanged) filesChanged++
  }

  if (filesChanged > 0 && !silent) {
    console.info(
      `✓ ${modelNamespaces.length} models (${filesWithSchema.length} schemas)${queryCount ? `, ${queryCount} queries` : ''}${mutationCount ? `, ${mutationCount} mutations` : ''}`
    )
  }

  // run after command
  if (filesChanged > 0 && after) {
    const { execSync } = await import('node:child_process')
    try {
      execSync(after, {
        stdio: 'inherit',
        env: { ...process.env, ON_ZERO_GENERATED_DIR: generatedDir },
      })
    } catch (err) {
      if (!silent) console.error(`Error running after command: ${err}`)
    }
  }

  // record the input hash + counts so the next boot can skip a no-op regen.
  generateCache.__generatorVersion = GENERATOR_CACHE_VERSION
  generateCache.__inputHash = inputHash
  generateCache.__counts = JSON.stringify({
    modelCount: modelNamespaces.length,
    schemaCount: filesWithSchema.length,
    queryCount,
    mutationCount,
  })
  saveCache()

  return {
    filesChanged,
    modelCount: modelNamespaces.length,
    schemaCount: filesWithSchema.length,
    queryCount,
    mutationCount,
  }
}

export async function watch(options: WatchOptions) {
  const { dir, debounce = 1000 } = options
  const baseDir = resolve(dir)
  const generatedDir = resolve(baseDir, 'generated')

  // initial run (silent)
  await generate({ ...options, silent: true })
  console.info('👀 watching...\n')

  const chokidar = await import('chokidar')

  let debounceTimer: ReturnType<typeof setTimeout> | null = null

  const debouncedRegenerate = (path: string, event: string) => {
    if (debounceTimer) clearTimeout(debounceTimer)
    console.info(`\n${event} ${path}`)
    debounceTimer = setTimeout(() => {
      generate({ ...options, silent: false })
    }, debounce)
  }

  const databaseDir = resolve(dirname(baseDir), 'database')
  const project = await loadGeneratorProject(baseDir)
  let layout: DataLayout
  try {
    layout = discoverDataLayout(
      project,
      baseDir,
      options.config ? resolve(options.config) : undefined
    )
  } finally {
    await project.close()
  }
  const watcher = chokidar.watch(
    [
      ...new Set([
        ...layout.sourceRoots,
        ...(existsSync(databaseDir) ? [databaseDir] : []),
      ]),
    ],
    {
      persistent: true,
      ignoreInitial: true,
      ignored: [generatedDir, /node_modules/],
    }
  )

  watcher.on('change', (path) => debouncedRegenerate(path, '📝'))
  watcher.on('add', (path) => debouncedRegenerate(path, '➕'))
  watcher.on('unlink', (path) => debouncedRegenerate(path, '🗑️ '))

  return watcher
}
