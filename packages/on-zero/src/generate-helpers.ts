// pure helpers shared by `generate.ts` (node CLI, full type resolution via
// the typescript compiler API) and `generate-lite.ts` (browser-safe, no
// typescript dependency). nothing in this file may import `typescript`,
// `node:fs`, `node:path`, `node:crypto`, or touch process/disk. callers
// that need those concerns handle them themselves.

// shared types

export type SchemaColumn = {
  type: string
  optional: boolean
  customType: unknown
  serverName?: string
}

export type ExtractedMutation = {
  name: string
  // 'void' | inline type string | unresolved reference
  paramType: string
  // generated valibot code, or '' when the payload is void/unknown
  valibotCode: string
}

export type ModelMutations = {
  modelName: string
  hasCRUD: boolean
  // populated when hasCRUD
  columns: Record<string, SchemaColumn>
  primaryKeys: string[]
  custom: ExtractedMutation[]
}

// identifier helpers

export function shouldSkipObjectKey(name: string): boolean {
  // typescript exposes symbol keys as synthetic names like "__@iterator@851".
  // these cannot come from json mutation payloads and break codegen if emitted.
  return name.startsWith('__@')
}

export function formatObjectKey(name: string): string {
  return /^[$A-Z_a-z][$\w]*$/.test(name) ? name : JSON.stringify(name)
}

// user → userPublic is a special case carried over from the existing generator
export function getModelImportName(name: string): string {
  return name === 'user' ? 'userPublic' : name
}

/**
 * Assigns each name a module alias that cannot collide with another namespace's
 * name or with another alias. `app` and `appSource` are both legal namespaces,
 * so a plain `${name}Source` suffix is not safe on its own; the same is true of
 * the `user` → `userPublic` rename above when both namespaces exist. Emitting a
 * duplicate identifier produces a generated file that does not compile, so the
 * disambiguation has to happen here rather than in the consuming app.
 *
 * Iteration order is the caller's, so output stays deterministic.
 */
export function uniqueModuleAliases(
  names: readonly string[],
  alias: (name: string) => string
): Map<string, string> {
  const taken = new Set<string>(names)
  const aliases = new Map<string, string>()
  for (const name of names) {
    let candidate = alias(name)
    // a candidate equal to its own name is the identity alias and always fine
    while (candidate !== name && taken.has(candidate)) candidate += '_'
    taken.add(candidate)
    aliases.set(name, candidate)
  }
  return aliases
}

// string-based type parser

function splitTopLevelTypeUnion(type: string): string[] {
  const parts: string[] = []
  let depth = 0
  let start = 0
  let quote: string | null = null

  for (let i = 0; i < type.length; i++) {
    const char = type[i]
    if (quote) {
      if (char === '\\') {
        i++
        continue
      }
      if (char === quote) quote = null
      continue
    }
    if (char === '"' || char === "'" || char === '`') {
      quote = char
      continue
    }
    if (char === '<' || char === '(' || char === '{' || char === '[') {
      depth++
      continue
    }
    if (char === '>' || char === ')' || char === '}' || char === ']') {
      depth = Math.max(0, depth - 1)
      continue
    }
    if (char === '|' && depth === 0) {
      parts.push(type.slice(start, i).trim())
      start = i + 1
    }
  }

  parts.push(type.slice(start).trim())
  return parts.filter(Boolean)
}

function splitTopLevelTypeIntersection(type: string): string[] {
  const parts: string[] = []
  let depth = 0
  let start = 0
  let quote: string | null = null

  for (let i = 0; i < type.length; i++) {
    const char = type[i]
    if (quote) {
      if (char === '\\') {
        i++
        continue
      }
      if (char === quote) quote = null
      continue
    }
    if (char === '"' || char === "'" || char === '`') {
      quote = char
      continue
    }
    if (char === '<' || char === '(' || char === '{' || char === '[') {
      depth++
      continue
    }
    if (char === '>' || char === ')' || char === '}' || char === ']') {
      depth = Math.max(0, depth - 1)
      continue
    }
    if (char === '&' && depth === 0 && type[i - 1] !== '&' && type[i + 1] !== '&') {
      parts.push(type.slice(start, i).trim())
      start = i + 1
    }
  }

  parts.push(type.slice(start).trim())
  return parts.filter(Boolean)
}

function stripTypeComments(type: string): string {
  let out = ''
  let quote: string | null = null

  for (let i = 0; i < type.length; i++) {
    const char = type[i]
    const next = type[i + 1]
    if (quote) {
      out += char
      if (char === '\\') {
        if (next) {
          out += next
          i++
        }
        continue
      }
      if (char === quote) quote = null
      continue
    }
    if (char === '"' || char === "'" || char === '`') {
      quote = char
      out += char
      continue
    }
    if (char === '/' && next === '/') {
      while (i < type.length && type[i] !== '\n') i++
      if (i < type.length) out += '\n'
      continue
    }
    if (char === '/' && next === '*') {
      i += 2
      while (i < type.length && !(type[i] === '*' && type[i + 1] === '/')) i++
      i++
      continue
    }
    out += char
  }

  return out
}

function nextNonWhitespace(type: string, start: number): string | undefined {
  for (let i = start; i < type.length; i++) {
    const char = type[i]
    if (char && !/\s/.test(char)) return char
  }
  return undefined
}

function splitTopLevelObjectEntries(type: string): string[] {
  const parts: string[] = []
  let depth = 0
  let start = 0
  let quote: string | null = null

  for (let i = 0; i < type.length; i++) {
    const char = type[i]
    if (quote) {
      if (char === '\\') {
        i++
        continue
      }
      if (char === quote) quote = null
      continue
    }
    if (char === '"' || char === "'" || char === '`') {
      quote = char
      continue
    }
    if (char === '<' || char === '(' || char === '{' || char === '[') {
      depth++
      continue
    }
    if (char === '>' || char === ')' || char === '}' || char === ']') {
      depth = Math.max(0, depth - 1)
      continue
    }
    if (
      depth === 0 &&
      (char === ';' ||
        char === ',' ||
        (char === '\n' &&
          type.slice(start, i).includes(':') &&
          !['|', '&'].includes(nextNonWhitespace(type, i + 1) ?? '')))
    ) {
      parts.push(type.slice(start, i).trim())
      start = i + 1
    }
  }

  parts.push(type.slice(start).trim())
  return parts.filter(Boolean)
}

function parseInlineObjectEntries(type: string): string[] | null | undefined {
  if (!type.startsWith('{') || !type.endsWith('}')) return undefined

  const inner = type.slice(1, -1).trim()
  if (!inner) return []

  const entries: string[] = []
  for (const part of splitTopLevelObjectEntries(inner)) {
    const trimmed = part.trim().replace(/[;,]\s*$/, '')
    if (!trimmed || trimmed.startsWith('[')) continue
    const match = trimmed.match(/^(?:readonly\s+)?(\w+)(\?)?:\s*(.+)$/s)
    if (!match) continue
    const [, name, opt, typeStr] = match
    const parsed = parseTypeString(typeStr!.trim())
    if (!parsed) return null // can't resolve inner type
    let val = parsed
    if (opt) val = `v.optional(${val})`
    entries.push(`${formatObjectKey(name!)}: ${val}`)
  }

  return entries
}

// simple string-based type parser for inline type annotations from source text.
// handles: primitives, inline objects { ... }, arrays, void/null/unknown.
// returns null for type references that cannot be resolved without a type
// checker (bare identifiers, generics, imported aliases).
export function parseTypeString(type: string): string | null {
  type = stripTypeComments(type.trim()).trim()

  const unionParts = splitTopLevelTypeUnion(type)
  if (unionParts.length > 1) {
    const hasNull = unionParts.includes('null')
    const hasUndefined = unionParts.includes('undefined') || unionParts.includes('void')
    const valueParts = unionParts.filter(
      (part) => part !== 'null' && part !== 'undefined' && part !== 'void'
    )
    if (valueParts.length === 0) return hasNull ? 'v.null_()' : 'v.void_()'

    const parsedParts = valueParts.map((part) => parseTypeString(part))
    if (parsedParts.some((part) => !part)) return null
    let val =
      parsedParts.length === 1 ? parsedParts[0]! : `v.union([${parsedParts.join(', ')}])`
    if (hasNull) val = `v.nullable(${val})`
    if (hasUndefined) val = `v.optional(${val})`
    return val
  }

  const intersectionParts = splitTopLevelTypeIntersection(type)
  if (intersectionParts.length > 1) {
    const entries: string[] = []
    for (const part of intersectionParts) {
      const objectEntries = parseInlineObjectEntries(part)
      if (!objectEntries) return null
      entries.push(...objectEntries)
    }
    if (entries.length === 0) return 'v.object({})'
    return `v.object({\n    ${entries.join(',\n    ')},\n  })`
  }

  // primitives
  if (type === 'string') return 'v.string()'
  if (type === 'number') return 'v.number()'
  if (type === 'boolean') return 'v.boolean()'
  if (type === 'void' || type === 'undefined') return 'v.void_()'
  if (type === 'null') return 'v.null_()'
  if (type === 'any' || type === 'unknown') return 'v.unknown()'

  // inline object: { key: type; ... }
  const objectEntries = parseInlineObjectEntries(type)
  if (objectEntries) {
    const entries = objectEntries
    if (entries.length === 0) return 'v.object({})'
    return `v.object({\n    ${entries.join(',\n    ')},\n  })`
  }
  if (objectEntries === null) return null

  // array: T[]
  if (type.endsWith('[]')) {
    const inner = parseTypeString(type.slice(0, -2).trim())
    return inner ? `v.array(${inner})` : null
  }

  // unrecognized (type reference, complex generic, etc.)
  return null
}

// file-content emitters
// these take already-parsed data and return the exact file contents the
// generator should write. they are pure strings-in/strings-out.

export type GeneratedModule = { name: string; importPath: string }

export type GeneratedInstance = {
  name: string
  scope: string | null
  queryNames: string[]
  modelNames: string[]
  tables: string[]
  syncTables: string[]
  supportTables: string[]
}

export function generateModelsFile(modules: GeneratedModule[]): string {
  const sorted = [...modules].sort((a, b) => a.name.localeCompare(b.name))
  const aliases = uniqueModuleAliases(
    sorted.map(({ name }) => name),
    getModelImportName
  )

  const imports = sorted
    .map(({ name, importPath }) => `import * as ${aliases.get(name)!} from '${importPath}'`)
    .join('\n')

  const sortedByImportName = [...sorted].sort((a, b) =>
    aliases.get(a.name)!.localeCompare(aliases.get(b.name)!)
  )
  const modelsObj = `export const models = {\n${sortedByImportName
    .map(({ name }) => {
      const importName = aliases.get(name)!
      return importName === name ? `  ${name},` : `  ${name}: ${importName},`
    })
    .join('\n')}\n}`

  return `// auto-generated by: on-zero generate\n${imports}\n\n${modelsObj}\n`
}

export function generateTypesFile(modelNames: string[]): string {
  const sorted = [...modelNames].sort((a, b) => a.localeCompare(b))
  // must match generateTablesFile: these types index into the schema names it exports
  const aliases = uniqueModuleAliases(sorted, getModelImportName)

  const typeExports = sorted
    .map((name) => {
      const pascalName = name.charAt(0).toUpperCase() + name.slice(1)
      const schemaName = aliases.get(name)!
      return `export type ${pascalName} = TableInsertRow<typeof schema.${schemaName}>\nexport type ${pascalName}Update = TableUpdateRow<typeof schema.${schemaName}>`
    })
    .join('\n\n')

  return `import type { TableInsertRow, TableUpdateRow } from 'on-zero'\nimport type * as schema from './tables'\n\n${typeExports}\n`
}

export function generateTablesFile(modules: GeneratedModule[]): string {
  const sorted = [...modules].sort((a, b) => a.name.localeCompare(b.name))
  const aliases = uniqueModuleAliases(
    sorted.map(({ name }) => name),
    getModelImportName
  )

  const exports = sorted
    .map(
      ({ name, importPath }) =>
        `export { schema as ${aliases.get(name)!} } from '${importPath}'`
    )
    .join('\n')

  return `// auto-generated by: on-zero generate\n\n${exports}\n`
}

export function generateReadmeFile(): string {
  return `# generated

this folder is auto-generated by on-zero. do not edit files here directly.

## what's generated

- \`models.ts\` - exports every namespace's mutation module
- \`types.ts\` - typescript types derived from table schemas
- \`tables.ts\` - exports table schemas for type inference
- \`groupedQueries.ts\` - namespaced query re-exports for client setup
- \`syncedQueries.ts\` - namespaced syncedQuery wrappers for server setup
- \`syncedMutations.ts\` - valibot validators for mutation args (server auto-validation)
- \`instances.ts\` - generated client partitions, sync tables, and server-only support tables

## usage guidelines

**do not import generated files outside of the data folder.**

### queries

write small namespaces in one file, or split a large namespace into
\`queries.ts\` and \`mutations.ts\` inside a folder:

\`\`\`ts
// ✅ good - import from queries
import { channelMessages } from '~/data/message/queries'
\`\`\`

the generated query files are only used internally by zero client/server setup.

### types

you can import types from this folder, but prefer re-exporting from your app's public types barrel:

\`\`\`ts
// ❌ okay but not preferred
import type { Message } from '~/data/generated/types'

// ✅ better - re-export from the app types barrel
import type { Message } from '~/types'
\`\`\`

## regeneration

files are regenerated when you run:

\`\`\`bash
bun on-zero generate
\`\`\`

or in watch mode:

\`\`\`bash
bun on-zero generate --watch
\`\`\`

## more info

see the [on-zero readme](./node_modules/on-zero/README.md) for full documentation.
`
}

export function generateGroupedQueriesFile(
  queries: Array<{ name: string; sourceFile: string; importPath: string }>
): string {
  const queryByFile = new Map<string, typeof queries>()
  for (const query of queries) {
    if (!queryByFile.has(query.sourceFile)) queryByFile.set(query.sourceFile, [])
    queryByFile.get(query.sourceFile)!.push(query)
  }

  const sortedFiles = [...queryByFile.keys()].sort()
  const aliases = uniqueModuleAliases(sortedFiles, (file) => `${file}Source`)
  const imports = sortedFiles
    .map(
      (file) =>
        `import * as ${aliases.get(file)!} from '${queryByFile.get(file)![0]!.importPath}'`
    )
    .join('\n')
  const exports = sortedFiles
    .map((file) => {
      const alias = aliases.get(file)!
      const names = queryByFile
        .get(file)!
        .map((query) => query.name)
        .sort()
      return `export const ${file} = {\n${names.map((name) => `  ${name}: ${alias}.${name},`).join('\n')}\n}`
    })
    .join('\n\n')

  return `/**
 * auto-generated by: on-zero generate
 *
 * grouped query re-exports for minification-safe query identity.
 * this file re-exports all query modules - while this breaks tree-shaking,
 * queries are typically small and few in number even in larger apps.
 */
${imports}${imports ? '\n\n' : ''}${exports}
`
}

export function generateInstancesFile(instances: GeneratedInstance[]): string {
  const entries = [...instances]
    .sort((a, b) =>
      a.name === 'default' ? -1 : b.name === 'default' ? 1 : a.name.localeCompare(b.name)
    )
    .map((instance) => {
      const queries = instance.queryNames
        .sort()
        .map((name) => `      ${formatObjectKey(name)}: groupedQueries.${name},`)
        .join('\n')
      const modelEntries = instance.modelNames
        .sort()
        .map((name) => `      ${formatObjectKey(name)}: models.${name},`)
        .join('\n')
      const scope = instance.scope ? JSON.stringify(instance.scope) : 'null'
      const visibility = instance.scope
        ? `(value: string) => ({ column: ${JSON.stringify(instance.scope)}, value })`
        : 'null'
      return `  ${formatObjectKey(instance.name)}: {
    schema,
    queries: {
${queries}
    },
    models: {
${modelEntries}
    },
    tables: ${JSON.stringify([...instance.tables].sort())},
    syncTables: ${JSON.stringify([...instance.syncTables].sort())},
    supportTables: ${JSON.stringify([...instance.supportTables].sort())},
    scope: ${scope},
    defaultVisibility: ${visibility},
  },`
    })
    .join('\n')

  return `// auto-generated by: on-zero generate
import { schema } from './schema'
import * as groupedQueries from './groupedQueries'
import { models } from './models'

export const instances = {
${entries}
} as const
`
}

export function generateSyncedQueriesFile(
  queries: Array<{
    name: string
    params: string
    valibotCode: string
    sourceFile: string
  }>
): string {
  const queryByFile = new Map<string, typeof queries>()
  for (const q of queries) {
    if (!queryByFile.has(q.sourceFile)) {
      queryByFile.set(q.sourceFile, [])
    }
    queryByFile.get(q.sourceFile)!.push(q)
  }

  const sortedFiles = Array.from(queryByFile.keys()).sort()

  const imports = `// auto-generated by: on-zero generate
// server-side query definitions with validators
import { defineQuery, defineQueries } from 'on-zero'
import * as v from 'valibot'
import * as Queries from './groupedQueries'
`

  const namespaceDefs = sortedFiles
    .map((file) => {
      const fileQueries = queryByFile
        .get(file)!
        .sort((a, b) => a.name.localeCompare(b.name))

      const queryDefs = fileQueries
        .map((q) => {
          const validatorDef = q.valibotCode.trim()

          if (q.params === 'void' || !validatorDef) {
            return `  ${q.name}: defineQuery(() => Queries.${file}.${q.name}()),`
          }

          const indentedValidator = validatorDef
            .split('\n')
            .map((line, i) => (i === 0 ? line : `    ${line}`))
            .join('\n')

          return `  ${q.name}: defineQuery(
    ${indentedValidator},
    ({ args }) => Queries.${file}.${q.name}(args)
  ),`
        })
        .join('\n')

      return `const ${file} = {\n${queryDefs}\n}`
    })
    .join('\n\n')

  const queriesObject = sortedFiles.map((file) => `  ${file},`).join('\n')

  return `${imports}
${namespaceDefs}

export const queries = defineQueries({
${queriesObject}
})
`
}

// column → valibot conversion helpers

export function columnTypeToValibot(col: SchemaColumn): string {
  let base = 'v.string()'
  switch (col.type) {
    case 'string':
      base = 'v.string()'
      break
    case 'number':
      base = 'v.number()'
      break
    case 'boolean':
      base = 'v.boolean()'
      break
    case 'json':
      base = 'v.unknown()'
      break
    case 'enum':
      base = 'v.string()'
      break
  }
  return col.optional ? `v.optional(v.nullable(${base}))` : base
}

export function schemaColumnsToValibot(
  columns: Record<string, SchemaColumn>,
  primaryKeys: string[],
  mode: 'insert' | 'update' | 'delete'
): string {
  const entries: string[] = []

  if (mode === 'delete') {
    // only pks
    for (const pk of primaryKeys) {
      const col = columns[pk]
      if (col)
        entries.push(
          `${formatObjectKey(pk)}: ${columnTypeToValibot({ ...col, optional: false })}`
        )
    }
  } else if (mode === 'update') {
    // pks required, rest optional
    for (const [name, col] of Object.entries(columns)) {
      const isPK = primaryKeys.includes(name)
      if (isPK) {
        entries.push(
          `${formatObjectKey(name)}: ${columnTypeToValibot({ ...col, optional: false })}`
        )
      } else {
        entries.push(
          `${formatObjectKey(name)}: ${columnTypeToValibot({ ...col, optional: true })}`
        )
      }
    }
  } else {
    // insert: all columns as-is
    for (const [name, col] of Object.entries(columns)) {
      entries.push(`${formatObjectKey(name)}: ${columnTypeToValibot(col)}`)
    }
  }

  return `v.object({\n    ${entries.join(',\n    ')},\n  })`
}

// returns as-is since output is already a valibot expression
export function extractValibotExpression(valibotCode: string): string {
  return valibotCode.trim() || 'v.unknown()'
}

// parse a column builder source-text fragment (e.g. "string().optional()") into
// a SchemaColumn. used by generate-lite.ts when the caller extracts raw
// column builder text from their AST.
export function parseColumnType(initText: string): SchemaColumn {
  const optional = initText.includes('.optional()')
  let type: SchemaColumn['type'] = 'string'

  if (initText.startsWith('number(')) type = 'number'
  else if (initText.startsWith('boolean(')) type = 'boolean'
  else if (initText.startsWith('json(') || initText.startsWith('json<')) type = 'json'
  else if (initText.startsWith('enumeration(')) type = 'enum'

  return { type, optional, customType: undefined }
}

export function generateSyncedMutationsFile(modelMutations: ModelMutations[]): string {
  const sorted = [...modelMutations].sort((a, b) =>
    a.modelName.localeCompare(b.modelName)
  )

  const modelDefs = sorted
    .map((model) => {
      const entries: string[] = []

      // crud validators from schema
      if (model.hasCRUD && Object.keys(model.columns).length > 0) {
        for (const mode of ['insert', 'update', 'delete'] as const) {
          // skip if custom mutation overrides this crud op
          const hasCustomOverride = model.custom.some((m) => m.name === mode)
          if (hasCustomOverride) {
            // use the custom version's validator instead
            const customMut = model.custom.find((m) => m.name === mode)!
            if (customMut.valibotCode) {
              entries.push(
                `    ${mode}: ${extractValibotExpression(customMut.valibotCode)},`
              )
            } else {
              // fall back to schema-derived
              entries.push(
                `    ${mode}: ${schemaColumnsToValibot(model.columns, model.primaryKeys, mode)},`
              )
            }
          } else {
            entries.push(
              `    ${mode}: ${schemaColumnsToValibot(model.columns, model.primaryKeys, mode)},`
            )
          }
        }
      }

      // custom mutations (excluding crud overrides already handled)
      for (const mut of model.custom) {
        if (model.hasCRUD && ['insert', 'update', 'delete', 'upsert'].includes(mut.name))
          continue
        if (mut.paramType === 'void') {
          entries.push(`    ${mut.name}: v.void_(),`)
          continue
        }
        if (!mut.valibotCode) {
          entries.push(`    ${mut.name}: v.unknown(),`)
          continue
        }
        entries.push(`    ${mut.name}: ${extractValibotExpression(mut.valibotCode)},`)
      }

      return `  ${model.modelName}: {\n${entries.join('\n')}\n  },`
    })
    .join('\n')

  return `// auto-generated by: on-zero generate
// mutation validators derived from model schemas and handler types
import * as v from 'valibot'

export const mutationValidators = {
${modelDefs}
}
`
}
