// browser-safe entry point for the on-zero generator.
//
// unlike `./generate.ts`, this module:
//   - does not import `typescript`, `node:fs`, `node:path`, or any node builtin
//   - does not read or write files (returns a map of filename → content)
//   - is parser-agnostic: callers supply a `parse` function that extracts the
//     small amount of ast info on-zero needs (a "lite" ast). callers typically
//     back that parse function with acorn + acorn-typescript, oxc, swc, or any
//     other ts-aware parser they already bundle.
//   - falls back to `v.unknown()` for any parameter whose type annotation text
//     can't be parsed by on-zero's existing string-based `parseTypeString`
//     helper. there is no cross-file type resolution.
//
// this makes the generator runnable inside web workers and other browser
// contexts without pulling in ~10mb of typescript.

import {
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
} from './generate-helpers'

import type { ModelMutations, SchemaColumn } from './generate-helpers'

// public types

// minimal ast info about a namespace mutation module (e.g. `post/mutations.ts`)
export type LiteMutationExport = {
  // the first arg to `mutations('NAME', ...)` — string literal from the source.
  // not currently emitted anywhere in the output; the model file basename is
  // used for the top-level key in syncedMutations instead. kept for parity with
  // what callers naturally extract, and to leave room for future use.
  modelName: string
  // handlers = keys of the last object literal arg to `mutations(...)`
  handlers: Array<{
    // handler property name, e.g. 'toggleActive'
    name: string
    // text of the second parameter's type annotation, if present.
    // e.g. '{ id: string; isActive: boolean }' or 'ToggleArgs' or null.
    // for inline type literals parseable by `parseTypeString`, on-zero emits
    // real v.object(...) validators. for references/generics/null, it falls
    // back to `v.unknown()`.
    paramTypeText: string | null
  }>
  // for models that declare `export const schema = table(...).columns(...)`,
  // the caller extracts the table name + column builder text. null if the
  // model doesn't declare a schema inline (most templates use drizzle-zero,
  // so null is common).
  schema: LiteSchemaInfo | null
}

export type LiteSchemaInfo = {
  tableName: string
  primaryKeys: string[]
  // per-column: name + the column builder chain as source text, e.g.
  // `"string().optional()"`. `parseColumnType` turns these into SchemaColumn.
  columns: Array<{ name: string; builderText: string }>
}

export type LiteQueryExport = {
  // exported variable name, e.g. 'flightById'
  name: string
  // text of the first parameter's type annotation, if present.
  // e.g. 'string' | '{ id: string }' | null. null means the query takes no
  // args and is treated as a void query in the generated output.
  paramTypeText: string | null
  // relation-name paths starting at this namespace's table. nested related()
  // calls are one path, e.g. [['comments', 'author']].
  relatedPaths?: string[][]
}

export type LiteRelationInfo = {
  sourceTable: string
  name: string
  targetTable: string
}

export type LiteTableInfo = {
  name: string
  columns: string[]
}

export type LiteDataConfig = {
  instances: Record<
    string,
    {
      dir?: string
      scope?: string
      supportTables?: string[]
    }
  >
}

// what the caller returns for each source file.
export type LiteParsedFile = {
  // a model file exports at most one `mutate = mutations(...)`, but an array
  // keeps the shape uniform and leaves room for future multi-export support.
  mutations: LiteMutationExport[]
  queries: LiteQueryExport[]
  relations?: LiteRelationInfo[]
  tables?: LiteTableInfo[]
  // static table names reached through tx.mutate.<table> or tx.query.<table>.
  // element access such as tx.mutate[name] is intentionally omitted.
  supportTables?: string[]
  // static import/export module specifiers used to follow mutation helpers.
  imports?: string[]
  // present only for an on-zero.config.ts default defineConfig export.
  dataConfig?: LiteDataConfig
  // parser syntax failure. the membership pass warns and ignores this file.
  parseError?: string
}

// the parser function signature the caller provides. pure: given source text
// and a path (for error messages), return the lite ast.
export type LiteParseFn = (sourceCode: string, filePath: string) => LiteParsedFile

export type LiteGenerateOptions = {
  // file path → source content. pass whatever path shape you have (absolute,
  // virtual, etc.), as long as you're consistent with `dir`.
  files: Record<string, string>
  // base data directory, e.g. '/proj/src/data'. namespaces are direct .ts
  // files or folders with queries.ts / mutations.ts.
  dir: string
  // explicit on-zero.config.ts path. auto-discovered in `dir` when omitted.
  config?: string
  parse: LiteParseFn
}

export type LiteGenerateResult = {
  // relative paths under `{dir}/generated/`, e.g. 'models.ts',
  // 'syncedMutations.ts'. callers decide where to write.
  files: Record<string, string>
  modelCount: number
  queryCount: number
  mutationCount: number
  schemaCount: number
}

// path helpers — deliberately string-only, no node:path

function stripTrailingSlash(s: string): string {
  return s.endsWith('/') ? s.slice(0, -1) : s
}

// returns the last '/' segment, stripped of a trailing `.ts` if present.
// does not depend on node:path.basename.
function baseName(path: string, ext?: string): string {
  const idx = path.lastIndexOf('/')
  let base = idx >= 0 ? path.slice(idx + 1) : path
  if (ext && base.endsWith(ext)) base = base.slice(0, -ext.length)
  return base
}

function resolvePath(path: string): string {
  const absolute = path.startsWith('/')
  const parts: string[] = []
  for (const part of path.split('/')) {
    if (!part || part === '.') continue
    if (part === '..') parts.pop()
    else parts.push(part)
  }
  return `${absolute ? '/' : ''}${parts.join('/')}`
}

function parentDir(path: string): string {
  return path.slice(0, path.lastIndexOf('/'))
}

function relativePath(from: string, to: string): string {
  const fromParts = resolvePath(from).split('/').filter(Boolean)
  const toParts = resolvePath(to).split('/').filter(Boolean)
  let common = 0
  while (fromParts[common] === toParts[common] && common < fromParts.length) common++
  return [
    ...Array.from({ length: fromParts.length - common }, () => '..'),
    ...toParts.slice(common),
  ].join('/')
}

const isWithin = (root: string, path: string) =>
  path === root || path.startsWith(`${root}/`)

// returns files whose path is an immediate child of `dirPrefix` and ends in
// `.ts` (but not `.d.ts`, test files, or anything nested further down).
function listDirectTsFiles(files: Record<string, string>, dirPrefix: string): string[] {
  const prefix = stripTrailingSlash(dirPrefix) + '/'
  const out: string[] = []
  for (const path of Object.keys(files)) {
    if (!path.startsWith(prefix)) continue
    const rest = path.slice(prefix.length)
    // must be a direct child (no further slashes)
    if (rest.includes('/')) continue
    if (!rest.endsWith('.ts')) continue
    if (rest.endsWith('.d.ts')) continue
    if (rest.endsWith('.test.ts') || rest.endsWith('.spec.ts')) continue
    out.push(path)
  }
  return out.sort()
}

type LiteNamespace = {
  name: string
  instance: string
  queryPath: string | null
  modelPath: string | null
}

type LiteInstance = {
  name: string
  dir: string
  scope: string | null
  declaredSupportTables: string[]
  namespaces: LiteNamespace[]
}

function discoverLiteLayout(
  files: Record<string, string>,
  baseDir: string,
  parse: LiteParseFn,
  explicitConfigPath?: string
) {
  const paths = Object.keys(files)
  const configPath = resolvePath(explicitConfigPath ?? `${baseDir}/on-zero.config.ts`)
  const configSource = files[configPath]
  let config: LiteDataConfig | undefined
  if (configSource !== undefined) {
    const parsed = parse(configSource, configPath)
    if (parsed.parseError) {
      throw new Error(`[on-zero] unable to parse ${configPath}: ${parsed.parseError}`)
    }
    config = parsed.dataConfig
    if (!config) {
      throw new Error(
        `[on-zero] ${configPath} must default export defineConfig({ instances: { ... } })`
      )
    }
  } else if (explicitConfigPath) {
    throw new Error(`[on-zero] config file does not exist: ${configPath}`)
  }
  if (explicitConfigPath && parentDir(configPath) !== baseDir) {
    throw new Error(`[on-zero] ${configPath} must be at the data root ${baseDir}`)
  }

  const instances: LiteInstance[] = config
    ? Object.entries(config.instances).map(([name, options]) => {
        if (options.dir?.startsWith('/')) {
          throw new Error(
            `[on-zero] instance '${name}' dir must be relative to ${configPath}`
          )
        }
        if (options.scope === '') {
          throw new Error(`[on-zero] instance '${name}' scope cannot be empty`)
        }
        if (options.supportTables?.some((table) => table === '')) {
          throw new Error(
            `[on-zero] instance '${name}' supportTables cannot contain an empty table name`
          )
        }
        return {
          name,
          dir: resolvePath(`${parentDir(configPath)}/${options.dir ?? name}`),
          scope: options.scope ?? null,
          declaredSupportTables: [...(options.supportTables ?? [])],
          namespaces: [],
        }
      })
    : [
        {
          name: 'default',
          dir: baseDir,
          scope: null,
          declaredSupportTables: [],
          namespaces: [],
        },
      ]
  if (config && instances.length === 0) {
    throw new Error(`[on-zero] ${configPath} must declare at least one instance`)
  }
  for (const instance of instances) {
    if (!paths.some((path) => isWithin(instance.dir, path))) {
      throw new Error(
        `[on-zero] instance '${instance.name}' directory does not exist: ${instance.dir}`
      )
    }
    const duplicate = instances.find(
      (candidate) => candidate !== instance && candidate.dir === instance.dir
    )
    if (duplicate) {
      throw new Error(
        `[on-zero] instances '${instance.name}' and '${duplicate.name}' resolve to the same directory: ${instance.dir}`
      )
    }
  }
  const instanceDirs = new Set(instances.map((instance) => instance.dir))
  const sourceRoots = [baseDir, ...instances.map((instance) => instance.dir)]
  const remnant = paths.find(
    (path) =>
      baseName(path) === 'instance.ts' && sourceRoots.some((root) => isWithin(root, path))
  )
  if (remnant) {
    throw new Error(
      `[on-zero] ${remnant} uses removed instance.ts configuration; delete it and configure instances in on-zero.config.ts`
    )
  }

  const parseDataFile = (path: string): LiteParsedFile | null => {
    let parsed: LiteParsedFile
    try {
      parsed = parse(files[path]!, path)
    } catch {
      console.warn(
        `[on-zero] ignoring ${path.slice(baseDir.lastIndexOf('/') + 1)}: no recognized data exports`
      )
      return null
    }
    if (parsed.parseError) {
      console.warn(
        `[on-zero] ignoring ${path.slice(baseDir.lastIndexOf('/') + 1)}: no recognized data exports`
      )
      return null
    }
    return parsed
  }
  const hasDataExport = (source: string, parsed: LiteParsedFile) =>
    parsed.queries.length > 0 ||
    parsed.mutations.length > 0 ||
    (parsed.tables?.length ?? 0) > 0 ||
    /export\s+const\s+(?:mutate|schema|where)\s*=\s*(?:mutations|serverWhere|table)\s*\(/.test(
      source
    )

  if (config) {
    for (const path of paths) {
      if (
        path === configPath ||
        !isWithin(baseDir, path) ||
        instances.some((instance) => isWithin(instance.dir, path)) ||
        isWithin(`${baseDir}/generated`, path) ||
        !path.endsWith('.ts') ||
        path.endsWith('.d.ts') ||
        path.endsWith('.test.ts') ||
        path.endsWith('.spec.ts') ||
        baseName(path) === 'instance.ts'
      ) {
        continue
      }
      const parsed = parseDataFile(path)
      if (parsed && hasDataExport(files[path]!, parsed)) {
        throw new Error(
          `[on-zero] data namespace ${path} is outside every instance directory declared in ${configPath}`
        )
      }
    }
  }

  for (const instance of instances) {
    const directFiles = listDirectTsFiles(files, instance.dir).filter(
      (path) => baseName(path) !== 'on-zero.config.ts'
    )
    for (const path of directFiles) {
      const source = files[path]!
      const parsed = parseDataFile(path)
      if (!parsed || !hasDataExport(source, parsed)) continue
      instance.namespaces.push({
        name: baseName(path, '.ts'),
        instance: instance.name,
        queryPath: path,
        modelPath: path,
      })
    }

    const prefix = `${instance.dir}/`
    const folders = new Set<string>()
    for (const path of paths) {
      if (!path.startsWith(prefix)) continue
      const rest = path.slice(prefix.length)
      const slash = rest.indexOf('/')
      if (slash > 0) folders.add(`${instance.dir}/${rest.slice(0, slash)}`)
    }
    for (const folder of [...folders].sort()) {
      if (folder === `${baseDir}/generated` || instanceDirs.has(folder)) continue
      const queries = `${folder}/queries.ts`
      const mutations = `${folder}/mutations.ts`
      if (!(queries in files) && !(mutations in files)) {
        const oldName = baseName(folder)
        if (
          ['models', 'mutations', 'queries'].includes(oldName) &&
          listDirectTsFiles(files, folder).length > 0
        ) {
          throw new Error(
            `[on-zero] ${folder} uses the removed top-level ${oldName}/ layout; ` +
              `move each namespace to <name>.ts or <name>/queries.ts + mutations.ts`
          )
        }
        continue
      }
      instance.namespaces.push({
        name: baseName(folder),
        instance: instance.name,
        queryPath: queries in files ? queries : null,
        modelPath: mutations in files ? mutations : null,
      })
    }
  }

  const owners = new Map<string, string>()
  for (const namespace of instances.flatMap((instance) => instance.namespaces)) {
    const owner = owners.get(namespace.name)
    if (owner) {
      throw new Error(
        `[on-zero] namespace '${namespace.name}' is claimed by instances '${owner}' and '${namespace.instance}'`
      )
    }
    owners.set(namespace.name, namespace.instance)
  }
  return instances
}

// main entry point

export function generateLite(opts: LiteGenerateOptions): LiteGenerateResult {
  const { files, parse } = opts
  const baseDir = resolvePath(stripTrailingSlash(opts.dir))
  const instances = discoverLiteLayout(files, baseDir, parse, opts.config)
  const sourceRoots = [baseDir, ...instances.map((instance) => instance.dir)]
  const namespaces = instances.flatMap((instance) => instance.namespaces)
  const modelNamespaces = namespaces.filter(
    (namespace): namespace is LiteNamespace & { modelPath: string } =>
      namespace.modelPath !== null
  )
  const relations = new Map<string, Map<string, string>>()
  const tableColumns = new Map<string, Set<string>>()
  for (const path of Object.keys(files).filter(
    (path) =>
      path.endsWith('/relations.ts') ||
      /\/database\/(?:schema[^/]*|zeroSchemaInput)\.ts$/.test(path)
  )) {
    const parsed = parse(files[path]!, path)
    for (const relation of parsed.relations ?? []) {
      const tableRelations = relations.get(relation.sourceTable) ?? new Map()
      tableRelations.set(relation.name, relation.targetTable)
      relations.set(relation.sourceTable, tableRelations)
    }
    for (const table of parsed.tables ?? []) {
      tableColumns.set(table.name, new Set(table.columns))
    }
  }

  // parse each model file and build ModelMutations records for the emitter
  const allModelMutations: ModelMutations[] = []
  const modelNamesWithSchema: string[] = []

  for (const namespace of modelNamespaces) {
    const filePath = namespace.modelPath
    const modelName = namespace.name
    const content = files[filePath]!
    const parsed = parse(content, filePath)

    // a model file has at most one mutate export, but the lite ast is an array
    const mutationExport = parsed.mutations[0] ?? null

    // extract schema info if present
    const columns: Record<string, SchemaColumn> = {}
    const primaryKeys: string[] = []
    let hasSchema = false

    if (mutationExport?.schema) {
      hasSchema = true
      modelNamesWithSchema.push(modelName)
      for (const pk of mutationExport.schema.primaryKeys) primaryKeys.push(pk)
      for (const col of mutationExport.schema.columns) {
        columns[col.name] = parseColumnType(col.builderText)
      }
      const names = new Set(mutationExport.schema.columns.map((column) => column.name))
      tableColumns.set(modelName, names)
      tableColumns.set(mutationExport.schema.tableName, names)
    }

    // a model participates in crud only when it has a schema AND its mutate
    // call is `mutations(schema, perm)` / `mutations(schema, perm, { ... })`.
    // in the lite ast, we don't know the call arity directly, so we use the
    // presence of a schema as the signal. this matches the real generator's
    // behavior for schemas-with-mutate: hasCRUD is true whenever both exist.
    //
    // models without `export const mutate` still appear in the output as
    // empty entries — see the test "treats models without export const mutate
    // as empty mutations".
    const hasCRUD = hasSchema && mutationExport !== null

    const custom = (mutationExport?.handlers ?? []).map((h) => {
      // null or empty annotation → void (no second param) → v.void_()
      if (h.paramTypeText == null) {
        return { name: h.name, paramType: 'void', valibotCode: '' }
      }

      const paramType = h.paramTypeText.trim()

      // unknown explicitly → permissive validator
      if (paramType === 'unknown') {
        return { name: h.name, paramType: 'unknown', valibotCode: '' }
      }

      // try the pure string parser. if it fails, fall back to v.unknown()
      // (no cross-file type resolution in lite mode).
      let valibotCode: string | null = null
      try {
        valibotCode = parseTypeString(paramType)
      } catch {
        valibotCode = null
      }

      return {
        name: h.name,
        paramType,
        valibotCode: valibotCode ?? 'v.unknown()',
      }
    })

    allModelMutations.push({
      modelName,
      hasCRUD,
      columns,
      primaryKeys,
      custom,
    })
  }

  // parse each query file
  const allQueries: Array<{
    name: string
    params: string
    valibotCode: string
    sourceFile: string
    importPath: string
  }> = []
  const relatedPaths = new Map<string, Array<{ query: string; paths: string[][] }>>()

  for (const namespace of namespaces.filter(
    (namespace): namespace is LiteNamespace & { queryPath: string } =>
      namespace.queryPath !== null
  )) {
    const filePath = namespace.queryPath
    const fileBaseName = namespace.name
    const content = files[filePath]!
    const parsed = parse(content, filePath)
    if (
      content.includes('.related(') &&
      !parsed.queries.some((query) => query.relatedPaths)
    ) {
      throw new Error(
        `[on-zero] ${filePath} uses related(), but the lite parser did not return relatedPaths`
      )
    }

    for (const q of parsed.queries) {
      if (['mutate', 'permission', 'schema', 'where'].includes(q.name)) continue
      if (q.relatedPaths?.length) {
        const entries = relatedPaths.get(namespace.name) ?? []
        entries.push({ query: q.name, paths: q.relatedPaths })
        relatedPaths.set(namespace.name, entries)
      }

      // null annotation → no first arg → void query
      if (q.paramTypeText == null) {
        allQueries.push({
          name: q.name,
          params: 'void',
          valibotCode: '',
          sourceFile: fileBaseName,
          importPath: `../${relativePath(baseDir, filePath).replace(/\.ts$/, '')}`,
        })
        continue
      }

      const paramType = q.paramTypeText.trim()

      // try to parse the annotation. if we can't, fall back to v.unknown()
      // so the query still makes it into the output (consistent with
      // mutations). the existing node generator silently drops queries
      // whose types can't be resolved; in lite mode we emit them with a
      // permissive validator rather than losing them.
      let valibotCode: string | null = null
      try {
        valibotCode = parseTypeString(paramType)
      } catch {
        valibotCode = null
      }

      allQueries.push({
        name: q.name,
        params: paramType,
        valibotCode: valibotCode ?? 'v.unknown()',
        sourceFile: fileBaseName,
        importPath: `../${relativePath(baseDir, filePath).replace(/\.ts$/, '')}`,
      })
    }
  }

  const owners = new Map(
    namespaces.map((namespace) => [namespace.name, namespace.instance])
  )
  const relatedOwners = new Map<string, string>()
  const syncTables = new Map<string, string[]>()
  for (const instance of instances) {
    const synced = new Set(instance.namespaces.map((namespace) => namespace.name))
    for (const namespace of instance.namespaces) {
      for (const query of relatedPaths.get(namespace.name) ?? []) {
        for (const path of query.paths) {
          let sourceTable = namespace.name
          for (const relationName of path) {
            const target = relations.get(sourceTable)?.get(relationName)
            if (!target) {
              throw new Error(
                `[on-zero] ${namespace.name}.${query.query} related('${relationName}') cannot be resolved ` +
                  `from table '${sourceTable}' through relations.ts`
              )
            }
            const owner = owners.get(target) ?? relatedOwners.get(target)
            if (owner && owner !== instance.name) {
              throw new Error(
                `[on-zero] ${namespace.name}.${query.query} in instance '${instance.name}' reaches ` +
                  `table '${target}' owned by instance '${owner}'`
              )
            }
            relatedOwners.set(target, instance.name)
            synced.add(target)
            sourceTable = target
          }
        }
      }
    }
    const tables = [...synced].sort()
    if (instance.scope) {
      for (const table of tables) {
        if (!tableColumns.get(table)?.has(instance.scope)) {
          throw new Error(
            `[on-zero] table '${table}' in instance '${instance.name}' is missing scope column '${instance.scope}'`
          )
        }
      }
    }
    syncTables.set(instance.name, tables)
  }

  const supportTables = new Map<string, string[]>()
  for (const instance of instances) {
    const supported = new Set<string>(instance.declaredSupportTables)
    for (const namespace of instance.namespaces) {
      if (!namespace.modelPath) continue
      const visited = new Set<string>()
      const scan = (path: string) => {
        if (visited.has(path)) return
        visited.add(path)
        const parsed = parse(files[path]!, path)
        if (parsed.parseError) {
          throw new Error(
            `[on-zero] unable to derive mutation dependencies from ${path}: ${parsed.parseError}`
          )
        }
        for (const table of parsed.supportTables ?? []) {
          if (owners.has(table) || relatedOwners.has(table)) continue
          if (!syncTables.get(instance.name)!.includes(table)) {
            supported.add(table)
          }
        }
        for (const specifier of parsed.imports ?? []) {
          const unresolved = specifier.startsWith('~/data/')
            ? `${baseDir}/${specifier.slice('~/data/'.length)}`
            : specifier.startsWith('.')
              ? `${parentDir(path)}/${specifier}`
              : null
          if (!unresolved) continue
          const normalized = resolvePath(unresolved)
          const dependency = [
            normalized.endsWith('.ts') ? normalized : `${normalized}.ts`,
            `${normalized}/index.ts`,
          ].find(
            (candidate) =>
              candidate in files && sourceRoots.some((root) => isWithin(root, candidate))
          )
          if (dependency) scan(dependency)
        }
      }
      scan(namespace.modelPath)
    }
    supportTables.set(instance.name, [...supported].sort())
  }

  // emit files
  const modelNames = modelNamespaces.map((namespace) => namespace.name)
  const out: Record<string, string> = {}

  out['models.ts'] = generateModelsFile(
    modelNamespaces.map((namespace) => ({
      name: namespace.name,
      importPath: `../${relativePath(baseDir, namespace.modelPath).replace(/\.ts$/, '')}`,
    }))
  )

  if (modelNamesWithSchema.length > 0) {
    out['types.ts'] = generateTypesFile(modelNamesWithSchema)
    out['tables.ts'] = generateTablesFile(
      modelNamespaces
        .filter((namespace) => modelNamesWithSchema.includes(namespace.name))
        .map((namespace) => ({
          name: namespace.name,
          importPath: `../${relativePath(baseDir, namespace.modelPath).replace(/\.ts$/, '')}`,
        }))
    )
  }

  out['README.md'] = generateReadmeFile()

  out['groupedQueries.ts'] = generateGroupedQueriesFile(allQueries)
  out['syncedQueries.ts'] = generateSyncedQueriesFile(allQueries)
  out['instances.ts'] = generateInstancesFile(
    instances.map((instance) => ({
      name: instance.name,
      scope: instance.scope,
      queryNames: instance.namespaces
        .map((namespace) => namespace.name)
        .filter((name) => allQueries.some((query) => query.sourceFile === name)),
      modelNames: instance.namespaces
        .filter((namespace) => namespace.modelPath)
        .map((namespace) => namespace.name),
      tables: instance.namespaces.map((namespace) => namespace.name),
      syncTables: syncTables.get(instance.name)!,
      supportTables: supportTables.get(instance.name)!,
    }))
  )

  if (allModelMutations.length > 0) {
    out['syncedMutations.ts'] = generateSyncedMutationsFile(allModelMutations)
  }

  // count mutations the same way `generate()` does: 3 per crud model plus
  // non-crud custom mutations (the crud-ops set is excluded when hasCRUD).
  let mutationCount = 0
  for (const m of allModelMutations) {
    if (m.hasCRUD) mutationCount += 3
    mutationCount += m.custom.filter(
      (mut) => !m.hasCRUD || !['insert', 'update', 'delete', 'upsert'].includes(mut.name)
    ).length
  }

  return {
    files: out,
    modelCount: modelNames.length,
    queryCount: allQueries.length,
    mutationCount,
    schemaCount: modelNamesWithSchema.length,
  }
}
