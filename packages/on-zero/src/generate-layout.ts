import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs'
import { basename, dirname, relative, resolve, sep } from 'node:path'

import type ts from 'typescript'

export type DataNamespace = {
  name: string
  instance: string
  queryPath: string | null
  modelPath: string | null
  sourcePaths: string[]
}

export type DataInstance = {
  name: string
  dir: string
  scope: string | null
  namespaces: DataNamespace[]
  syncTables: string[]
  supportTables: string[]
  /** `supportTables` declared in `on-zero.config.ts`. */
  declaredSupportTables: string[]
}

export type DataLayout = {
  instances: DataInstance[]
  namespaces: DataNamespace[]
  metadataPaths: string[]
  sourceRoots: string[]
}

const isSourceFile = (name: string) =>
  name.endsWith('.ts') &&
  !name.endsWith('.d.ts') &&
  !name.endsWith('.test.ts') &&
  !name.endsWith('.spec.ts')

const toImportPath = (baseDir: string, path: string) =>
  relative(baseDir, path).split(sep).join('/').replace(/\.ts$/, '')

const isWithin = (root: string, path: string) => {
  const child = relative(root, path)
  return child === '' || (child !== '..' && !child.startsWith(`..${sep}`))
}

type ParsedInstanceConfig = {
  name: string
  dir: string
  scope: string | null
  supportTables: string[]
}

function propertyName(
  ts: typeof import('typescript'),
  name: ts.PropertyName
): string | null {
  if (ts.isIdentifier(name) || ts.isStringLiteral(name) || ts.isNumericLiteral(name)) {
    return name.text
  }
  return null
}

function readDataConfig(
  ts: typeof import('typescript'),
  baseDir: string,
  configPath: string | undefined
): { path: string; instances: ParsedInstanceConfig[] } | null {
  const path = configPath ? resolve(configPath) : resolve(baseDir, 'on-zero.config.ts')
  if (!existsSync(path)) {
    if (configPath) throw new Error(`[on-zero] config file does not exist: ${path}`)
    return null
  }
  if (dirname(path) !== baseDir) {
    throw new Error(`[on-zero] ${path} must be at the data root ${baseDir}`)
  }
  const source = ts.createSourceFile(
    path,
    readFileSync(path, 'utf8'),
    ts.ScriptTarget.Latest,
    true
  )
  if (hasParseErrors(source)) throw new Error(`[on-zero] unable to parse ${path}`)

  let config: ts.ObjectLiteralExpression | null = null
  for (const statement of source.statements) {
    if (!ts.isExportAssignment(statement) || statement.isExportEquals) continue
    const call = statement.expression
    if (
      !ts.isCallExpression(call) ||
      call.expression.getText(source) !== 'defineConfig'
    ) {
      continue
    }
    const value = call.arguments[0]
    if (value && ts.isObjectLiteralExpression(value)) config = value
  }
  if (!config) {
    throw new Error(
      `[on-zero] ${path} must default export defineConfig({ instances: { ... } })`
    )
  }

  const rootOptions = new Map<string, ts.Expression>()
  for (const option of config.properties) {
    if (!ts.isPropertyAssignment(option)) {
      throw new Error(`[on-zero] ${path} options must use explicit property assignments`)
    }
    const name = propertyName(ts, option.name)
    if (!name) throw new Error(`[on-zero] ${path} has an unsupported option name`)
    if (rootOptions.has(name))
      throw new Error(`[on-zero] ${path} repeats option '${name}'`)
    rootOptions.set(name, option.initializer)
  }
  for (const name of rootOptions.keys()) {
    if (name !== 'instances')
      throw new Error(`[on-zero] ${path} has unknown option '${name}'`)
  }
  const instancesNode = rootOptions.get('instances')
  if (!instancesNode || !ts.isObjectLiteralExpression(instancesNode)) {
    throw new Error(`[on-zero] ${path} must declare a non-empty instances object`)
  }

  const instances: ParsedInstanceConfig[] = []
  for (const property of instancesNode.properties) {
    if (!ts.isPropertyAssignment(property)) {
      throw new Error(
        `[on-zero] ${path} instances must use explicit property assignments`
      )
    }
    const name = propertyName(ts, property.name)
    if (!name) throw new Error(`[on-zero] ${path} has an unsupported instance name`)
    if (!ts.isObjectLiteralExpression(property.initializer)) {
      throw new Error(`[on-zero] instance '${name}' must be an object`)
    }
    if (instances.some((instance) => instance.name === name)) {
      throw new Error(`[on-zero] duplicate instance name '${name}'`)
    }
    let dir = resolve(dirname(path), name)
    let scope: string | null = null
    const supportTables: string[] = []
    const seen = new Set<string>()
    for (const option of property.initializer.properties) {
      if (!ts.isPropertyAssignment(option)) {
        throw new Error(`[on-zero] instance '${name}' options must be assignments`)
      }
      const optionName = propertyName(ts, option.name)
      if (!optionName)
        throw new Error(`[on-zero] instance '${name}' has an invalid option`)
      if (seen.has(optionName)) {
        throw new Error(`[on-zero] instance '${name}' repeats option '${optionName}'`)
      }
      seen.add(optionName)
      if (optionName === 'dir') {
        if (!ts.isStringLiteral(option.initializer)) {
          throw new Error(`[on-zero] instance '${name}' dir must be a string literal`)
        }
        if (option.initializer.text.startsWith('/')) {
          throw new Error(`[on-zero] instance '${name}' dir must be relative to ${path}`)
        }
        dir = resolve(dirname(path), option.initializer.text)
        continue
      }
      if (optionName === 'scope') {
        if (!ts.isStringLiteral(option.initializer)) {
          throw new Error(`[on-zero] instance '${name}' scope must be a string literal`)
        }
        if (!option.initializer.text) {
          throw new Error(`[on-zero] instance '${name}' scope cannot be empty`)
        }
        scope = option.initializer.text
        continue
      }
      if (optionName === 'supportTables') {
        if (!ts.isArrayLiteralExpression(option.initializer)) {
          throw new Error(`[on-zero] instance '${name}' supportTables must be an array`)
        }
        for (const table of option.initializer.elements) {
          if (!ts.isStringLiteral(table)) {
            throw new Error(
              `[on-zero] instance '${name}' supportTables must contain string literals`
            )
          }
          if (!table.text) {
            throw new Error(
              `[on-zero] instance '${name}' supportTables cannot contain an empty table name`
            )
          }
          supportTables.push(table.text)
        }
        continue
      }
      throw new Error(`[on-zero] instance '${name}' has unknown option '${optionName}'`)
    }
    if (!existsSync(dir) || !statSync(dir).isDirectory()) {
      throw new Error(`[on-zero] instance '${name}' directory does not exist: ${dir}`)
    }
    instances.push({ name, dir, scope, supportTables })
  }
  if (instances.length === 0) {
    throw new Error(`[on-zero] ${path} must declare at least one instance`)
  }
  for (const instance of instances) {
    const duplicate = instances.find(
      (candidate) => candidate !== instance && candidate.dir === instance.dir
    )
    if (duplicate) {
      throw new Error(
        `[on-zero] instances '${instance.name}' and '${duplicate.name}' resolve to the same directory: ${instance.dir}`
      )
    }
  }
  return { path, instances }
}

function hasParseErrors(source: ts.SourceFile): boolean {
  // typescript keeps syntax diagnostics on SourceFile but does not expose them
  // in its public type.
  return Boolean(
    (
      source as unknown as {
        parseDiagnostics?: readonly unknown[]
      }
    ).parseDiagnostics?.length
  )
}

/**
 * Which kinds of data exports a single-file namespace declares.
 *
 * `model` covers the exports that belong in generated `models.ts`: `mutate`,
 * `where`, and a `schema` table declaration. `query` covers exported functions
 * that reach a query builder. The two are reported separately because a
 * query-only file must NOT enter models — a models entry with no `mutate` makes
 * `GetZeroMutators` fail its constraint, which silently degrades EVERY
 * `zero.mutate.*` call site in the app to an untyped error. Folder namespaces
 * already get this right by construction (no `mutations.ts` means no model).
 */
type NamespaceExportKinds = { model: boolean; query: boolean }

function namespaceExportKinds(
  ts: typeof import('typescript'),
  baseDir: string,
  path: string
): NamespaceExportKinds {
  const none: NamespaceExportKinds = { model: false, query: false }
  const source = ts.createSourceFile(
    path,
    readFileSync(path, 'utf8'),
    ts.ScriptTarget.Latest,
    true
  )
  if (hasParseErrors(source)) {
    const displayPath = relative(dirname(baseDir), path).split(sep).join('/')
    console.warn(`[on-zero] ignoring ${displayPath}: no recognized data exports`)
    return none
  }
  const functions = new Map<string, ts.ConciseBody>()
  const exported = new Set<string>()

  for (const statement of source.statements) {
    if (ts.isVariableStatement(statement)) {
      const isExported = statement.modifiers?.some(
        (modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword
      )
      for (const declaration of statement.declarationList.declarations) {
        if (!ts.isIdentifier(declaration.name) || !declaration.initializer) continue
        const name = declaration.name.text
        const initializer = declaration.initializer
        if (isExported && ts.isCallExpression(initializer)) {
          const initializerText = initializer.getText(source)
          if (
            (name === 'mutate' && initializerText.startsWith('mutations(')) ||
            (name === 'where' && initializerText.startsWith('serverWhere(')) ||
            (name === 'schema' && initializerText.startsWith('table('))
          ) {
            return { model: true, query: false }
          }
        }
        if (ts.isArrowFunction(initializer) || ts.isFunctionExpression(initializer)) {
          functions.set(name, initializer.body)
          if (isExported) exported.add(name)
        }
      }
      continue
    }
    if (ts.isFunctionDeclaration(statement) && statement.name && statement.body) {
      functions.set(statement.name.text, statement.body)
      if (
        statement.modifiers?.some(
          (modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword
        )
      ) {
        exported.add(statement.name.text)
      }
    }
  }

  const visiting = new Set<string>()
  const reachesQuery = (node: ts.Node): boolean => {
    if (ts.isPropertyAccessExpression(node)) {
      if (ts.isIdentifier(node.expression) && node.expression.text === 'zql') return true
      if (
        ts.isPropertyAccessExpression(node.expression) &&
        node.expression.name.text === 'query'
      )
        return true
    }
    if (ts.isCallExpression(node) && ts.isIdentifier(node.expression)) {
      const name = node.expression.text
      const helper = functions.get(name)
      if (helper && !visiting.has(name)) {
        visiting.add(name)
        const found = reachesQuery(helper)
        visiting.delete(name)
        if (found) return true
      }
    }
    let found = false
    ts.forEachChild(node, (child) => {
      if (!found && reachesQuery(child)) found = true
    })
    return found
  }

  const query = [...exported].some((name) => reachesQuery(functions.get(name)!))
  return { model: false, query }
}

function discoverNamespaces(
  ts: typeof import('typescript'),
  baseDir: string,
  instance: DataInstance,
  instanceDirs: Set<string>
) {
  const namespaces: DataNamespace[] = []
  for (const entry of readdirSync(instance.dir, { withFileTypes: true }).sort((a, b) =>
    a.name.localeCompare(b.name)
  )) {
    if (entry.isFile()) {
      if (!isSourceFile(entry.name) || entry.name === 'on-zero.config.ts') continue
      const path = resolve(instance.dir, entry.name)
      const kinds = namespaceExportKinds(ts, baseDir, path)
      if (!kinds.model && !kinds.query) continue
      const name = basename(entry.name, '.ts')
      namespaces.push({
        name,
        instance: instance.name,
        queryPath: path,
        // a query-only file is not a model, matching the folder layout where a
        // missing mutations.ts leaves modelPath null
        modelPath: kinds.model ? path : null,
        sourcePaths: [path],
      })
      continue
    }
    if (!entry.isDirectory()) continue
    const folder = resolve(instance.dir, entry.name)
    if (entry.name === 'generated' || instanceDirs.has(folder)) continue

    const queryPath = resolve(folder, 'queries.ts')
    const modelPath = resolve(folder, 'mutations.ts')
    const hasQueries = existsSync(queryPath)
    const hasMutations = existsSync(modelPath)
    if (!hasQueries && !hasMutations) {
      if (
        ['models', 'mutations', 'queries'].includes(entry.name) &&
        readdirSync(folder).some(isSourceFile)
      ) {
        throw new Error(
          `[on-zero] ${folder} uses the removed top-level ${entry.name}/ layout; ` +
            `move each namespace to <name>.ts or <name>/queries.ts + mutations.ts`
        )
      }
      continue
    }
    namespaces.push({
      name: entry.name,
      instance: instance.name,
      queryPath: hasQueries ? queryPath : null,
      modelPath: hasMutations ? modelPath : null,
      sourcePaths: [hasQueries && queryPath, hasMutations && modelPath].filter(
        (path): path is string => Boolean(path)
      ),
    })
  }
  return namespaces
}

function metadataPaths(baseDir: string): string[] {
  const paths: string[] = []
  const relations = resolve(baseDir, 'relations.ts')
  if (existsSync(relations)) paths.push(relations)

  const databaseDir = resolve(dirname(baseDir), 'database')
  if (!existsSync(databaseDir)) return paths
  for (const entry of readdirSync(databaseDir, { withFileTypes: true })) {
    if (!entry.isFile() || !isSourceFile(entry.name)) continue
    if (
      entry.name === 'relations.ts' ||
      entry.name === 'zeroSchemaInput.ts' ||
      entry.name.startsWith('schema')
    ) {
      paths.push(resolve(databaseDir, entry.name))
    }
  }
  return paths.sort()
}

function relationTargets(
  ts: typeof import('typescript'),
  paths: string[]
): Map<string, Map<string, string>> {
  const relations = new Map<string, Map<string, string>>()
  for (const path of paths.filter((path) => basename(path) === 'relations.ts')) {
    const source = ts.createSourceFile(
      path,
      readFileSync(path, 'utf8'),
      ts.ScriptTarget.Latest,
      true
    )
    const visit = (node: ts.Node) => {
      if (
        !ts.isCallExpression(node) ||
        node.expression.getText(source) !== 'defineRelations'
      ) {
        ts.forEachChild(node, visit)
        return
      }
      const factory = node.arguments[1]
      if (!factory || (!ts.isArrowFunction(factory) && !ts.isFunctionExpression(factory)))
        return
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
        const tableRelations = relations.get(table) ?? new Map<string, string>()
        for (const relationProperty of tableProperty.initializer.properties) {
          if (
            !ts.isPropertyAssignment(relationProperty) ||
            !ts.isCallExpression(relationProperty.initializer)
          ) {
            continue
          }
          const expression = relationProperty.initializer.expression
          if (!ts.isPropertyAccessExpression(expression)) continue
          const name = relationProperty.name.getText(source).replace(/^['"]|['"]$/g, '')
          tableRelations.set(name, expression.name.text)
        }
        relations.set(table, tableRelations)
      }
    }
    visit(source)
  }
  return relations
}

function tableColumns(
  ts: typeof import('typescript'),
  paths: string[],
  namespaces: DataNamespace[]
): Map<string, Set<string>> {
  const columns = new Map<string, Set<string>>()
  const sources = new Set([
    ...paths,
    ...namespaces.flatMap((namespace) => namespace.sourcePaths),
  ])
  for (const path of sources) {
    const source = ts.createSourceFile(
      path,
      readFileSync(path, 'utf8'),
      ts.ScriptTarget.Latest,
      true
    )
    const visit = (node: ts.Node) => {
      if (
        !ts.isVariableDeclaration(node) ||
        !ts.isIdentifier(node.name) ||
        !node.initializer
      ) {
        ts.forEachChild(node, visit)
        return
      }
      const tableNames = new Set([node.name.text])
      let foundColumns: ts.ObjectLiteralExpression | null = null
      const inspect = (candidate: ts.Node) => {
        if (!ts.isCallExpression(candidate)) {
          ts.forEachChild(candidate, inspect)
          return
        }
        for (const argument of candidate.arguments) {
          if (
            ts.isStringLiteral(argument) &&
            /table/i.test(candidate.expression.getText(source))
          ) {
            tableNames.add(argument.text)
          }
          if (ts.isObjectLiteralExpression(argument)) foundColumns = argument
        }
        inspect(candidate.expression)
      }
      inspect(node.initializer)
      if (foundColumns) {
        const names = new Set(
          (foundColumns as ts.ObjectLiteralExpression).properties
            .map((property) => property.name?.getText(source).replace(/^['"]|['"]$/g, ''))
            .filter((name): name is string => Boolean(name))
        )
        for (const tableName of tableNames) columns.set(tableName, names)
      }
      ts.forEachChild(node, visit)
    }
    visit(source)
  }
  return columns
}

function relatedTables(
  ts: typeof import('typescript'),
  namespace: DataNamespace,
  relations: Map<string, Map<string, string>>
): Array<{ table: string; query: string }> {
  if (!namespace.queryPath) return []
  const source = ts.createSourceFile(
    namespace.queryPath,
    readFileSync(namespace.queryPath, 'utf8'),
    ts.ScriptTarget.Latest,
    true
  )
  const reached: Array<{ table: string; query: string }> = []
  const functions = new Map<string, ts.ConciseBody>()
  const exported = new Set<string>()

  for (const statement of source.statements) {
    if (ts.isVariableStatement(statement)) {
      const isExported = statement.modifiers?.some(
        (modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword
      )
      for (const declaration of statement.declarationList.declarations) {
        if (
          ts.isIdentifier(declaration.name) &&
          declaration.initializer &&
          (ts.isArrowFunction(declaration.initializer) ||
            ts.isFunctionExpression(declaration.initializer))
        ) {
          functions.set(declaration.name.text, declaration.initializer.body)
          if (isExported) exported.add(declaration.name.text)
        }
      }
    } else if (ts.isFunctionDeclaration(statement) && statement.name && statement.body) {
      functions.set(statement.name.text, statement.body)
      if (
        statement.modifiers?.some(
          (modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword
        )
      ) {
        exported.add(statement.name.text)
      }
    }
  }

  const rootTable = (node: ts.Node): string | null => {
    const text = node.getText(source)
    return text.match(/(?:\bzql|\.query)\.([A-Za-z_$][\w$]*)/)?.[1] ?? null
  }

  const visiting = new Set<string>()
  const visit = (node: ts.Node, currentTable: string, query: string) => {
    if (
      ts.isCallExpression(node) &&
      ts.isPropertyAccessExpression(node.expression) &&
      node.expression.name.text === 'related'
    ) {
      const nameArg = node.arguments[0]
      if (!nameArg || !ts.isStringLiteral(nameArg)) {
        throw new Error(
          `[on-zero] ${namespace.name}.${query} uses related() without a string literal; ` +
            `sync membership must be statically derivable`
        )
      }
      const sourceTable = rootTable(node.expression.expression) ?? currentTable
      const target = relations.get(sourceTable)?.get(nameArg.text)
      if (!target) {
        throw new Error(
          `[on-zero] ${namespace.name}.${query} related('${nameArg.text}') cannot be resolved ` +
            `from table '${sourceTable}' through relations.ts`
        )
      }
      reached.push({ table: target, query })
      visit(node.expression.expression, currentTable, query)
      const callback = node.arguments[1]
      if (
        callback &&
        (ts.isArrowFunction(callback) || ts.isFunctionExpression(callback))
      ) {
        visit(callback.body, target, query)
      }
      return
    }
    if (ts.isCallExpression(node) && ts.isIdentifier(node.expression)) {
      const helper = functions.get(node.expression.text)
      const key = `${query}:${node.expression.text}`
      if (helper && !visiting.has(key)) {
        visiting.add(key)
        visit(helper, currentTable, query)
        visiting.delete(key)
      }
    }
    ts.forEachChild(node, (child) => visit(child, currentTable, query))
  }

  for (const name of exported) {
    if (['mutate', 'schema', 'where'].includes(name)) continue
    visit(functions.get(name)!, namespace.name, name)
  }
  return reached
}

function mutationSupportTables(
  ts: typeof import('typescript'),
  baseDir: string,
  sourceRoots: string[],
  namespace: DataNamespace
): string[] {
  if (!namespace.modelPath) return []
  const tables = new Set<string>()
  const visited = new Set<string>()

  const scan = (path: string) => {
    if (visited.has(path)) return
    visited.add(path)
    const source = ts.createSourceFile(
      path,
      readFileSync(path, 'utf8'),
      ts.ScriptTarget.Latest,
      true
    )

    const visit = (node: ts.Node) => {
      if (
        ts.isPropertyAccessExpression(node) &&
        ts.isPropertyAccessExpression(node.expression) &&
        ['mutate', 'query'].includes(node.expression.name.text)
      ) {
        const transaction = node.expression.expression
        if (
          (ts.isIdentifier(transaction) && transaction.text === 'tx') ||
          (ts.isPropertyAccessExpression(transaction) && transaction.name.text === 'tx')
        ) {
          tables.add(node.name.text)
        }
      }

      if (
        (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) &&
        node.moduleSpecifier &&
        ts.isStringLiteral(node.moduleSpecifier)
      ) {
        const specifier = node.moduleSpecifier.text
        const unresolved = specifier.startsWith('~/data/')
          ? resolve(baseDir, specifier.slice('~/data/'.length))
          : specifier.startsWith('.')
            ? resolve(dirname(path), specifier)
            : null
        if (unresolved) {
          for (const candidate of [
            unresolved.endsWith('.ts') ? unresolved : `${unresolved}.ts`,
            resolve(unresolved, 'index.ts'),
          ]) {
            if (
              existsSync(candidate) &&
              sourceRoots.some((root) => isWithin(root, candidate))
            ) {
              scan(candidate)
              break
            }
          }
        }
      }

      ts.forEachChild(node, visit)
    }
    visit(source)
  }

  scan(namespace.modelPath)
  return [...tables].sort()
}

function assertNoInstanceFiles(roots: string[]) {
  const visited = new Set<string>()
  const walk = (dir: string) => {
    if (visited.has(dir) || !existsSync(dir)) return
    visited.add(dir)
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (entry.name === 'node_modules' || entry.name === 'generated') continue
      const path = resolve(dir, entry.name)
      if (entry.isDirectory()) {
        walk(path)
      } else if (entry.isFile() && entry.name === 'instance.ts') {
        throw new Error(
          `[on-zero] ${path} uses removed instance.ts configuration; delete it and configure instances in on-zero.config.ts`
        )
      }
    }
  }
  for (const root of roots) walk(root)
}

function assertNoUnclaimedNamespaces(
  ts: typeof import('typescript'),
  baseDir: string,
  configPath: string,
  instanceDirs: string[]
) {
  const walk = (dir: string) => {
    if (instanceDirs.some((instanceDir) => isWithin(instanceDir, dir))) return
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (entry.name === 'node_modules' || entry.name === 'generated') continue
      const path = resolve(dir, entry.name)
      if (entry.isDirectory()) {
        walk(path)
        continue
      }
      if (
        !entry.isFile() ||
        !isSourceFile(entry.name) ||
        path === configPath ||
        entry.name === 'instance.ts'
      ) {
        continue
      }
      const kinds = namespaceExportKinds(ts, baseDir, path)
      if (kinds.model || kinds.query) {
        throw new Error(
          `[on-zero] data namespace ${path} is outside every instance directory declared in ${configPath}`
        )
      }
    }
  }
  walk(baseDir)
}

export function discoverDataLayout(
  ts: typeof import('typescript'),
  baseDir: string,
  configPath?: string
): DataLayout {
  const config = readDataConfig(ts, baseDir, configPath)
  const configured =
    config?.instances ??
    ([
      { name: 'default', dir: baseDir, scope: null, supportTables: [] },
    ] satisfies ParsedInstanceConfig[])
  const sourceRoots = [
    ...new Set([baseDir, ...configured.map((instance) => instance.dir)]),
  ]
  assertNoInstanceFiles(sourceRoots)
  if (config) {
    assertNoUnclaimedNamespaces(
      ts,
      baseDir,
      config.path,
      configured.map((instance) => instance.dir)
    )
  }
  const instances: DataInstance[] = configured.map((instance) => ({
    name: instance.name,
    dir: instance.dir,
    scope: instance.scope,
    namespaces: [],
    syncTables: [],
    supportTables: [],
    declaredSupportTables: instance.supportTables,
  }))
  const instanceDirs = new Set(instances.map((instance) => instance.dir))

  for (const instance of instances) {
    instance.namespaces = discoverNamespaces(ts, baseDir, instance, instanceDirs)
  }
  const namespaces = instances.flatMap((instance) => instance.namespaces)
  const owners = new Map<string, string>()
  for (const namespace of namespaces) {
    const owner = owners.get(namespace.name)
    if (owner) {
      throw new Error(
        `[on-zero] namespace '${namespace.name}' is claimed by instances '${owner}' and '${namespace.instance}'`
      )
    }
    owners.set(namespace.name, namespace.instance)
  }

  const metadata = metadataPaths(baseDir)
  const relations = relationTargets(ts, metadata)
  const columns = tableColumns(ts, metadata, namespaces)
  const relatedOwners = new Map<string, string>()
  for (const instance of instances) {
    const syncTables = new Set(instance.namespaces.map((namespace) => namespace.name))
    for (const namespace of instance.namespaces) {
      for (const reached of relatedTables(ts, namespace, relations)) {
        const owner = owners.get(reached.table) ?? relatedOwners.get(reached.table)
        if (owner && owner !== instance.name) {
          throw new Error(
            `[on-zero] ${namespace.name}.${reached.query} in instance '${instance.name}' reaches ` +
              `table '${reached.table}' owned by instance '${owner}'`
          )
        }
        relatedOwners.set(reached.table, instance.name)
        syncTables.add(reached.table)
      }
    }
    instance.syncTables = [...syncTables].sort()
    if (instance.scope) {
      for (const table of instance.syncTables) {
        if (!columns.get(table)?.has(instance.scope)) {
          throw new Error(
            `[on-zero] table '${table}' in instance '${instance.name}' is missing scope column '${instance.scope}'`
          )
        }
      }
    }
  }

  for (const instance of instances) {
    // declared entries deliberately bypass the owner guard below: a table owned
    // by another instance can still be written here (a control transaction that
    // seeds project-owned rows), and that write has to stay mappable in THIS
    // instance's change log or every later pull throws on it.
    const supportTables = new Set<string>(instance.declaredSupportTables)
    for (const namespace of instance.namespaces) {
      for (const table of mutationSupportTables(ts, baseDir, sourceRoots, namespace)) {
        if (owners.has(table) || relatedOwners.has(table)) continue
        if (!instance.syncTables.includes(table)) {
          supportTables.add(table)
        }
      }
    }
    instance.supportTables = [...supportTables]
      .filter((table) => !instance.syncTables.includes(table))
      .sort()
  }

  return {
    instances,
    namespaces,
    metadataPaths: config ? [...metadata, config.path].sort() : metadata,
    sourceRoots,
  }
}

export function namespaceImportPath(baseDir: string, path: string): string {
  return `../${toImportPath(baseDir, path)}`
}
