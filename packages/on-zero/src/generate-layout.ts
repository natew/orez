import { existsSync, readFileSync, readdirSync } from 'node:fs'
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
}

export type DataLayout = {
  instances: DataInstance[]
  namespaces: DataNamespace[]
  metadataPaths: string[]
}

const isSourceFile = (name: string) =>
  name.endsWith('.ts') &&
  !name.endsWith('.d.ts') &&
  !name.endsWith('.test.ts') &&
  !name.endsWith('.spec.ts')

const toImportPath = (baseDir: string, path: string) =>
  relative(baseDir, path).split(sep).join('/').replace(/\.ts$/, '')

function readScope(ts: typeof import('typescript'), instancePath: string): string {
  const source = ts.createSourceFile(
    instancePath,
    readFileSync(instancePath, 'utf8'),
    ts.ScriptTarget.Latest,
    true
  )
  let scope: string | null = null

  for (const statement of source.statements) {
    if (!ts.isExportAssignment(statement) || statement.isExportEquals) continue
    const call = statement.expression
    if (
      !ts.isCallExpression(call) ||
      call.expression.getText(source) !== 'defineInstance'
    ) {
      continue
    }
    const options = call.arguments[0]
    if (!options || !ts.isObjectLiteralExpression(options)) continue
    for (const property of options.properties) {
      if (
        !ts.isPropertyAssignment(property) ||
        property.name.getText(source) !== 'scope'
      ) {
        continue
      }
      if (ts.isStringLiteral(property.initializer)) scope = property.initializer.text
    }
  }

  if (!scope) {
    throw new Error(
      `[on-zero] ${instancePath} must default export defineInstance({ scope: 'columnName' })`
    )
  }
  return scope
}

function collectInstanceDirs(baseDir: string): string[] {
  const found: string[] = []
  const walk = (dir: string) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (
        !entry.isDirectory() ||
        entry.name === 'generated' ||
        entry.name === 'node_modules'
      ) {
        continue
      }
      const child = resolve(dir, entry.name)
      if (existsSync(resolve(child, 'instance.ts'))) found.push(child)
      walk(child)
    }
  }
  walk(baseDir)
  return found.sort()
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

function hasNamespaceExports(
  ts: typeof import('typescript'),
  baseDir: string,
  path: string
): boolean {
  const source = ts.createSourceFile(
    path,
    readFileSync(path, 'utf8'),
    ts.ScriptTarget.Latest,
    true
  )
  if (hasParseErrors(source)) {
    const displayPath = relative(dirname(baseDir), path).split(sep).join('/')
    console.warn(`[on-zero] ignoring ${displayPath}: no recognized data exports`)
    return false
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
            return true
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

  return [...exported].some((name) => reachesQuery(functions.get(name)!))
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
      if (!isSourceFile(entry.name) || entry.name === 'instance.ts') continue
      const path = resolve(instance.dir, entry.name)
      if (!hasNamespaceExports(ts, baseDir, path)) continue
      const name = basename(entry.name, '.ts')
      namespaces.push({
        name,
        instance: instance.name,
        queryPath: path,
        modelPath: path,
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
              (candidate === baseDir || candidate.startsWith(`${baseDir}${sep}`))
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

export function discoverDataLayout(
  ts: typeof import('typescript'),
  baseDir: string
): DataLayout {
  const instanceDirs = collectInstanceDirs(baseDir)
  const instanceDirSet = new Set(instanceDirs)
  const instances: DataInstance[] = [
    {
      name: 'default',
      dir: baseDir,
      scope: null,
      namespaces: [],
      syncTables: [],
      supportTables: [],
    },
    ...instanceDirs.map((dir) => ({
      name: basename(dir),
      dir,
      scope: readScope(ts, resolve(dir, 'instance.ts')),
      namespaces: [],
      syncTables: [],
      supportTables: [],
    })),
  ]
  const duplicateInstance = instances.find(
    (instance, index) =>
      instances.findIndex((candidate) => candidate.name === instance.name) !== index
  )
  if (duplicateInstance) {
    throw new Error(`[on-zero] duplicate instance name '${duplicateInstance.name}'`)
  }

  for (const instance of instances) {
    instance.namespaces = discoverNamespaces(ts, baseDir, instance, instanceDirSet)
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
    const supportTables = new Set<string>()
    for (const namespace of instance.namespaces) {
      for (const table of mutationSupportTables(ts, baseDir, namespace)) {
        if (owners.has(table) || relatedOwners.has(table)) continue
        if (!instance.syncTables.includes(table)) {
          supportTables.add(table)
        }
      }
    }
    instance.supportTables = [...supportTables].sort()
  }

  return { instances, namespaces, metadataPaths: metadata }
}

export function namespaceImportPath(baseDir: string, path: string): string {
  return `../${toImportPath(baseDir, path)}`
}
