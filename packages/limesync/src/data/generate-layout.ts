import { existsSync, readdirSync, statSync } from 'node:fs'
import { basename, dirname, relative, resolve, sep } from 'node:path'

import * as ts from 'typescript/unstable/ast'

import type { NativeTypeScriptProject } from './native-typescript.js'

export type DataNamespace = {
  name: string
  instance: string
  queryPath: string | null
  modelPath: string | null
  aggregatePath: string | null
  table: string | null
  sourcePaths: string[]
}

export type DataInstance = {
  name: string
  dir: string
  scope: string | null
  namespaces: DataNamespace[]
  tables: string[]
  syncTables: string[]
  supportTables: string[]
  /** `supportTables` declared in `data.config.ts`. */
  declaredSupportTables: string[]
}

export type DataLayout = {
  instances: DataInstance[]
  namespaces: DataNamespace[]
  metadataPaths: string[]
  sourceRoots: string[]
  localQueries: Set<string>
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

function propertyName(name: ts.PropertyName): string | null {
  if (ts.isIdentifier(name) || ts.isStringLiteral(name) || ts.isNumericLiteral(name)) {
    return name.text
  }
  return null
}

function readDataConfig(
  project: NativeTypeScriptProject,
  baseDir: string,
  configPath: string | undefined
): { path: string; instances: ParsedInstanceConfig[] } | null {
  const path = configPath ? resolve(configPath) : resolve(baseDir, 'data.config.ts')
  if (!existsSync(path)) {
    if (configPath) throw new Error(`[limesync] config file does not exist: ${path}`)
    return null
  }
  if (dirname(path) !== baseDir) {
    throw new Error(`[limesync] ${path} must be at the data root ${baseDir}`)
  }
  const source = project.sourceFile(path)
  if (project.hasParseErrors(path)) throw new Error(`[limesync] unable to parse ${path}`)

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
      `[limesync] ${path} must default export defineConfig({ instances: { ... } })`
    )
  }

  const rootOptions = new Map<string, ts.Expression>()
  for (const option of config.properties) {
    if (!ts.isPropertyAssignment(option)) {
      throw new Error(`[limesync] ${path} options must use explicit property assignments`)
    }
    const name = propertyName(option.name)
    if (!name) throw new Error(`[limesync] ${path} has an unsupported option name`)
    if (rootOptions.has(name))
      throw new Error(`[limesync] ${path} repeats option '${name}'`)
    rootOptions.set(name, option.initializer)
  }
  for (const name of rootOptions.keys()) {
    if (name !== 'instances')
      throw new Error(`[limesync] ${path} has unknown option '${name}'`)
  }
  const instancesNode = rootOptions.get('instances')
  if (!instancesNode || !ts.isObjectLiteralExpression(instancesNode)) {
    throw new Error(`[limesync] ${path} must declare a non-empty instances object`)
  }

  const instances: ParsedInstanceConfig[] = []
  for (const property of instancesNode.properties) {
    if (!ts.isPropertyAssignment(property)) {
      throw new Error(
        `[limesync] ${path} instances must use explicit property assignments`
      )
    }
    const name = propertyName(property.name)
    if (!name) throw new Error(`[limesync] ${path} has an unsupported instance name`)
    if (!ts.isObjectLiteralExpression(property.initializer)) {
      throw new Error(`[limesync] instance '${name}' must be an object`)
    }
    if (instances.some((instance) => instance.name === name)) {
      throw new Error(`[limesync] duplicate instance name '${name}'`)
    }
    let dir = resolve(dirname(path), name)
    let scope: string | null = null
    const supportTables: string[] = []
    const seen = new Set<string>()
    for (const option of property.initializer.properties) {
      if (!ts.isPropertyAssignment(option)) {
        throw new Error(`[limesync] instance '${name}' options must be assignments`)
      }
      const optionName = propertyName(option.name)
      if (!optionName)
        throw new Error(`[limesync] instance '${name}' has an invalid option`)
      if (seen.has(optionName)) {
        throw new Error(`[limesync] instance '${name}' repeats option '${optionName}'`)
      }
      seen.add(optionName)
      if (optionName === 'dir') {
        if (!ts.isStringLiteral(option.initializer)) {
          throw new Error(`[limesync] instance '${name}' dir must be a string literal`)
        }
        if (option.initializer.text.startsWith('/')) {
          throw new Error(`[limesync] instance '${name}' dir must be relative to ${path}`)
        }
        dir = resolve(dirname(path), option.initializer.text)
        continue
      }
      if (optionName === 'scope') {
        if (!ts.isStringLiteral(option.initializer)) {
          throw new Error(`[limesync] instance '${name}' scope must be a string literal`)
        }
        if (!option.initializer.text) {
          throw new Error(`[limesync] instance '${name}' scope cannot be empty`)
        }
        scope = option.initializer.text
        continue
      }
      if (optionName === 'supportTables') {
        if (!ts.isArrayLiteralExpression(option.initializer)) {
          throw new Error(`[limesync] instance '${name}' supportTables must be an array`)
        }
        for (const table of option.initializer.elements) {
          if (!ts.isStringLiteral(table)) {
            throw new Error(
              `[limesync] instance '${name}' supportTables must contain string literals`
            )
          }
          if (!table.text) {
            throw new Error(
              `[limesync] instance '${name}' supportTables cannot contain an empty table name`
            )
          }
          supportTables.push(table.text)
        }
        continue
      }
      throw new Error(`[limesync] instance '${name}' has unknown option '${optionName}'`)
    }
    if (!existsSync(dir) || !statSync(dir).isDirectory()) {
      throw new Error(`[limesync] instance '${name}' directory does not exist: ${dir}`)
    }
    instances.push({ name, dir, scope, supportTables })
  }
  if (instances.length === 0) {
    throw new Error(`[limesync] ${path} must declare at least one instance`)
  }
  for (const instance of instances) {
    const duplicate = instances.find(
      (candidate) => candidate !== instance && candidate.dir === instance.dir
    )
    if (duplicate) {
      throw new Error(
        `[limesync] instances '${instance.name}' and '${duplicate.name}' resolve to the same directory: ${instance.dir}`
      )
    }
  }
  return { path, instances }
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
const BUILDER_IMPORT_SOURCES = new Set([
  'limesync/data',
  'limesync/zero',
  'on-zero',
  '~/zero/core',
])
const BUILDER_EXPORT_NAMES = new Set(['query', 'zql', 'builder'])

function collectBuilderBindings(source: ts.SourceFile): Set<string> {
  const bindings = new Set<string>(['zql'])
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement)) continue
    if (statement.importClause?.phaseModifier === ts.SyntaxKind.TypeKeyword) continue
    if (!ts.isStringLiteral(statement.moduleSpecifier)) continue
    const spec = statement.moduleSpecifier.text
    const isBuilderSource =
      BUILDER_IMPORT_SOURCES.has(spec) ||
      /(?:^|\/)(?:schema|syncedQueries|generated)(?:\.[cm]?[jt]sx?)?$/.test(spec)
    if (!isBuilderSource) continue
    const named = statement.importClause?.namedBindings
    if (!named || !ts.isNamedImports(named)) continue
    for (const el of named.elements) {
      if (el.isTypeOnly) continue
      const imported = el.propertyName ? el.propertyName.text : el.name.text
      if (BUILDER_EXPORT_NAMES.has(imported)) bindings.add(el.name.text)
    }
  }
  return bindings
}

function collectParamNamesFromBinding(name: ts.BindingName, out: Set<string>) {
  if (ts.isIdentifier(name)) {
    out.add(name.text)
  } else if (ts.isObjectBindingPattern(name) || ts.isArrayBindingPattern(name)) {
    for (const el of name.elements) {
      if (ts.isBindingElement(el) && el.name) collectParamNamesFromBinding(el.name, out)
    }
  }
}

function collectShadowedParams(fn: ts.Node): Set<string> {
  const out = new Set<string>()
  if (
    ts.isArrowFunction(fn) ||
    ts.isFunctionExpression(fn) ||
    ts.isFunctionDeclaration(fn) ||
    ts.isMethodDeclaration(fn)
  ) {
    for (const p of fn.parameters) collectParamNamesFromBinding(p.name, out)
  }
  return out
}

function extendScopeForNode(
  node: ts.Node,
  parentShadowed: ReadonlySet<string>
): Set<string> {
  const next = new Set(parentShadowed)
  if (
    ts.isArrowFunction(node) ||
    ts.isFunctionExpression(node) ||
    ts.isFunctionDeclaration(node) ||
    ts.isMethodDeclaration(node)
  ) {
    for (const p of node.parameters) collectParamNamesFromBinding(p.name, next)
    if (ts.isFunctionExpression(node) && node.name) next.add(node.name.text)
    return next
  }
  if (ts.isBlock(node)) {
    for (const stmt of node.statements) {
      if (ts.isVariableStatement(stmt)) {
        for (const decl of stmt.declarationList.declarations) {
          collectParamNamesFromBinding(decl.name, next)
        }
      } else if (ts.isFunctionDeclaration(stmt) && stmt.name) {
        next.add(stmt.name.text)
      }
    }
  }
  if (ts.isCatchClause(node) && node.variableDeclaration) {
    collectParamNamesFromBinding(node.variableDeclaration.name, next)
  }
  return next
}

type NamespaceExportKinds = { model: boolean; query: boolean }

function namespaceExportKinds(
  project: NativeTypeScriptProject,
  baseDir: string,
  path: string
): NamespaceExportKinds {
  const none: NamespaceExportKinds = { model: false, query: false }
  const source = project.sourceFile(path)
  if (project.hasParseErrors(path)) {
    const displayPath = relative(dirname(baseDir), path).split(sep).join('/')
    console.warn(`[limesync] ignoring ${displayPath}: no recognized data exports`)
    return none
  }
  const builderBindings = collectBuilderBindings(source)
  const functions = new Map<string, { body: ts.ConciseBody; shadowed: Set<string> }>()
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
          functions.set(name, {
            body: initializer.body,
            shadowed: collectShadowedParams(initializer),
          })
          if (isExported) exported.add(name)
        }
      }
      continue
    }
    if (ts.isFunctionDeclaration(statement) && statement.name && statement.body) {
      functions.set(statement.name.text, {
        body: statement.body,
        shadowed: collectShadowedParams(statement),
      })
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
  const reachesQuery = (node: ts.Node, shadowed: ReadonlySet<string>): boolean => {
    const nextShadowed =
      ts.isArrowFunction(node) ||
      ts.isFunctionExpression(node) ||
      ts.isFunctionDeclaration(node) ||
      ts.isMethodDeclaration(node) ||
      ts.isBlock(node) ||
      ts.isCatchClause(node)
        ? extendScopeForNode(node, shadowed)
        : shadowed
    if (ts.isPropertyAccessExpression(node)) {
      if (
        ts.isIdentifier(node.expression) &&
        builderBindings.has(node.expression.text) &&
        !nextShadowed.has(node.expression.text)
      )
        return true
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
        const found = reachesQuery(helper.body, helper.shadowed)
        visiting.delete(name)
        if (found) return true
      }
    }
    let found = false
    node.forEachChild((child) => {
      if (!found && reachesQuery(child, nextShadowed)) found = true
    })
    return found
  }

  const query = [...exported].some((name) => {
    const fn = functions.get(name)!
    return reachesQuery(fn.body, fn.shadowed)
  })
  return { model: false, query }
}

function mutationTable(
  project: NativeTypeScriptProject,
  path: string,
  namespace: string,
  databaseColumns: ReadonlyMap<string, Set<string>>
): string | null {
  const source = project.sourceFile(path)
  let schemaTable: string | null = null
  let actionOnly = false

  for (const statement of source.statements) {
    if (!ts.isVariableStatement(statement)) continue
    if (
      !statement.modifiers?.some(
        (modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword
      )
    ) {
      continue
    }
    for (const declaration of statement.declarationList.declarations) {
      if (!ts.isIdentifier(declaration.name) || !declaration.initializer) continue
      if (
        declaration.name.text === 'mutate' &&
        ts.isCallExpression(declaration.initializer)
      ) {
        const firstArgument = declaration.initializer.arguments[0]
        if (firstArgument && ts.isStringLiteral(firstArgument)) {
          return firstArgument.text
        }
        actionOnly =
          declaration.initializer.arguments.length === 1 &&
          firstArgument !== undefined &&
          ts.isObjectLiteralExpression(firstArgument)
      }
      if (declaration.name.text !== 'schema') continue
      const visit = (node: ts.Node) => {
        if (
          ts.isCallExpression(node) &&
          node.expression.getText(source) === 'table' &&
          node.arguments[0] &&
          ts.isStringLiteral(node.arguments[0])
        ) {
          schemaTable = node.arguments[0].text
          return
        }
        node.forEachChild(visit)
      }
      visit(declaration.initializer)
    }
  }

  return schemaTable ?? (actionOnly && !databaseColumns.has(namespace) ? null : namespace)
}

function discoverNamespaces(
  project: NativeTypeScriptProject,
  baseDir: string,
  instance: DataInstance,
  instanceDirs: Set<string>,
  databaseColumns: ReadonlyMap<string, Set<string>>
) {
  const namespaces: DataNamespace[] = []
  for (const entry of readdirSync(instance.dir, { withFileTypes: true }).sort((a, b) =>
    a.name.localeCompare(b.name)
  )) {
    if (entry.isFile()) {
      if (!isSourceFile(entry.name) || entry.name === 'data.config.ts') continue
      const path = resolve(instance.dir, entry.name)
      const kinds = namespaceExportKinds(project, baseDir, path)
      if (!kinds.model && !kinds.query) continue
      const name = basename(entry.name, '.ts')
      namespaces.push({
        name,
        instance: instance.name,
        queryPath: path,
        // a query-only file is not a model, matching the folder layout where a
        // missing mutations.ts leaves modelPath null
        modelPath: kinds.model ? path : null,
        aggregatePath: null,
        table: kinds.model ? mutationTable(project, path, name, databaseColumns) : null,
        sourcePaths: [path],
      })
      continue
    }
    if (!entry.isDirectory()) continue
    const folder = resolve(instance.dir, entry.name)
    if (entry.name === 'generated' || instanceDirs.has(folder)) continue

    const queryPath = resolve(folder, 'queries.ts')
    const modelPath = resolve(folder, 'mutations.ts')
    const aggregatePath = resolve(folder, 'aggregates.ts')
    const hasQueries = existsSync(queryPath)
    const hasMutations = existsSync(modelPath)
    const hasAggregates = existsSync(aggregatePath)
    if (!hasQueries && !hasMutations && !hasAggregates) {
      if (
        ['models', 'mutations', 'queries'].includes(entry.name) &&
        readdirSync(folder).some(isSourceFile)
      ) {
        throw new Error(
          `[limesync] ${folder} uses the removed top-level ${entry.name}/ layout; ` +
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
      aggregatePath: hasAggregates ? aggregatePath : null,
      table: hasMutations
        ? mutationTable(project, modelPath, entry.name, databaseColumns)
        : null,
      sourcePaths: [
        hasQueries && queryPath,
        hasMutations && modelPath,
        hasAggregates && aggregatePath,
      ].filter((path): path is string => Boolean(path)),
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
  project: NativeTypeScriptProject,
  paths: string[]
): Map<string, Map<string, string>> {
  const relations = new Map<string, Map<string, string>>()
  for (const path of paths.filter((path) => basename(path) === 'relations.ts')) {
    const source = project.sourceFile(path)
    const visit = (node: ts.Node) => {
      if (
        !ts.isCallExpression(node) ||
        node.expression.getText(source) !== 'defineRelations'
      ) {
        node.forEachChild(visit)
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

function extractOneTableChainTs(node: ts.Expression): {
  name: string
  visibility?: 'local' | 'public' | 'private'
  chainNodes: ts.Node[]
} | null {
  let cur: ts.Expression | undefined = node
  let visibility: 'local' | 'public' | 'private' | undefined
  let tableName: string | null = null
  const chainNodes: ts.Node[] = []
  while (cur && ts.isCallExpression(cur)) {
    chainNodes.push(cur)
    if (ts.isPropertyAccessExpression(cur.expression)) {
      const method = cur.expression.name.text
      if (
        visibility === undefined &&
        (method === 'local' || method === 'public' || method === 'private')
      ) {
        visibility = method
      }
      cur = cur.expression.expression
      continue
    }
    if (
      ts.isIdentifier(cur.expression) &&
      cur.expression.text === 'table' &&
      cur.arguments[0] &&
      ts.isStringLiteral(cur.arguments[0])
    ) {
      tableName = cur.arguments[0].text
    }
    break
  }
  return tableName ? { name: tableName, visibility, chainNodes } : null
}

function collectCreateSchemaLocalTables(
  expr: ts.Expression,
  bindingTables: ReadonlyMap<
    string,
    { name: string; visibility?: 'local' | 'public' | 'private' }
  >,
  localTables: Set<string>
): boolean {
  if (
    !ts.isCallExpression(expr) ||
    !ts.isIdentifier(expr.expression) ||
    expr.expression.text !== 'createSchema' ||
    !expr.arguments[0] ||
    !ts.isObjectLiteralExpression(expr.arguments[0])
  ) {
    return false
  }
  for (const prop of expr.arguments[0].properties) {
    if (
      ts.isPropertyAssignment(prop) &&
      propertyName(prop.name) === 'tables' &&
      ts.isArrayLiteralExpression(prop.initializer)
    ) {
      for (const el of prop.initializer.elements) {
        if (ts.isIdentifier(el)) {
          const bound = bindingTables.get(el.text)
          if (bound?.visibility === 'local') localTables.add(bound.name)
        } else if (ts.isCallExpression(el)) {
          const extracted = extractOneTableChainTs(el)
          if (extracted?.visibility === 'local') localTables.add(extracted.name)
        }
      }
      return true
    }
  }
  return false
}

function oneLocalTables(source: ts.SourceFile): Set<string> {
  const localTables = new Set<string>()
  const bindingTables = new Map<
    string,
    { name: string; visibility?: 'local' | 'public' | 'private' }
  >()
  const topLevelSchemaExprs: Array<{
    expr: ts.Expression
    exported: boolean
    isSchemaName: boolean
  }> = []

  // inspect ONLY top-level module statements so nested helper declarations and
  // uncalled decoy createSchema() calls never shadow the exported module schema.
  for (const stmt of source.statements) {
    if (ts.isVariableStatement(stmt)) {
      const isExported = Boolean(
        stmt.modifiers?.some((m) => m.kind === ts.SyntaxKind.ExportKeyword)
      )
      for (const decl of stmt.declarationList.declarations) {
        if (!ts.isIdentifier(decl.name) || !decl.initializer) continue
        const extracted = extractOneTableChainTs(decl.initializer)
        if (extracted) {
          bindingTables.set(decl.name.text, {
            name: extracted.name,
            visibility: extracted.visibility,
          })
          continue
        }
        if (
          ts.isCallExpression(decl.initializer) &&
          ts.isIdentifier(decl.initializer.expression) &&
          decl.initializer.expression.text === 'createSchema'
        ) {
          topLevelSchemaExprs.push({
            expr: decl.initializer,
            exported: isExported,
            isSchemaName: decl.name.text === 'schema',
          })
        }
      }
    } else if (ts.isExportAssignment(stmt)) {
      if (
        ts.isCallExpression(stmt.expression) &&
        ts.isIdentifier(stmt.expression.expression) &&
        stmt.expression.expression.text === 'createSchema'
      ) {
        topLevelSchemaExprs.push({
          expr: stmt.expression,
          exported: true,
          isSchemaName: true,
        })
      }
    }
  }

  // prefer `export const schema = createSchema(...)` / `export default createSchema(...)`,
  // then any exported createSchema(...), then any top-level createSchema(...).
  const preferred = topLevelSchemaExprs.filter((s) => s.exported && s.isSchemaName)
  const candidates =
    preferred.length > 0
      ? preferred
      : topLevelSchemaExprs.filter((s) => s.exported).length > 0
        ? topLevelSchemaExprs.filter((s) => s.exported)
        : topLevelSchemaExprs

  let foundCreateSchema = false
  for (const candidate of candidates) {
    if (collectCreateSchemaLocalTables(candidate.expr, bindingTables, localTables)) {
      foundCreateSchema = true
    }
  }

  if (!foundCreateSchema) {
    for (const bound of bindingTables.values()) {
      if (bound.visibility === 'local') localTables.add(bound.name)
    }
  }

  return localTables
}

function oneRelationTargets(source: ts.SourceFile) {
  const tables = new Map<string, string>()
  const visitTable = (node: ts.Node) => {
    if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name)) {
      let init = node.initializer
      while (init && ts.isCallExpression(init)) {
        const [name] = init.arguments
        if (
          ts.isIdentifier(init.expression) &&
          init.expression.text === 'table' &&
          name &&
          ts.isStringLiteral(name)
        ) {
          tables.set(node.name.text, name.text)
          break
        }
        init = ts.isPropertyAccessExpression(init.expression)
          ? init.expression.expression
          : undefined
      }
    }
    node.forEachChild(visitTable)
  }
  visitTable(source)
  const relations = new Map<string, Map<string, string>>()
  const readRelation = (node: ts.Node) => {
    if (
      ts.isCallExpression(node) &&
      ts.isIdentifier(node.expression) &&
      node.expression.text === 'relationships'
    ) {
      const [table, configure] = node.arguments
      const sourceTable = table && tables.get(table.getText(source))
      if (sourceTable && configure && ts.isArrowFunction(configure)) {
        const body = ts.isParenthesizedExpression(configure.body)
          ? configure.body.expression
          : configure.body
        if (ts.isObjectLiteralExpression(body)) {
          const edges = new Map<string, string>()
          for (const field of body.properties) {
            if (
              !ts.isPropertyAssignment(field) ||
              !ts.isCallExpression(field.initializer)
            )
              continue
            const [connection] = field.initializer.arguments
            if (!connection || !ts.isObjectLiteralExpression(connection)) continue
            for (const option of connection.properties) {
              if (
                !ts.isPropertyAssignment(option) ||
                option.name.getText(source) !== 'destSchema'
              )
                continue
              const target = tables.get(option.initializer.getText(source))
              if (target)
                edges.set(field.name.getText(source).replace(/^['"]|['"]$/g, ''), target)
            }
          }
          relations.set(sourceTable, edges)
        }
      }
    }
    node.forEachChild(readRelation)
  }
  readRelation(source)
  return relations
}

function tableColumns(
  project: NativeTypeScriptProject,
  paths: Iterable<string>,
  columns = new Map<string, Set<string>>()
): Map<string, Set<string>> {
  for (const path of paths) {
    const source = project.sourceFile(path)
    const visit = (node: ts.Node) => {
      if (
        !ts.isVariableDeclaration(node) ||
        !ts.isIdentifier(node.name) ||
        !node.initializer
      ) {
        node.forEachChild(visit)
        return
      }
      const tableNames = new Set([node.name.text])
      let foundColumns: ts.ObjectLiteralExpression | null = null
      const inspect = (candidate: ts.Node) => {
        if (!ts.isCallExpression(candidate)) {
          candidate.forEachChild(inspect)
          return
        }
        const isTable = /table/i.test(candidate.expression.getText(source))
        for (const argument of candidate.arguments) {
          if (isTable && ts.isStringLiteral(argument)) {
            tableNames.add(argument.text)
          }
          if (ts.isObjectLiteralExpression(argument)) foundColumns = argument
        }
        const columnBuilder = candidate.arguments[1]
        if (isTable && columnBuilder && ts.isArrowFunction(columnBuilder)) {
          const body = ts.isParenthesizedExpression(columnBuilder.body)
            ? columnBuilder.body.expression
            : columnBuilder.body
          if (ts.isObjectLiteralExpression(body)) foundColumns = body
        }
        inspect(candidate.expression)
      }
      inspect(node.initializer)
      if (foundColumns) {
        const names = new Set(
          (foundColumns as ts.ObjectLiteralExpression).properties
            .map((property) =>
              ts.isSpreadAssignment(property)
                ? null
                : property.name?.getText(source).replace(/^['"]|['"]$/g, '')
            )
            .filter((name): name is string => Boolean(name))
        )
        for (const tableName of tableNames) columns.set(tableName, names)
      }
      node.forEachChild(visit)
    }
    visit(source)
  }
  return columns
}

function queriedTables(
  project: NativeTypeScriptProject,
  namespace: DataNamespace,
  relations: Map<string, Map<string, string>>
): Array<{ table: string; query: string; root: boolean }> {
  if (!namespace.queryPath) return []
  const source = project.sourceFile(namespace.queryPath)
  const reached: Array<{ table: string; query: string; root: boolean }> = []
  const builderBindings = collectBuilderBindings(source)
  const functions = new Map<string, ts.ConciseBody>()
  const functionShadowed = new Map<string, Set<string>>()
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
          functionShadowed.set(
            declaration.name.text,
            collectShadowedParams(declaration.initializer)
          )
          if (isExported) exported.add(declaration.name.text)
        }
      }
    } else if (ts.isFunctionDeclaration(statement) && statement.name && statement.body) {
      functions.set(statement.name.text, statement.body)
      functionShadowed.set(statement.name.text, collectShadowedParams(statement))
      if (
        statement.modifiers?.some(
          (modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword
        )
      ) {
        exported.add(statement.name.text)
      }
    }
  }

  const rootTable = (
    node: ts.Node,
    initialShadowed: ReadonlySet<string>
  ): string | null => {
    let found: string | null = null
    const walk = (n: ts.Node, shadowed: ReadonlySet<string>) => {
      if (found) return
      const nextShadowed =
        ts.isArrowFunction(n) ||
        ts.isFunctionExpression(n) ||
        ts.isFunctionDeclaration(n) ||
        ts.isMethodDeclaration(n) ||
        ts.isBlock(n) ||
        ts.isCatchClause(n)
          ? extendScopeForNode(n, shadowed)
          : shadowed
      if (ts.isPropertyAccessExpression(n)) {
        if (
          ts.isIdentifier(n.expression) &&
          builderBindings.has(n.expression.text) &&
          !nextShadowed.has(n.expression.text)
        ) {
          found = n.name.text
          return
        }
        if (
          ts.isPropertyAccessExpression(n.expression) &&
          n.expression.name.text === 'query'
        ) {
          found = n.name.text
          return
        }
      }
      n.forEachChild((child) => walk(child, nextShadowed))
    }
    walk(node, initialShadowed)
    return found
  }

  const visiting = new Set<string>()
  const visit = (
    node: ts.Node,
    currentTable: string,
    query: string,
    shadowed: ReadonlySet<string>
  ) => {
    const nextShadowed =
      ts.isArrowFunction(node) ||
      ts.isFunctionExpression(node) ||
      ts.isFunctionDeclaration(node) ||
      ts.isMethodDeclaration(node) ||
      ts.isBlock(node) ||
      ts.isCatchClause(node)
        ? extendScopeForNode(node, shadowed)
        : shadowed
    if (
      ts.isCallExpression(node) &&
      ts.isPropertyAccessExpression(node.expression) &&
      node.expression.name.text === 'related'
    ) {
      const nameArg = node.arguments[0]
      if (!nameArg || !ts.isStringLiteral(nameArg)) {
        throw new Error(
          `[limesync] ${namespace.name}.${query} uses related() without a string literal; ` +
            `sync membership must be statically derivable`
        )
      }
      const sourceTable =
        rootTable(node.expression.expression, nextShadowed) ?? currentTable
      const target = relations.get(sourceTable)?.get(nameArg.text)
      if (!target) {
        throw new Error(
          `[limesync] ${namespace.name}.${query} related('${nameArg.text}') cannot be resolved ` +
            `from table '${sourceTable}' through relations.ts`
        )
      }
      reached.push({ table: target, query, root: false })
      visit(node.expression.expression, currentTable, query, nextShadowed)
      const callback = node.arguments[1]
      if (
        callback &&
        (ts.isArrowFunction(callback) || ts.isFunctionExpression(callback))
      ) {
        const cbShadowed = extendScopeForNode(callback, nextShadowed)
        visit(callback.body, target, query, cbShadowed)
      }
      return
    }
    if (ts.isCallExpression(node) && ts.isIdentifier(node.expression)) {
      const helper = functions.get(node.expression.text)
      const key = `${query}:${node.expression.text}`
      if (helper && !visiting.has(key)) {
        visiting.add(key)
        // helper traversal uses ONLY the helper's own lexical scope, never the caller's params
        const helperShadowed =
          functionShadowed.get(node.expression.text) ?? new Set<string>()
        const helperRoot = rootTable(helper, helperShadowed)
        if (helperRoot) reached.push({ table: helperRoot, query, root: true })
        visit(helper, helperRoot ?? currentTable, query, helperShadowed)
        visiting.delete(key)
      }
    }
    node.forEachChild((child) => visit(child, currentTable, query, nextShadowed))
  }

  for (const name of exported) {
    if (['mutate', 'schema', 'where'].includes(name)) continue
    const body = functions.get(name)!
    const queryShadowed = functionShadowed.get(name) ?? new Set<string>()
    const queryRoot = rootTable(body, queryShadowed)
    if (queryRoot) reached.push({ table: queryRoot, query: name, root: true })
    visit(body, queryRoot ?? namespace.name, name, queryShadowed)
  }
  return reached
}

function aggregateTables(
  project: NativeTypeScriptProject,
  namespace: DataNamespace
): string[] {
  if (!namespace.aggregatePath) return []
  const source = project.sourceFile(namespace.aggregatePath)
  const unwrap = (node: ts.Expression): ts.Expression => {
    let current = node
    while (
      ts.isSatisfiesExpression(current) ||
      ts.isAsExpression(current) ||
      ts.isParenthesizedExpression(current)
    ) {
      current = current.expression
    }
    return current
  }
  const tables = new Set<string>()
  for (const statement of source.statements) {
    if (!ts.isVariableStatement(statement)) continue
    if (
      !statement.modifiers?.some(
        (modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword
      )
    ) {
      continue
    }
    for (const declaration of statement.declarationList.declarations) {
      if (
        !ts.isIdentifier(declaration.name) ||
        declaration.name.text !== 'aggregates' ||
        !declaration.initializer
      ) {
        continue
      }
      const definitions = unwrap(declaration.initializer)
      if (!ts.isObjectLiteralExpression(definitions)) {
        throw new Error(
          `[limesync] ${namespace.aggregatePath} must export aggregates as an object literal`
        )
      }
      for (const property of definitions.properties) {
        if (!ts.isPropertyAssignment(property)) {
          throw new Error(
            `[limesync] ${namespace.aggregatePath} aggregate definitions must use explicit properties`
          )
        }
        const definition = unwrap(property.initializer)
        if (!ts.isObjectLiteralExpression(definition)) {
          throw new Error(
            `[limesync] ${namespace.aggregatePath} aggregate ${property.name.getText(source)} must be an object literal`
          )
        }
        for (const fieldName of ['source', 'target']) {
          const field = definition.properties.find(
            (candidate): candidate is ts.PropertyAssignment =>
              ts.isPropertyAssignment(candidate) &&
              propertyName(candidate.name) === fieldName
          )
          if (!field || !ts.isStringLiteral(unwrap(field.initializer))) {
            throw new Error(
              `[limesync] ${namespace.aggregatePath} aggregate ${property.name.getText(source)} ${fieldName} must be a string literal`
            )
          }
          tables.add((unwrap(field.initializer) as ts.StringLiteral).text)
        }
      }
    }
  }
  return [...tables].sort()
}

function mutationSupportTables(
  project: NativeTypeScriptProject,
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
    const source = project.sourceFile(path)

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

      node.forEachChild(visit)
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
          `[limesync] ${path} uses removed instance.ts configuration; delete it and configure instances in data.config.ts`
        )
      }
    }
  }
  for (const root of roots) walk(root)
}

function assertNoUnclaimedNamespaces(
  project: NativeTypeScriptProject,
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
      const kinds = namespaceExportKinds(project, baseDir, path)
      if (kinds.model || kinds.query) {
        throw new Error(
          `[limesync] data namespace ${path} is outside every instance directory declared in ${configPath}`
        )
      }
    }
  }
  walk(baseDir)
}

export function discoverDataLayout(
  project: NativeTypeScriptProject,
  baseDir: string,
  configPath?: string,
  oneSchema?: string
): DataLayout {
  const config = readDataConfig(project, baseDir, configPath)
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
      project,
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
    tables: [],
    syncTables: [],
    supportTables: [],
    declaredSupportTables: instance.supportTables,
  }))
  const instanceDirs = new Set(instances.map((instance) => instance.dir))
  const metadata = oneSchema ? [oneSchema] : metadataPaths(baseDir)
  const columns = tableColumns(project, metadata)

  for (const instance of instances) {
    instance.namespaces = discoverNamespaces(
      project,
      baseDir,
      instance,
      instanceDirs,
      columns
    )
  }
  const namespaces = instances.flatMap((instance) => instance.namespaces)
  const namespaceOwners = new Map<string, string>()
  for (const namespace of namespaces) {
    const owner = namespaceOwners.get(namespace.name)
    if (owner) {
      throw new Error(
        `[limesync] namespace '${namespace.name}' is claimed by instances '${owner}' and '${namespace.instance}'`
      )
    }
    namespaceOwners.set(namespace.name, namespace.instance)
  }
  const tableOwners = new Map<string, string>()
  for (const namespace of namespaces) {
    if (namespace.table === null) continue
    const owner = tableOwners.get(namespace.table)
    if (owner && owner !== namespace.instance) {
      throw new Error(
        `[limesync] table '${namespace.table}' is claimed by instances '${owner}' and '${namespace.instance}'`
      )
    }
    tableOwners.set(namespace.table, namespace.instance)
  }

  const relations = oneSchema
    ? oneRelationTargets(project.sourceFile(oneSchema))
    : relationTargets(project, metadata)
  const localTables = oneSchema
    ? oneLocalTables(project.sourceFile(oneSchema))
    : new Set<string>()
  const localQueries = new Set<string>()
  tableColumns(
    project,
    new Set(namespaces.flatMap((namespace) => namespace.sourcePaths)),
    columns
  )
  const relatedOwners = new Map<string, string>()
  for (const instance of instances) {
    const tables = new Set(
      instance.namespaces
        .map((namespace) => namespace.table)
        .filter((table): table is string => table !== null && !localTables.has(table))
    )
    const syncTables = new Set(tables)
    for (const namespace of instance.namespaces) {
      for (const table of aggregateTables(project, namespace)) {
        const owner = tableOwners.get(table) ?? relatedOwners.get(table)
        if (owner && owner !== instance.name) {
          throw new Error(
            `[limesync] ${namespace.name} aggregates in instance '${instance.name}' reach ` +
              `table '${table}' owned by instance '${owner}'`
          )
        }
        relatedOwners.set(table, instance.name)
        tables.add(table)
        syncTables.add(table)
      }
      const reachedEntries = queriedTables(project, namespace, relations)
      const byQuery = new Map<string, typeof reachedEntries>()
      for (const entry of reachedEntries) {
        const list = byQuery.get(entry.query) ?? []
        list.push(entry)
        byQuery.set(entry.query, list)
      }
      for (const [queryName, entries] of byQuery) {
        const hasLocal = entries.some((e) => localTables.has(e.table))
        const hasSync = entries.some((e) => !localTables.has(e.table))
        if (hasLocal && hasSync) {
          throw new Error(
            `[limesync] ${namespace.name}.${queryName} mixes local and synced tables ` +
              `([${[...new Set(entries.map((e) => e.table))].join(', ')}]), which is not supported in v1.`
          )
        }
        if (hasLocal) {
          // named local queries stay in the client registry only, never syncTables or server registry
          localQueries.add(`${namespace.name}.${queryName}`)
          continue
        }
        for (const reached of entries) {
          const owner = tableOwners.get(reached.table) ?? relatedOwners.get(reached.table)
          if (owner && owner !== instance.name) {
            throw new Error(
              `[limesync] ${namespace.name}.${reached.query} in instance '${instance.name}' reaches ` +
                `table '${reached.table}' owned by instance '${owner}'`
            )
          }
          relatedOwners.set(reached.table, instance.name)
          if (reached.root) tables.add(reached.table)
          syncTables.add(reached.table)
        }
      }
    }
    instance.tables = [...tables].sort()
    instance.syncTables = [...syncTables].sort()
    if (instance.scope) {
      for (const table of instance.syncTables) {
        if (!columns.get(table)?.has(instance.scope)) {
          throw new Error(
            `[limesync] table '${table}' in instance '${instance.name}' is missing scope column '${instance.scope}'`
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
      for (const table of mutationSupportTables(
        project,
        baseDir,
        sourceRoots,
        namespace
      )) {
        if (tableOwners.has(table) || relatedOwners.has(table)) continue
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
    localQueries,
  }
}

export function namespaceImportPath(baseDir: string, path: string): string {
  return `../${toImportPath(baseDir, path)}`
}
