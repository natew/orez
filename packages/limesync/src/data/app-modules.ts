import { randomUUID } from 'node:crypto'
import { statSync } from 'node:fs'
import { mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises'
import { isAbsolute, relative, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

import { init, parse } from 'es-module-lexer'

import { generateModules } from './generate.js'

export type DataAppOptions = {
  dir?: string
  schema?: string
  tsconfig?: string
}

export async function generateAppModules(root: string, options: DataAppOptions) {
  const dir = resolve(root, options.dir ?? 'data')
  const tsconfig = options.tsconfig && resolve(root, options.tsconfig)
  const schema = resolve(root, options.schema ?? resolve(dir, 'schema.ts'))
  const generated = await generateModules({ dir, tsconfig })
  const { files, watchFiles, sourceRoots } = generated
  if (statSync(schema, { throwIfNoEntry: false })?.isFile()) {
    files['schema.ts'] = `import * as source from ${JSON.stringify(schema)}
export const schema = source.schema
`
    watchFiles.push(schema)
  } else if (options.schema || !files['schema.ts']) {
    throw new Error(`missing limesync schema: ${schema}`)
  }
  if (tsconfig) watchFiles.push(tsconfig)
  const aggregates = files['aggregates.ts']
    ? 'import { aggregates } from "./aggregates"\n'
    : ''
  files['app.ts'] = `import { createDataApp } from 'limesync/data'
import { schema } from './schema'
import { models } from './models'
import * as groupedQueries from './groupedQueries'
${aggregates}export const app = createDataApp({ schema, models, groupedQueries${aggregates ? ', aggregates' : ''} })
export { schema, models, groupedQueries }
export { instances } from './instances'
`
  const validators = files['syncedMutations.ts']
    ? 'import { mutationValidators } from "./syncedMutations"\n'
    : 'const mutationValidators = {}\n'
  files['app-server.ts'] = `import { schema } from './schema'
import { models } from './models'
import { queries } from './syncedQueries'
${validators}export const app = { schema, models, queries, mutationValidators }
export { queries, mutationValidators }
`
  return { files, watchFiles: [...new Set(watchFiles)], sourceRoots }
}

export async function renderAppModule(
  file: string,
  dir: string,
  files: Record<string, string>,
  siblingImport: (file: string) => string,
  sourceImport: (file: string) => string = (path) => path
) {
  const original = Object.hasOwn(files, file) ? files[file] : undefined
  if (original === undefined) throw new Error(`unknown limesync module ${file}`)
  await init
  const [imports] = parse(original)
  let contents = original
  for (const { n, ss, se, s, e, d } of [...imports].reverse()) {
    if (d !== -1 || !n) continue
    let specifier: string
    if (n === 'valibot') {
      specifier = sourceImport(fileURLToPath(import.meta.resolve('valibot')))
    } else if (n.startsWith('.')) {
      const sibling = `${n.slice(2)}.ts`
      specifier =
        n.startsWith('./') && Object.hasOwn(files, sibling)
          ? siblingImport(sibling)
          : sourceImport(resolve(dir, 'generated', n + '.ts'))
    } else if (isAbsolute(n)) {
      specifier = sourceImport(n)
    } else {
      continue
    }
    const quoteOffset = s - 1
    if (quoteOffset < ss || e >= se)
      throw new Error(`invalid generated import in ${file}`)
    contents =
      contents.slice(0, quoteOffset) + JSON.stringify(specifier) + contents.slice(e + 1)
  }
  return contents
}

export async function prepareDataApp(root: string, options: DataAppOptions) {
  const dir = resolve(root, options.dir ?? 'data')
  const cache = resolve(root, 'node_modules/.limesync-data/app')
  const { files } = await generateAppModules(root, options)
  const sourceImport = (path: string) => {
    const offset = relative(cache, path).split(sep).join('/')
    return offset.startsWith('../') ? offset : './' + offset
  }
  const modules = await Promise.all(
    Object.keys(files).map(async (file) => ({
      file,
      contents: await renderAppModule(
        file,
        dir,
        files,
        (sibling) => './' + sibling.slice(0, -3),
        sourceImport
      ),
    }))
  )
  await mkdir(cache, { recursive: true })
  const scratch = resolve(cache, randomUUID())
  await mkdir(scratch)
  try {
    for (const { file, contents } of modules) {
      const path = resolve(cache, file)
      if (
        statSync(path, { throwIfNoEntry: false })?.isFile() &&
        (await readFile(path, 'utf8')) === contents
      ) {
        continue
      }
      const staged = resolve(scratch, file)
      await writeFile(staged, contents)
      await rename(staged, path)
    }
  } finally {
    await rm(scratch, { recursive: true })
  }
}
