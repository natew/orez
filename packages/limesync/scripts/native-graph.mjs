// tsc emits one module graph, and its explicit `.js` specifiers never resolve to
// platform-conditioned siblings (`.native.js`, `.one.js`, `.bun.js`), so conditioned
// entry points would otherwise import the default web graph. this script writes each
// conditioned graph: every module that reaches a conditioned source gets a twin whose
// relative imports point at the matching condition variants.
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'

const dist = resolve(import.meta.dirname, '../dist')
const specifier = /((?:from|import)\s*\(?\s*['"])(\.\.?\/[^'"]+?)\.js(['"])/g
const variantSuffixes = ['.native.js', '.one.js', '.bun.js']

function jsFiles(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) return jsFiles(path)
    return entry.name.endsWith('.js') ? [path] : []
  })
}

const allFiles = jsFiles(dist)
const baseFiles = allFiles.filter(
  (file) => !variantSuffixes.some((suffix) => file.endsWith(suffix))
)
const imports = (file) =>
  [...readFileSync(file, 'utf8').matchAll(specifier)].map((match) =>
    resolve(dirname(file), `${match[2]}.js`)
  )

function buildGraph(targetSuffix, fallbackSuffixes = []) {
  const variantOf = (file, suffix = targetSuffix) => file.replace(/\.js$/, suffix)
  const resolveSeed = (file) => {
    if (existsSync(variantOf(file, targetSuffix))) return variantOf(file, targetSuffix)
    for (const fallback of fallbackSuffixes) {
      if (existsSync(variantOf(file, fallback))) return variantOf(file, fallback)
    }
    return null
  }

  const members = new Set(baseFiles.filter((file) => resolveSeed(file) !== null))
  let grew = true
  while (grew) {
    grew = false
    for (const file of baseFiles) {
      if (!members.has(file) && imports(file).some((dep) => members.has(dep))) {
        members.add(file)
        grew = true
      }
    }
  }

  const rewrite = (sourceFile) =>
    readFileSync(sourceFile, 'utf8').replace(specifier, (whole, head, path, tail) => {
      const targetBase = resolve(dirname(sourceFile), `${path}.js`)
      return members.has(targetBase) ? `${head}${path}${targetSuffix}${tail}` : whole
    })

  for (const file of members) {
    const source = resolveSeed(file) ?? file
    writeFileSync(variantOf(file), rewrite(source))
  }
}

buildGraph('.native.js')
// one-native inherits react-native modules (e.g. no node:async_hooks) and overrides
// with `.one.js` sources (One.Database SQLite local persistence & kvStore)
buildGraph('.one.js', ['.native.js'])
buildGraph('.bun.js')
