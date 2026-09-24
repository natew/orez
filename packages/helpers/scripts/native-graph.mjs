// tsc emits one module graph, and its explicit `.js` specifiers never resolve to a
// `.native.js` sibling, so the react-native entry would still import the web
// variants (node:async_hooks, navigator.clipboard, IS_WEB = true). this writes the
// native graph: every module that reaches a `.native` source gets a `.native.js`
// copy whose relative imports point at the native variants.
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'

const dist = resolve(import.meta.dirname, '../dist')
const specifier = /((?:from|import)\s*\(?\s*['"])(\.\.?\/[^'"]+?)\.js(['"])/g

function jsFiles(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) return jsFiles(path)
    return entry.name.endsWith('.js') ? [path] : []
  })
}

const files = jsFiles(dist)
const webFiles = files.filter((file) => !file.endsWith('.native.js'))
const nativeOf = (file) => file.replace(/\.js$/, '.native.js')
const imports = (file) =>
  [...readFileSync(file, 'utf8').matchAll(specifier)].map((match) =>
    resolve(dirname(file), `${match[2]}.js`)
  )

// a web module needs a native twin when it has a native source or imports one
const native = new Set(webFiles.filter((file) => existsSync(nativeOf(file))))
let grew = true
while (grew) {
  grew = false
  for (const file of webFiles) {
    if (!native.has(file) && imports(file).some((dep) => native.has(dep))) {
      native.add(file)
      grew = true
    }
  }
}

const rewrite = (file) =>
  readFileSync(file, 'utf8').replace(specifier, (whole, head, path, tail) =>
    native.has(resolve(dirname(file), `${path}.js`))
      ? `${head}${path}.native.js${tail}`
      : whole
  )

// a native source keeps its own body and only has its imports redirected
for (const file of native) {
  const target = nativeOf(file)
  writeFileSync(target, rewrite(existsSync(target) ? target : file))
}
