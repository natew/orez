import {
  existsSync,
  readFileSync,
  readdirSync,
  realpathSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'fs'
import { dirname, isAbsolute, join, relative, resolve, sep } from 'path'

const WORKER_MODULE_EXTENSION = /\.(?:cjs|js|mjs|wasm)$/
const WORKER_SOURCE_MODULE_EXTENSION = /\.(?:cjs|js|mjs)$/

function listWorkerModuleFiles(dir: string, files: string[] = []): string[] {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.name.startsWith('.')) continue
    const file = join(dir, entry.name)
    if (entry.isDirectory()) {
      listWorkerModuleFiles(file, files)
    } else if (WORKER_MODULE_EXTENSION.test(entry.name)) {
      files.push(resolve(file))
    }
  }
  return files
}

function staticWorkerModuleReferences(source: string): string[] {
  const references: string[] = []
  const staticRefRe =
    /\b(?:import\s+(?:[^;]*?\s+from\s+)?|export\s+[^;]*?\s+from\s+)["'](\.\.?\/[^"']+\.(?:cjs|js|mjs|wasm))["']/g
  let match: RegExpExecArray | null
  while ((match = staticRefRe.exec(source))) references.push(match[1])
  return references
}

export function assertWorkerStaticModuleImportsResolve(workerDir: string): void {
  const moduleFiles = listWorkerModuleFiles(workerDir)
  const moduleSet = new Set(moduleFiles)
  const missing: Array<{ importer: string; specifier: string }> = []
  for (const importer of moduleFiles) {
    if (!WORKER_SOURCE_MODULE_EXTENSION.test(importer)) continue
    let source: string
    try {
      source = readFileSync(importer, 'utf-8')
    } catch {
      continue
    }
    for (const specifier of staticWorkerModuleReferences(source)) {
      if (moduleSet.has(resolve(dirname(importer), specifier))) continue
      missing.push({ importer: relative(workerDir, importer), specifier })
    }
  }
  if (missing.length === 0) return
  throw new Error(
    `worker modules contain unresolved static imports:\n${missing
      .map(({ importer, specifier }) => `  ${importer} imports ${specifier}`)
      .join('\n')}`
  )
}

// remove asset chunks not reachable (static OR dynamic) from the worker entry.
export function pruneUnreachableWorkerModules(
  workerDir: string,
  entryName: string
): void {
  const assetsDir = join(workerDir, 'assets')
  if (!existsSync(assetsDir)) return
  const reachable = new Set<string>()
  const canon = (p: string) => {
    try {
      return realpathSync(p)
    } catch {
      return p
    }
  }
  const visit = (file: string) => {
    const f = canon(file)
    if (reachable.has(f)) return
    reachable.add(f)
    let src: string
    try {
      src = readFileSync(f, 'utf-8')
    } catch {
      return
    }
    // match ANY relative module ref in import/export/import(). a FRESH regex per
    // call — a shared /g regex's lastIndex is clobbered by the recursive visits.
    const refRe = /["'](\.\.?\/[^"']+\.(?:js|mjs|wasm))["']/g
    const refs: string[] = []
    let m: RegExpExecArray | null
    while ((m = refRe.exec(src))) refs.push(m[1])
    for (const ref of refs) visit(join(dirname(f), ref))
  }
  const entryPath = join(workerDir, entryName)
  visit(entryPath)
  const allAssets = readdirSync(assetsDir).filter(
    (n) => n.endsWith('.js') || n.endsWith('.mjs')
  )
  const reachableAssets = allAssets.filter((n) =>
    reachable.has(canon(join(assetsDir, n)))
  )
  // SAFETY: if the trace found (almost) nothing reachable, the entry/ref
  // resolution is broken — do NOT prune (pruning everything bricks the worker).
  if (reachableAssets.length < allAssets.length * 0.05) {
    // eslint-disable-next-line no-console
    console.log(
      `[cloudflare] skip module-prune: only ${reachableAssets.length}/${allAssets.length} assets reached from ${entryName} (trace looks broken)`
    )
    return
  }
  let removed = 0
  for (const name of allAssets) {
    if (!reachable.has(canon(join(assetsDir, name)))) {
      rmSync(join(assetsDir, name), { force: true })
      removed++
    }
  }
  if (removed > 0) {
    // eslint-disable-next-line no-console
    console.log(`[cloudflare] pruned ${removed} unreachable asset modules`)
  }
}

export interface WorkerChunkPruneSignatures {
  browserOnlyChunkSignature: RegExp
  serverNodeOnlyChunkSignatures: readonly string[]
}

// remove async chunks matching caller-owned signatures from dist/worker/assets
// before wrangler attaches modules. returns the count + bytes pruned for the
// deploy log.
export function pruneWorkerChunksBySignature(
  workerDir: string,
  signatures: WorkerChunkPruneSignatures
): {
  removed: number
  bytes: number
} {
  const assetsDir = join(workerDir, 'assets')
  if (!existsSync(assetsDir)) {
    assertWorkerStaticModuleImportsResolve(workerDir)
    return { removed: 0, bytes: 0 }
  }
  const candidates: Array<{
    file: string
    reason: string
  }> = []
  for (const name of readdirSync(assetsDir)) {
    if (!name.endsWith('.js') && !name.endsWith('.mjs')) continue
    const file = join(assetsDir, name)
    let head: string
    try {
      // signatures appear in the module-eval body; reading the whole chunk is
      // fine — these are at most a few MiB and we only do it once per deploy.
      head = readFileSync(file, 'utf-8')
    } catch {
      continue
    }
    signatures.browserOnlyChunkSignature.lastIndex = 0
    const browserOnly = signatures.browserOnlyChunkSignature.test(head)
    signatures.browserOnlyChunkSignature.lastIndex = 0
    const serverNodeOnly = signatures.serverNodeOnlyChunkSignatures.find((sig) =>
      head.includes(sig)
    )
    if (!browserOnly && !serverNodeOnly) continue
    candidates.push({
      file,
      reason: browserOnly
        ? `browser-only signature ${signatures.browserOnlyChunkSignature}`
        : `server-only signature ${JSON.stringify(serverNodeOnly)}`,
    })
  }

  // signatures classify whole chunks from content. bundlers may co-locate a
  // shared module with a matching string, so remove asset-level static
  // dependents with the candidate rather than retaining modules that can never
  // evaluate. a dependency chain that reaches a worker root rejects the whole
  // prune before any file changes, because that code is part of the worker's
  // eager module graph.
  const deletion = new Map<
    string,
    { candidate: (typeof candidates)[number]; chain: string[] }
  >(
    candidates.map((candidate) => [
      resolve(candidate.file),
      { candidate, chain: [relative(workerDir, candidate.file)] },
    ])
  )
  const staticImporters = new Map<string, string[]>()
  for (const file of listWorkerModuleFiles(workerDir)) {
    if (!WORKER_SOURCE_MODULE_EXTENSION.test(file)) continue
    try {
      const source = readFileSync(file, 'utf-8')
      for (const specifier of staticWorkerModuleReferences(source)) {
        const dependency = resolve(dirname(file), specifier)
        const importers = staticImporters.get(dependency)
        if (importers) importers.push(file)
        else staticImporters.set(dependency, [file])
      }
    } catch {}
  }
  const deletionQueue = [...deletion.keys()]
  for (let index = 0; index < deletionQueue.length; index++) {
    const dependencyFile = deletionQueue[index]
    const dependency = deletion.get(dependencyFile)!
    for (const importer of staticImporters.get(dependencyFile) ?? []) {
      if (deletion.has(importer)) continue
      const importerRelative = relative(workerDir, importer)
      const assetRelative = relative(resolve(assetsDir), importer)
      const isAsset =
        !isAbsolute(assetRelative) &&
        assetRelative !== '..' &&
        !assetRelative.startsWith(`..${sep}`)
      if (!isAsset) {
        throw new Error(
          `refusing to prune ${relative(workerDir, dependency.candidate.file)} (${dependency.candidate.reason}): static dependency chain ${[...dependency.chain, importerRelative].join(' <- ')}`
        )
      }
      deletion.set(importer, {
        candidate: dependency.candidate,
        chain: [...dependency.chain, importerRelative],
      })
      deletionQueue.push(importer)
    }
  }

  let bytes = 0
  for (const file of deletion.keys()) {
    bytes += statSync(file).size
    rmSync(file, { force: true })
  }
  const removed = deletion.size
  // removing the browser-only chunks orphans every chunk that was reachable
  // ONLY through them (e.g. the ~466 shiki/textmate-grammar/wasm chunks
  // pulled in solely by the codemirror+lsp editor surface — 2.6 MiB gz).
  // The initial reachability prune ran during app-worker bundling before
  // these deletions, so it counted them as live; re-run it now to drop the
  // newly-unreachable set. without this the worker ships dead grammar chunks
  // and trips CF's 10 MiB code limit.
  if (removed > 0) pruneUnreachableWorkerModules(workerDir, 'index.js')
  assertWorkerStaticModuleImportsResolve(workerDir)
  return { removed, bytes }
}

// true when the worker serves NO page routes — run_worker_first lists only
// /api and /sync paths, so the ASSETS binding serves every (prebuilt SPA/SSG)
// page directly and the worker's fetch never renders one. this is the ground
// truth: an ssr route or a route with a loader MUST appear in run_worker_first
// to function (otherwise ASSETS short-circuits to a stale static shell and the
// worker never runs), so an api/sync-only list proves the worker renders
// nothing. for any app that does render pages this returns false and the strip
// below is a no-op.
export function workerServesNoPageRoutes(workerDir: string): boolean {
  try {
    const wrangler = JSON.parse(
      readFileSync(join(workerDir, 'wrangler.json'), 'utf-8')
    ) as { assets?: { run_worker_first?: unknown } }
    const rwf = wrangler.assets?.run_worker_first
    if (!Array.isArray(rwf) || rwf.length === 0) return false
    return rwf.every(
      (p) =>
        typeof p === 'string' && (/^\/api(\/|\b)/.test(p) || /^\/sync(\/|\b)/.test(p))
    )
  } catch {
    return false
  }
}

// total bytes of assets/*.{js,mjs} — a proxy for resident module weight, since
// no_bundle + the `assets/**/*.js` rule makes workerd attach every one as a
// resident ESModule in the 128 MiB DO isolate.
export function workerAssetsBytes(workerDir: string): number {
  const assetsDir = join(workerDir, 'assets')
  if (!existsSync(assetsDir)) return 0
  let bytes = 0
  for (const n of readdirSync(assetsDir)) {
    if (!n.endsWith('.js') && !n.endsWith('.mjs')) continue
    try {
      bytes += statSync(join(assetsDir, n)).size
    } catch {
      // ignore a file that vanished mid-scan
    }
  }
  return bytes
}

// neutralize one-app.js's lazy serverEntry + page-component imports so the
// reachability prune can drop the resident client graph (router ~1.4 MiB,
// zero-client ~777 KiB, every page chunk + transitive tamagui UI) the worker
// never executes. ONLY safe when workerServesNoPageRoutes is true. the api{}
// imports (push/pull/auth — these DO run on the worker) are deliberately left
// intact: the rewrite is scoped to the serverEntry line + the pages{} block,
// which the api{} block follows and bounds. returns true if it rewrote.
export function stripDeadPageRouteImports(workerDir: string): boolean {
  const files = readdirSync(workerDir).filter((n) => /^one-app-.*\.js$/.test(n))
  if (files.length !== 1) {
    // eslint-disable-next-line no-console
    console.log(
      `[cloudflare] skip page-import strip: expected 1 one-app-*.js, found ${files.length}`
    )
    return false
  }
  const file = join(workerDir, files[0])
  const orig = readFileSync(file, 'utf-8')
  // a thunk that, if EVER called (it never is for an api/sync-only worker),
  // fails loudly at the call site instead of serving a blank page — so a future
  // app that slips past the gate surfaces in the deploy's runtime validation.
  const DEAD =
    '() => Promise.reject(new Error("page route not served by this worker (run_worker_first is api/sync only); chunk stripped to free DO isolate memory"))'
  // esbuild emits double-quoted specifiers: import("./assets/<chunk>.js").
  const thunk = /\(\)\s*=>\s*import\("\.\/assets\/[^"]+"\)/g
  // serverEntry: a single line preceding the pages block.
  let src = orig.replace(
    /serverEntry:\s*\(\)\s*=>\s*import\("\.\/assets\/[^"]+"\)/,
    `serverEntry: ${DEAD}`
  )
  // the pages:{...} block only, bounded by the api:{...} block that follows it.
  const pagesStart = src.indexOf('pages:')
  const apiStart = pagesStart === -1 ? -1 : src.indexOf('api:', pagesStart)
  if (pagesStart !== -1 && apiStart > pagesStart) {
    const block = src.slice(pagesStart, apiStart).replace(thunk, DEAD)
    src = src.slice(0, pagesStart) + block + src.slice(apiStart)
  }
  if (src === orig) {
    // eslint-disable-next-line no-console
    console.log('[cloudflare] skip page-import strip: no lazy page imports matched')
    return false
  }
  writeFileSync(file, src)
  return true
}

// workerd does NOT populate `import.meta.url` for attached ESModule worker
// modules — it evaluates to `undefined` (verified at runtime against compat-date
// 2026-03-29 + nodejs_compat). rolldown/esbuild emit a CJS-interop banner
// `var __require = createRequire(import.meta.url)` at the top of any chunk that
// bundles a CommonJS dependency (e.g. react.production.js, pulled into the SSR
// render graph). with `import.meta.url` undefined, that top-level call throws
// `ERR_INVALID_ARG_VALUE` the instant the chunk is evaluated.
//
// it only surfaces on `+ssr` routes: SPA/SSG routes serve prebuilt html and
// never evaluate the server render graph in the worker, so the very first
// request-time render (a `+ssr` page) is what trips it. One's handlePage
// swallows the throw and returns an empty 200, so every `+ssr` route renders a
// blank body and deploy validation (which only hits prebuilt routes) stays
// green — exactly how this shipped unnoticed.
//
// shim it the same way `__filename`/`__dirname` are shimmed in the esbuild
// `define` above, but reach the external `assets/*` chunks too (they're marked
// external to esbuild, so the `define` never touches them): give createRequire a
// valid base URL. `import.meta.url` is already undefined at runtime, so nothing
// can depend on its real value — replacing it is strictly safe. matches the
// literal rolldown/esbuild banner only; the aliased form inside the sub-worker
// banner *template string* (`createRequire as CREATE_REQUIRE_NAME`) is left
// intact, since that template is injected into node sub-workers where
// `import.meta.url` is real.
export function shimWorkerCreateRequire(workerDir: string): void {
  const re = /createRequire\(\s*import\.meta\.url\s*\)/g
  let patched = 0
  const visit = (dir: string) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      // skip dot-dirs (.vite/.orez/.do-bundle) and node_modules — only emitted
      // worker modules (root entry + chunks + assets) carry the banner.
      if (entry.name.startsWith('.') || entry.name === 'node_modules') continue
      const p = join(dir, entry.name)
      if (entry.isDirectory()) {
        visit(p)
        continue
      }
      if (!p.endsWith('.js') && !p.endsWith('.mjs')) continue
      const src = readFileSync(p, 'utf-8')
      const out = src.replace(re, 'createRequire("file:///index.js")')
      if (out !== src) {
        writeFileSync(p, out)
        patched++
      }
    }
  }
  visit(workerDir)
  if (patched > 0) {
    console.log(
      `[cloudflare] shimmed createRequire(import.meta.url) in ${patched} worker module(s)`
    )
  }
}
