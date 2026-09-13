import { existsSync, readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'

import { parse } from '@babel/parser'
import { API, type Project } from 'typescript/unstable/async'

import type { SourceFile } from 'typescript/unstable/ast'

process.env.GOMAXPROCS ??= '2'

export type NativeTypeScriptProject = {
  projectForFile(path: string): Project
  sourceFile(path: string): SourceFile
  hasParseErrors(path: string): boolean
  close(): Promise<void>
}

type CompilerSession = {
  api: API
  paths: Set<string>
  root: string
}

let compiler: CompilerSession | null = null
let compilerClose: Promise<void> | null = null
let compilerCloseTimer: ReturnType<typeof setTimeout> | null = null
let compilerTurn = Promise.resolve()

async function closeCompiler(session: CompilerSession) {
  try {
    if (session.paths.size > 0) {
      const snapshot = await session.api.updateSnapshot({
        closeFiles: [...session.paths],
      })
      await snapshot.dispose()
    }
  } finally {
    await session.api.close()
  }
}

async function acquireCompiler(root: string, sourcePaths: string[]) {
  const previousTurn = compilerTurn
  let releaseTurn = () => {}
  compilerTurn = new Promise<void>((resolveTurn) => {
    releaseTurn = resolveTurn
  })
  await previousTurn
  if (compilerCloseTimer) {
    clearTimeout(compilerCloseTimer)
    compilerCloseTimer = null
  }
  if (compilerClose) await compilerClose
  if (compiler && compiler.root !== root) {
    const previous = compiler
    compiler = null
    await closeCompiler(previous)
  }
  compiler ??= { api: new API({ cwd: root }), paths: new Set(), root }
  compiler.api.clearSourceFileCache()
  const previousPaths = compiler.paths
  const nextPaths = new Set(sourcePaths)
  compiler.paths = nextPaths
  if (previousPaths.size > 0) {
    const closedSnapshot = await compiler.api.updateSnapshot({
      closeFiles: [...previousPaths],
      fileChanges: { invalidateAll: true },
    })
    await closedSnapshot.dispose()
    compiler.api.clearSourceFileCache()
  }
  const snapshot = await compiler.api.updateSnapshot({
    openFiles: sourcePaths,
    fileChanges: { invalidateAll: true },
  })
  compiler.api.clearSourceFileCache()
  return { session: compiler, snapshot, releaseTurn }
}

function releaseCompiler(session: CompilerSession, releaseTurn: () => void) {
  releaseTurn()
  compilerCloseTimer = setTimeout(() => {
    compilerCloseTimer = null
    if (compiler !== session) return
    compiler = null
    compilerClose = closeCompiler(session).finally(() => {
      compilerClose = null
    })
  }, 25)
  compilerCloseTimer.unref?.()
}

export function collectTypeScriptSourcePaths(roots: string[]): string[] {
  const paths = new Set<string>()
  const visited = new Set<string>()
  const walk = (dir: string) => {
    if (visited.has(dir) || !existsSync(dir)) return
    visited.add(dir)
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (entry.name === 'node_modules') continue
      const path = resolve(dir, entry.name)
      if (entry.isDirectory()) {
        walk(path)
      } else if (
        entry.isFile() &&
        (entry.name.endsWith('.ts') || entry.name.endsWith('.tsx'))
      ) {
        paths.add(path)
      }
    }
  }
  for (const root of roots) walk(resolve(root))
  return [...paths].sort()
}

export async function createNativeTypeScriptProject(
  root: string,
  sourcePaths: string[]
): Promise<NativeTypeScriptProject> {
  if (sourcePaths.length === 0) {
    throw new Error(`[on-zero] no TypeScript source files found under ${root}`)
  }

  const { session, snapshot, releaseTurn } = await acquireCompiler(root, sourcePaths)
  const sources = new Map<string, SourceFile>()
  const projectsByFile = new Map<string, Project>()
  const parseErrors = new Set<string>()

  try {
    for (const path of sourcePaths) {
      const project = await snapshot.getDefaultProjectForFile(path)
      if (!project) throw new Error(`[on-zero] TypeScript did not open ${path}`)
      const source = await project.program.getSourceFile(path)
      if (!source) throw new Error(`[on-zero] TypeScript did not parse ${path}`)
      sources.set(path, source)
      projectsByFile.set(path, project)
      try {
        parse(readFileSync(path, 'utf8'), {
          plugins: ['jsx', 'typescript'],
          sourceType: 'module',
        })
      } catch {
        parseErrors.add(path)
      }
    }
  } catch (error) {
    await snapshot.dispose()
    releaseCompiler(session, releaseTurn)
    throw error
  }

  let closed = false
  return {
    projectForFile(path) {
      const project = projectsByFile.get(resolve(path))
      if (!project)
        throw new Error(`[on-zero] TypeScript project does not contain ${path}`)
      return project
    },
    sourceFile(path) {
      const source = sources.get(resolve(path))
      if (!source)
        throw new Error(`[on-zero] TypeScript project does not contain ${path}`)
      return source
    },
    hasParseErrors(path) {
      return parseErrors.has(resolve(path))
    },
    async close() {
      if (closed) return
      closed = true
      await snapshot.dispose()
      releaseCompiler(session, releaseTurn)
    },
  }
}
