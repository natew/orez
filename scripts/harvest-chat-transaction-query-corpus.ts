import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { parseArgs } from 'node:util'

const CHAT_COMMIT = 'cc2d26fa24a88161231f3337c0e0cae9d43ae2d1'
const EXPECTED_CASES = 252
const EXPECTED_QUERIES = 123

const args = parseArgs({
  options: {
    chat: { type: 'string' },
    out: { type: 'string' },
  },
  strict: true,
}).values

const chat = resolve(args.chat ?? '../chat')
const output = resolve(args.out ?? 'harness/corpus/chat-transaction-query-v1.json')
const revision = execFileSync('git', ['rev-parse', 'HEAD'], {
  cwd: chat,
  encoding: 'utf8',
}).trim()
if (revision !== CHAT_COMMIT) {
  throw new Error(`chat checkout must be ${CHAT_COMMIT}, got ${revision}`)
}

const fromChat = (path: string) => pathToFileURL(resolve(chat, path)).href
await import(fromChat('src/zero/zeroServer.ts'))
const { setAuthData } = (await import(
  fromChat('node_modules/on-zero/dist/esm/index.mjs')
)) as { setAuthData(auth: { id: string; email: string }): void }
const { asQueryInternals } = (await import(
  fromChat('node_modules/@rocicorp/zero/out/zero/src/bindings.js')
)) as {
  asQueryInternals(query: unknown): { ast: unknown; format: unknown }
}
const groupedQueries = (await import(
  fromChat('src/data/generated/groupedQueries.ts')
)) as Record<string, Record<string, (args: Record<string, unknown>) => unknown>>
const fixture = (await import(fromChat('rust-sync/chat-config/corpus-fixture.ts'))) as {
  CORPUS_CASES: Array<{
    ns: string
    name: string
    args: Record<string, unknown>
    user: string
    expect: string[]
  }>
  CORPUS_SEED: Record<string, string[]>
  PK_COLS: Record<string, string[]>
  ROOT_TABLE: Record<string, string>
}

const queryByName = new Map<string, (args: Record<string, unknown>) => unknown>()
for (const group of Object.values(groupedQueries)) {
  for (const [name, query] of Object.entries(group)) {
    if (typeof query !== 'function') continue
    if (queryByName.has(name)) throw new Error(`duplicate Chat query function '${name}'`)
    queryByName.set(name, query)
  }
}

const json = <Value>(value: Value): Value => JSON.parse(JSON.stringify(value)) as Value
const cases = fixture.CORPUS_CASES.map((testCase) => {
  const query = queryByName.get(testCase.name)
  if (!query) throw new Error(`missing Chat query function '${testCase.name}'`)
  setAuthData({ id: testCase.user, email: `${testCase.user}@example.test` })
  const internals = asQueryInternals(query(testCase.args))
  return {
    ...testCase,
    rootTable: fixture.ROOT_TABLE[testCase.name],
    ast: json(internals.ast),
    format: json(internals.format),
  }
})
const names = new Set(cases.map(({ name }) => name))
if (cases.length !== EXPECTED_CASES || names.size !== EXPECTED_QUERIES) {
  throw new Error(
    `Chat corpus size changed: ${cases.length} cases and ${names.size} queries`
  )
}

const schema = JSON.parse(
  readFileSync(resolve(chat, 'rust-sync/chat-config/chat-schema.json'), 'utf8')
)
const packageJson = JSON.parse(
  readFileSync(resolve(chat, 'node_modules/@rocicorp/zero/package.json'), 'utf8')
) as { version: string }
const corpus = {
  version: 1,
  source: {
    repository: 'chat',
    commit: CHAT_COMMIT,
    zero: packageJson.version,
    generator: 'scripts/harvest-chat-transaction-query-corpus.ts',
  },
  counts: { cases: cases.length, queries: names.size },
  schema,
  seed: fixture.CORPUS_SEED,
  primaryKeys: fixture.PK_COLS,
  cases,
}

await Bun.write(output, `${JSON.stringify(corpus, null, 2)}\n`)
execFileSync(resolve('node_modules/.bin/oxfmt'), [output], { stdio: 'inherit' })
console.log(`wrote ${cases.length} Chat cases across ${names.size} queries to ${output}`)
