import { readFileSync } from 'node:fs'
import { join } from 'node:path'

type Source = {
  id: string
  repository: string
  commit: string
  license: { spdx: string; path: string }
}

type Scenario = {
  id: string
  source: { repository: string; path: string; scenario: string }
  contract: string
  adaptation: string
  supportedHosts: string[]
}

type Corpus = {
  schemaVersion: number
  hosts: Record<string, unknown>
  sources: Source[]
  scenarios: Scenario[]
}

const corpusPath = join(import.meta.dirname, '..', 'corpus', 'upstream-scenarios.json')
const corpus = JSON.parse(readFileSync(corpusPath, 'utf8')) as Corpus

const problems: string[] = []
const unique = (values: string[], label: string) => {
  const seen = new Set<string>()
  for (const value of values) {
    if (seen.has(value)) problems.push(`duplicate ${label}: ${value}`)
    seen.add(value)
  }
}

if (corpus.schemaVersion !== 1) problems.push('schemaVersion must be 1')
unique(
  corpus.sources.map(({ id }) => id),
  'source id'
)
unique(
  corpus.scenarios.map(({ id }) => id),
  'scenario id'
)

const sourceIDs = new Set(corpus.sources.map(({ id }) => id))
const hostIDs = new Set(Object.keys(corpus.hosts))
for (const source of corpus.sources) {
  if (!/^https:\/\/github\.com\/[^/]+\/[^/]+$/.test(source.repository))
    problems.push(`${source.id}: repository must be a GitHub repository URL`)
  if (!/^[0-9a-f]{40}$/.test(source.commit))
    problems.push(`${source.id}: commit must be a full 40-character SHA`)
  if (!source.license.spdx || !source.license.path)
    problems.push(`${source.id}: license SPDX and path are required`)
}

for (const scenario of corpus.scenarios) {
  if (!sourceIDs.has(scenario.source.repository))
    problems.push(`${scenario.id}: unknown source ${scenario.source.repository}`)
  if (!scenario.source.path || !scenario.source.scenario)
    problems.push(`${scenario.id}: source path and scenario are required`)
  if (!scenario.contract || !scenario.adaptation)
    problems.push(`${scenario.id}: contract and adaptation are required`)
  if (scenario.supportedHosts.length === 0)
    problems.push(`${scenario.id}: at least one supported host is required`)
  for (const host of scenario.supportedHosts) {
    if (!hostIDs.has(host)) problems.push(`${scenario.id}: unknown host ${host}`)
  }
}

if (problems.length > 0) {
  throw new Error(`invalid upstream corpus:\n- ${problems.join('\n- ')}`)
}

console.log(
  `[corpus-check] PASS: ${corpus.sources.length} pinned sources, ${corpus.scenarios.length} scenarios, ${hostIDs.size} hosts`
)
