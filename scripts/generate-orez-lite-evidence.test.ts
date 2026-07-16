import { describe, expect, it } from 'bun:test'
import { readFileSync } from 'node:fs'

import { classifyEvidenceIdentity } from '../site/lib/evidence-identity'
import {
  statusForQualifiedBuild,
  validate,
  type Evidence,
  type Status,
} from './generate-orez-lite-evidence'

const releaseSha = '1111111111111111111111111111111111111111'
const mainBuildSha = '2222222222222222222222222222222222222222'
const fallback = JSON.parse(
  readFileSync(new URL('../site/data/orez-lite-evidence.json', import.meta.url), 'utf8')
) as Evidence

function fixture(status: Status, buildSha: string): Evidence {
  const evidence = structuredClone(fallback)
  evidence.status = status
  evidence.release.sha = releaseSha
  evidence.build = {
    sha: buildSha,
    url: `https://github.com/natew/orez/tree/${buildSha}`,
  }

  evidence.qualification = {
    qualifiedAt: '2026-07-13T20:00:00.000Z',
    lastGreen: {
      runId: 123,
      url: 'https://github.com/natew/orez/actions/runs/123',
    },
    scenarioCount: 1,
    randomizedSeed: 123,
    operationCount: 1,
    operationUnit: 'fixture operations',
    restarts: 0,
    durationMs: 1,
  }
  for (const suite of evidence.suites) {
    suite.status = 'pass'
    suite.scenarioCount = 1
    suite.durationMs = 1
    suite.logsUrl = 'https://github.com/natew/orez/actions/runs/123'
    suite.artifactsUrl = 'https://github.com/natew/orez/actions/runs/123#artifacts'
  }

  if (status === 'verified') {
    evidence.supportedContracts = ['fixture contract']
    for (const row of evidence.compatibility) {
      row.status = 'pass'
    }
  }

  return evidence
}

describe('exact-SHA release evidence identity', () => {
  it('keeps the checked-in no-build fallback unverified', () => {
    expect(
      classifyEvidenceIdentity(fallback.status, fallback.release.sha, fallback.build.sha)
    ).toBe('unverified')
    expect(() => validate(structuredClone(fallback))).not.toThrow()
  })

  it('classifies an exact release-tag build as a verified release', () => {
    const evidence = fixture('verified', releaseSha)
    expect(statusForQualifiedBuild(releaseSha)).toBe('verified')
    expect(
      classifyEvidenceIdentity(evidence.status, evidence.release.sha, releaseSha)
    ).toBe('verified-release')
    expect(() => validate(evidence, releaseSha)).not.toThrow()
  })

  it('classifies a green main build newer than the release as verified build evidence', () => {
    const evidence = fixture('verified', mainBuildSha)
    expect(
      classifyEvidenceIdentity(evidence.status, evidence.release.sha, mainBuildSha)
    ).toBe('verified-build')
    expect(() => validate(evidence, mainBuildSha)).not.toThrow()
  })

  it('does not call an unverified build verified merely because it has a SHA', () => {
    const evidence = fixture('unverified', mainBuildSha)
    evidence.supportedContracts = []
    expect(
      classifyEvidenceIdentity(evidence.status, evidence.release.sha, mainBuildSha)
    ).toBe('unverified')
    expect(() => validate(evidence, mainBuildSha)).not.toThrow()
  })

  it('still requires build evidence to identify the expected CI SHA', () => {
    const evidence = fixture('verified', mainBuildSha)
    expect(() => validate(evidence, releaseSha)).toThrow(
      `evidence SHA ${mainBuildSha} does not match ${releaseSha}`
    )
  })

  it('keeps a missing build unverified', () => {
    expect(statusForQualifiedBuild(null)).toBe('unverified')
  })
})
