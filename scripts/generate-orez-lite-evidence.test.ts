import { describe, expect, it } from 'bun:test'
import { readFileSync } from 'node:fs'

import { classifyEvidenceIdentity } from '../site/lib/evidence-identity'
import {
  statusForBuild,
  validate,
  type Evidence,
  type Status,
} from './generate-orez-lite-evidence'

const releaseSha = '1111111111111111111111111111111111111111'
const candidateSha = '2222222222222222222222222222222222222222'
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

  it('accepts verified evidence only when release and tested build SHAs match', () => {
    const evidence = fixture('verified', releaseSha)
    expect(statusForBuild(evidence.release.sha, releaseSha)).toBe('verified')
    expect(
      classifyEvidenceIdentity(evidence.status, evidence.release.sha, releaseSha)
    ).toBe('verified')
    expect(() => validate(evidence, releaseSha)).not.toThrow()
  })

  it('rejects a verified release label when the tested SHA differs', () => {
    const evidence = fixture('verified', candidateSha)
    expect(
      classifyEvidenceIdentity(evidence.status, evidence.release.sha, candidateSha)
    ).toBe('candidate')
    expect(() => validate(evidence, candidateSha)).toThrow(
      `verified release SHA ${releaseSha} does not match tested build SHA ${candidateSha}`
    )
  })

  it('accepts a mismatched main build only as an unverified candidate', () => {
    const evidence = fixture('unverified', candidateSha)
    expect(statusForBuild(evidence.release.sha, candidateSha)).toBe('unverified')
    expect(
      classifyEvidenceIdentity(evidence.status, evidence.release.sha, candidateSha)
    ).toBe('candidate')
    expect(() => validate(evidence, candidateSha)).not.toThrow()
  })

  it('still requires candidate evidence to identify the expected CI SHA', () => {
    const evidence = fixture('unverified', candidateSha)
    expect(() => validate(evidence, releaseSha)).toThrow(
      `evidence SHA ${candidateSha} does not match ${releaseSha}`
    )
  })
})
