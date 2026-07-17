import { describe, expect, it } from 'bun:test'
import { readFileSync } from 'node:fs'

import { classifyEvidenceIdentity } from '../site/lib/evidence-identity'
import {
  laneCountFromWorkflow,
  statusForQualifiedBuild,
  traceRestartCount,
  traceStepCount,
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

describe('honest suite stat derivations', () => {
  const workflow = [
    'jobs:',
    '  rust-local:',
    '    steps:',
    '      - run: cd harness && bun src/shapes.ts --against rust-local',
    '      - run: cd harness && bun src/sweep.ts --against rust-local --rounds 10',
    // duplicate lane must not double-count',
    '      - run: cd harness && bun src/shapes.ts --against rust-local --replay',
    '  rust-local-faults:',
    '    steps:',
    '      - run: cd harness && bun src/protocol-fuzz.ts --target rust-local',
    '      - run: |',
    '          cd harness',
    '          bun src/state-machine.ts --against rust-local --seed 1 --steps 18',
    '  no-lanes:',
    '    steps:',
    '      - run: cargo test --workspace',
    '',
  ].join('\n')

  it('counts distinct harness lanes per job from the workflow', () => {
    expect(laneCountFromWorkflow(workflow, 'rust-local')).toBe(2)
    expect(laneCountFromWorkflow(workflow, 'rust-local-faults')).toBe(2)
  })

  it('throws for an unknown job or a job with no harness lanes', () => {
    expect(() => laneCountFromWorkflow(workflow, 'missing')).toThrow(
      'ci.yml has no job missing'
    )
    expect(() => laneCountFromWorkflow(workflow, 'no-lanes')).toThrow(
      'runs no harness lanes'
    )
  })

  it('counts state-machine trace steps and restarts, not other kinds', () => {
    const runs = [
      {
        target: 'rust-local',
        trace: [
          { kind: 'write' },
          { kind: 'serverRestart' },
          { kind: 'clientRestart' },
          { kind: 'prune' },
        ],
      },
      { target: 'rust-local', trace: [{ kind: 'write' }, { kind: 'clientRestart' }] },
    ]
    expect(traceStepCount(runs)).toBe(6)
    expect(traceRestartCount(runs)).toBe(3)
    expect(traceStepCount([])).toBe(0)
    expect(traceRestartCount([])).toBe(0)
  })

  it('derives the real per-suite counts against the checked-in workflow', () => {
    const ci = readFileSync(
      new URL('../.github/workflows/ci.yml', import.meta.url),
      'utf8'
    )
    // every host job runs at least one harness lane; exact counts drift with the
    // lanes on purpose, so assert the honest floor rather than a frozen number.
    for (const job of ['harness', 'rust-local', 'rust-local-faults', 'sync-cf-host']) {
      expect(laneCountFromWorkflow(ci, job)).toBeGreaterThan(0)
    }
  })
})
