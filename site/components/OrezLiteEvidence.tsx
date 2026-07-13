import evidence from '~/data/orez-lite-evidence.json' with { type: 'json' }

import { AppLink } from './AppLink'

function compactSha(sha: string | null) {
  return sha ? sha.slice(0, 12) : 'No qualified SHA'
}

function formatDuration(value: number | null) {
  if (value === null) return '—'
  const seconds = Math.round(value / 1000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  return `${minutes}m ${seconds % 60}s`
}

function formatNumber(value: number | null) {
  return value === null ? '—' : value.toLocaleString('en-US')
}

function formatTimestamp(value: string | null) {
  if (!value) return 'Not yet qualified'
  return new Intl.DateTimeFormat('en', {
    dateStyle: 'medium',
    timeStyle: 'short',
    timeZone: 'UTC',
  }).format(new Date(value))
}

function EvidenceLink({ href, children }: { href: string | null; children: string }) {
  return href ? (
    <AppLink href={href} target="_blank" className="evidence-link">
      {children}
    </AppLink>
  ) : (
    <span className="evidence-link evidence-link-disabled">{children}</span>
  )
}

export function VerifiedBuildCard() {
  const verified = evidence.status === 'verified'
  return (
    <aside className="verified-build-card" aria-labelledby="verified-build-title">
      <div className="verified-build-heading">
        <div>
          <span className="evidence-eyebrow">Build evidence</span>
          <h2 id="verified-build-title">Verified build</h2>
        </div>
        <span
          className={`evidence-status evidence-status-${verified ? 'pass' : 'awaiting'}`}
        >
          {verified ? 'CI verified' : 'Awaiting green CI'}
        </span>
      </div>
      <dl className="verified-build-facts">
        <div>
          <dt>Release</dt>
          <dd>
            <EvidenceLink href={evidence.release.url}>
              {evidence.release.tag}
            </EvidenceLink>
          </dd>
        </div>
        <div>
          <dt>Build SHA</dt>
          <dd>
            {evidence.build.url ? (
              <EvidenceLink href={evidence.build.url}>
                {compactSha(evidence.build.sha)}
              </EvidenceLink>
            ) : (
              <code>{compactSha(evidence.build.sha)}</code>
            )}
          </dd>
        </div>
        <div>
          <dt>Zero</dt>
          <dd>{evidence.versions.zero}</dd>
        </div>
        <div>
          <dt>Qualified</dt>
          <dd>{formatTimestamp(evidence.qualification.qualifiedAt)}</dd>
        </div>
      </dl>
      <p className="verified-build-contracts">
        <strong>Supported contracts:</strong>{' '}
        {verified
          ? evidence.supportedContracts.join(' · ')
          : 'None advertised until every required job passes at one main-branch SHA.'}
      </p>
      <AppLink
        href="/docs/orez-lite/testing#evidence-ledger"
        className="evidence-card-link"
      >
        Open the full evidence ledger →
      </AppLink>
    </aside>
  )
}

export function EvidenceLedger() {
  const verified = evidence.status === 'verified'
  const lastGreen = evidence.qualification.lastGreen as {
    runId: number
    url: string
  } | null
  return (
    <div id="evidence-ledger" className="evidence-ledger">
      <div
        className={`evidence-notice evidence-notice-${verified ? 'pass' : 'awaiting'}`}
      >
        <strong>
          {verified
            ? 'Qualified at this exact SHA.'
            : 'No verified build is published yet.'}
        </strong>
        <span>
          {verified
            ? 'This ledger was generated only after every required CI job passed, then included in the static site build.'
            : 'The checked-in fallback stays unverified because the latest observed main-branch run was red. CI will replace it in the static build only after the exact-SHA gate is green.'}
        </span>
      </div>

      <h2>Build identity</h2>
      <div className="evidence-fact-grid">
        <dl>
          <dt>Release</dt>
          <dd>{evidence.release.tag}</dd>
          <dt>Release SHA</dt>
          <dd>
            <code>{evidence.release.sha ?? '—'}</code>
          </dd>
          <dt>Qualified build SHA</dt>
          <dd>
            <code>{evidence.build.sha ?? '—'}</code>
          </dd>
          <dt>Last green run</dt>
          <dd>
            {lastGreen ? (
              <EvidenceLink href={lastGreen.url}>{`Run ${lastGreen.runId}`}</EvidenceLink>
            ) : (
              'None'
            )}
          </dd>
        </dl>
        <dl>
          <dt>Zero</dt>
          <dd>{evidence.versions.zero}</dd>
          <dt>Rust toolchain</dt>
          <dd>{evidence.versions.rust}</dd>
          <dt>SQLite</dt>
          <dd>
            {evidence.versions.sqlite} · rusqlite {evidence.versions.rusqlite} ·
            libsqlite3-sys {evidence.versions.libsqlite3Sys}
          </dd>
          <dt>workerd / Wrangler</dt>
          <dd>
            {evidence.versions.workerd} / {evidence.versions.wrangler}
          </dd>
        </dl>
      </div>

      <h2>Qualification summary</h2>
      <dl className="evidence-metrics">
        <div>
          <dt>Qualified</dt>
          <dd>{formatTimestamp(evidence.qualification.qualifiedAt)}</dd>
        </div>
        <div>
          <dt>Scenarios</dt>
          <dd>{formatNumber(evidence.qualification.scenarioCount)}</dd>
        </div>
        <div>
          <dt>Seed</dt>
          <dd>{formatNumber(evidence.qualification.randomizedSeed)}</dd>
        </div>
        <div>
          <dt>Counted operations</dt>
          <dd>{formatNumber(evidence.qualification.operationCount)}</dd>
        </div>
        <div>
          <dt>Restarts</dt>
          <dd>{formatNumber(evidence.qualification.restarts)}</dd>
        </div>
        <div>
          <dt>Wall duration</dt>
          <dd>{formatDuration(evidence.qualification.durationMs)}</dd>
        </div>
      </dl>
      {evidence.qualification.operationUnit ? (
        <p className="evidence-caption">{evidence.qualification.operationUnit}</p>
      ) : null}

      <h2>Per-host compatibility</h2>
      <div
        className="table-scroll"
        role="region"
        aria-label="Host compatibility"
        tabIndex={0}
      >
        <table>
          <thead>
            <tr>
              <th>Host</th>
              <th>Status</th>
              <th>Qualified contract</th>
            </tr>
          </thead>
          <tbody>
            {evidence.compatibility.map((row) => (
              <tr key={row.host}>
                <td>{row.host}</td>
                <td>
                  <span className={`evidence-status evidence-status-${row.status}`}>
                    {row.status}
                  </span>
                </td>
                <td>{row.contract}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h2>Suite ledger</h2>
      <div className="evidence-suites">
        {evidence.suites.map((suite) => (
          <section
            className="evidence-suite"
            key={suite.id}
            aria-labelledby={`${suite.id}-title`}
          >
            <header>
              <div>
                <span className="evidence-eyebrow">{suite.host}</span>
                <h3 id={`${suite.id}-title`}>{suite.name}</h3>
              </div>
              <span className={`evidence-status evidence-status-${suite.status}`}>
                {suite.status}
              </span>
            </header>
            <dl className="evidence-suite-metrics">
              <div>
                <dt>Scenarios</dt>
                <dd>{formatNumber(suite.scenarioCount)}</dd>
              </div>
              <div>
                <dt>Seed</dt>
                <dd>{formatNumber(suite.randomizedSeed)}</dd>
              </div>
              <div>
                <dt>Operations</dt>
                <dd>{formatNumber(suite.operationCount)}</dd>
              </div>
              <div>
                <dt>Restarts</dt>
                <dd>{formatNumber(suite.restarts)}</dd>
              </div>
              <div>
                <dt>Duration</dt>
                <dd>{formatDuration(suite.durationMs)}</dd>
              </div>
            </dl>
            <div className="evidence-proof-grid">
              <div>
                <h4>What this proves</h4>
                <p>{suite.whatItProves}</p>
              </div>
              <div>
                <h4>What this does not prove</h4>
                <p>{suite.whatItDoesNotProve}</p>
              </div>
            </div>
            <details>
              <summary>Reproduce this suite</summary>
              {suite.commands.map((command) => (
                <pre key={command}>
                  <code>{command}</code>
                </pre>
              ))}
            </details>
            <div className="evidence-suite-links">
              <EvidenceLink href={suite.logsUrl}>Immutable logs</EvidenceLink>
              <EvidenceLink href={suite.artifactsUrl}>Run artifacts</EvidenceLink>
            </div>
          </section>
        ))}
      </div>

      <h2>Reproduce the qualification</h2>
      <h3>Environment</h3>
      <pre>
        <code>{evidence.reproduction.environment.join('\n')}</code>
      </pre>
      <h3>Core commands</h3>
      <pre>
        <code>{evidence.reproduction.full.join('\n')}</code>
      </pre>

      <h2>Logs, artifacts, and regression traces</h2>
      <div className="evidence-artifact-links">
        <EvidenceLink href={evidence.artifacts.logsUrl}>Qualification logs</EvidenceLink>
        <EvidenceLink href={evidence.artifacts.evidenceJsonUrl}>
          CI evidence JSON
        </EvidenceLink>
        <EvidenceLink href={evidence.artifacts.regressionTracesUrl}>
          {`Minimized regression traces (${evidence.artifacts.regressionTraceCount})`}
        </EvidenceLink>
      </div>
      <p className="evidence-caption">
        Links identify one immutable workflow run. GitHub retains its logs and artifacts
        for {evidence.artifacts.retentionDays} days. An empty trace archive includes a
        manifest; seeded failures include their minimized JSON and exact replay command.
      </p>

      <h2>Known limitations</h2>
      <ul>
        {evidence.knownLimitations.map((limitation) => (
          <li key={limitation}>{limitation}</li>
        ))}
      </ul>

      <h2>Unresolved red or fragile lanes</h2>
      <div className="evidence-unresolved">
        {evidence.unresolvedLanes.map((lane) => (
          <article key={lane.name}>
            <div>
              <h3>{lane.name}</h3>
              <span className={`evidence-status evidence-status-${lane.status}`}>
                {lane.status}
              </span>
            </div>
            <p>{lane.detail}</p>
            <EvidenceLink href={lane.url}>Lane record</EvidenceLink>
          </article>
        ))}
      </div>

      <h2>Release gate</h2>
      <p>{evidence.gate.policy}</p>
      <p>
        Required jobs: <code>{evidence.gate.requiredJobs.join(', ')}</code>.
      </p>
    </div>
  )
}
