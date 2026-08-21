import { metrics } from '@opentelemetry/api'

type MetricAttributes = Record<string, string | number | boolean>
type Histogram = { record(value: number, attributes?: MetricAttributes): void }
type Counter = { add(value: number, attributes?: MetricAttributes): void }

export type ServingLagInstruments = {
  servingLag: Histogram
  servingLagClamps: Counter
  upstreamClockSkew: Histogram
}

let defaultInstruments: ServingLagInstruments | undefined

function instruments(): ServingLagInstruments {
  if (defaultInstruments) return defaultInstruments
  const meter = metrics.getMeter('orez')
  defaultInstruments = {
    servingLag: meter.createHistogram('orez.sync.e2e_serving_lag', {
      description:
        'End-to-end completion lag from an upstream commit until a client group is served through that advancement.',
      unit: 's',
    }),
    servingLagClamps: meter.createCounter('orez.sync.e2e_serving_lag_clamps', {
      description:
        'Negative end-to-end serving-lag observations clamped to zero because the upstream clock was ahead.',
      unit: '{observation}',
    }),
    upstreamClockSkew: meter.createHistogram('orez.replication.upstream_clock_skew', {
      description:
        'Estimated upstream clock offset from the Orez host. Positive means upstream is ahead.',
      unit: 's',
    }),
  }
  return defaultInstruments
}

function counter(value: unknown): bigint | null {
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) {
    return BigInt(value)
  }
  if (typeof value === 'string' && /^(?:0|[1-9]\d*)$/.test(value)) {
    return BigInt(value)
  }
  return null
}

type PendingCommit = { watermark: bigint; commitTimeMs: number }

/**
 * Coalesces upstream commits independently for each active client group. A
 * pull clears a pending observation even when its patch is empty: being
 * current through the version is the completion event, not receiving a row.
 */
export class ServingLagTracker {
  readonly #pending = new Map<string, PendingCommit>()

  constructor(private readonly metrics: ServingLagInstruments = instruments()) {}

  onVersionReady(
    watermarkValue: unknown,
    upstreamCommitTimeMs: number,
    clientGroupIDs: Iterable<string>
  ): void {
    const watermark = counter(watermarkValue)
    if (watermark === null || !Number.isFinite(upstreamCommitTimeMs)) return
    for (const clientGroupID of clientGroupIDs) {
      if (!clientGroupID) continue
      const previous = this.#pending.get(clientGroupID)
      this.#pending.set(clientGroupID, {
        watermark,
        commitTimeMs: previous
          ? Math.min(previous.commitTimeMs, upstreamCommitTimeMs)
          : upstreamCommitTimeMs,
      })
    }
  }

  onVersionServed(
    clientGroupID: string,
    watermarkValue: unknown,
    nowMs = Date.now()
  ): void {
    const served = counter(watermarkValue)
    const pending = this.#pending.get(clientGroupID)
    if (served === null || !pending || served < pending.watermark) return
    this.#pending.delete(clientGroupID)
    this.#record(nowMs - pending.commitTimeMs, { outcome: 'advanced' })
  }

  recordNoChange(upstreamCommitTimeMs: number, nowMs = Date.now()): void {
    if (!Number.isFinite(upstreamCommitTimeMs)) return
    this.#record(nowMs - upstreamCommitTimeMs, { outcome: 'no_change' })
  }

  recordClockSkew(sourceTimeMs: number, sendTimeMs: number, receiveTimeMs: number): void {
    if (![sourceTimeMs, sendTimeMs, receiveTimeMs].every(Number.isFinite)) return
    this.metrics.upstreamClockSkew.record(
      (sourceTimeMs - (sendTimeMs + receiveTimeMs) / 2) / 1_000
    )
  }

  #record(rawLagMs: number, attributes: MetricAttributes): void {
    const clamped = rawLagMs < 0
    this.metrics.servingLag.record(Math.max(0, rawLagMs) / 1_000, attributes)
    if (clamped) this.metrics.servingLagClamps.add(1, attributes)
  }
}
