import {
  EMBED_WARM_INTERVAL_MS,
  EMBED_WARM_REQUEST_TIMEOUT_MS,
  EMBED_WARM_TIMEOUT_MS,
  HEALTH_POLL_INTERVAL_MS,
  HEALTH_POLL_MAX_ATTEMPTS,
} from './leaves.js'

export type CloudflareDeployLog = (msg: string) => void

export type WarmZeroCacheEmbedOptions = {
  /** request one fresh boot before terminal deploy-only polling */
  startFresh?: boolean
}

// resolve the account's *.workers.dev subdomain so BETTER_AUTH_URL points at
// the actual deployed worker. cached per-process since it never changes for a
// given account.
let cachedSubdomain: { accountId: string; subdomain: string } | undefined
export async function getAccountWorkersSubdomain(
  accountId: string,
  apiToken: string
): Promise<string> {
  if (cachedSubdomain?.accountId === accountId) return cachedSubdomain.subdomain
  const res = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/subdomain`,
    { headers: { Authorization: `Bearer ${apiToken}` } }
  )
  if (!res.ok) {
    throw new Error(
      `failed to read workers subdomain for account ${accountId}: ${res.status} ${(await res.text()).slice(0, 200)}`
    )
  }
  const body = (await res.json()) as { result?: { subdomain?: string } }
  const subdomain = body.result?.subdomain
  if (!subdomain) {
    throw new Error(
      `workers subdomain response for account ${accountId} had no result.subdomain`
    )
  }
  cachedSubdomain = { accountId, subdomain }
  return subdomain
}

export async function warmZeroCacheEmbed(
  url: string,
  log: CloudflareDeployLog,
  options: WarmZeroCacheEmbedOptions = {}
): Promise<void> {
  // kick the zero-cache embed cold boot NOW and WAIT until it can actually
  // hydrate a client, instead of making the first visitor (or the runtime
  // validation that runs right after) race a half-booted embed. /keepalive
  // routes into ZeroCacheDO and kicks the alarm-carried boot (which
  // survives a request abort), returning 200 once initial-sync is done and
  // 202 while still booting. poll until ready; a fresh deploy's initial
  // sync runs minutes of per-statement libpg parses, so the ceiling is
  // generous and small apps fall through early on the first 200.
  log('[cloudflare] warming zero-cache embed (waiting for initial sync)...')
  const warmDeadline = Date.now() + EMBED_WARM_TIMEOUT_MS
  let warmReady = false
  if (options.startFresh) {
    try {
      const start = await fetch(`${url}/keepalive`, {
        signal: AbortSignal.timeout(EMBED_WARM_REQUEST_TIMEOUT_MS),
      })
      if (start.status === 200) {
        log('[cloudflare] zero-cache embed ready after fresh boot request')
        return
      }
    } catch {
      // the request may still have scheduled the alarm; deploy probes below
      // observe its durable marker without starting another generation.
    }
  }
  for (let warmAttempt = 1; Date.now() < warmDeadline; warmAttempt++) {
    let terminalBootFailures: number | undefined
    let terminalBootReason: string | undefined
    try {
      const warm = await fetch(`${url}/keepalive?deploy=1`, {
        signal: AbortSignal.timeout(EMBED_WARM_REQUEST_TIMEOUT_MS),
      })
      if (warm.status === 200) {
        log(`[cloudflare] zero-cache embed ready after ${warmAttempt} probe(s)`)
        warmReady = true
        break
      }
      if (warm.status === 409) {
        const body: unknown = await warm.json()
        if (
          typeof body === 'object' &&
          body !== null &&
          'status' in body &&
          body.status === 'boot-failed' &&
          'failures' in body &&
          typeof body.failures === 'number' &&
          Number.isInteger(body.failures) &&
          body.failures > 0
        ) {
          terminalBootFailures = body.failures
          if (
            'reason' in body &&
            typeof body.reason === 'string' &&
            body.reason.length > 0
          ) {
            terminalBootReason = body.reason.slice(0, 500)
          }
        }
      }
    } catch {
      // transient (request timed out / boot churning) — keep polling
    }
    if (terminalBootFailures !== undefined) {
      const attempts = terminalBootFailures === 1 ? 'attempt' : 'attempts'
      const reason = terminalBootReason ? `: ${terminalBootReason}` : ''
      throw new Error(
        `zero-cache embed boot failed after ${terminalBootFailures} ${attempts}${reason}; deploy aborted to avoid restarting initial sync`
      )
    }
    await new Promise((r) => setTimeout(r, EMBED_WARM_INTERVAL_MS))
  }
  if (!warmReady) {
    throw new Error(
      `zero-cache embed did not become ready within ${EMBED_WARM_TIMEOUT_MS / 1000}s`
    )
  }
}

export async function pollWorkerReady({
  url,
  expectedVersion,
  workerName,
  warmZeroCache = true,
  log,
}: {
  url: string
  expectedVersion: string
  workerName: string
  warmZeroCache?: boolean
  log: CloudflareDeployLog
}): Promise<void> {
  // poll until the worker and asset binding respond. zero-cache embed
  // cold-init can run for a while during the DO's ctx.blockConcurrencyWhile;
  // Cloudflare can also serve the module before static assets are reachable.
  log(`[cloudflare] polling ${url} for readiness...`)
  let lastError: string | undefined
  for (let attempt = 1; attempt <= HEALTH_POLL_MAX_ATTEMPTS; attempt++) {
    let res: Response
    let versionRes: Response
    try {
      const responses = await Promise.all([
        fetch(url, { redirect: 'manual' }),
        fetch(`${url}/version.json`, {
          redirect: 'manual',
          headers: {
            'cache-control': 'no-cache',
            pragma: 'no-cache',
          },
        }),
      ])
      res = responses[0]
      versionRes = responses[1]
    } catch (err) {
      lastError = (err as Error).message
      await new Promise((r) => setTimeout(r, HEALTH_POLL_INTERVAL_MS))
      continue
    }
    const versionBody: unknown =
      versionRes.status === 200 ? await versionRes.json() : null
    // Only 2xx/3xx means the worker is actually serving. A 404 during
    // cold-start means Cloudflare's default handler is still responding
    // — the worker module's fetch() hasn't been registered yet.
    if (
      res.status >= 200 &&
      res.status < 400 &&
      typeof versionBody === 'object' &&
      versionBody !== null &&
      'version' in versionBody &&
      versionBody.version === expectedVersion
    ) {
      log(`[cloudflare] worker responded ${res.status} after ${attempt} attempt(s)`)
      if (warmZeroCache) await warmZeroCacheEmbed(url, log)
      return
    }
    lastError = `root ${res.status}; version ${versionRes.status} ${JSON.stringify(versionBody)}`
    await new Promise((r) => setTimeout(r, HEALTH_POLL_INTERVAL_MS))
  }
  throw new Error(
    `worker ${workerName} did not become ready within ${(HEALTH_POLL_MAX_ATTEMPTS * HEALTH_POLL_INTERVAL_MS) / 1000}s; last: ${lastError}`
  )
}
