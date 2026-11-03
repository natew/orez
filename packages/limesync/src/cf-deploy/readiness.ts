const DEFAULT_INTERVAL_MS = 2_000
const DEFAULT_MAX_ATTEMPTS = 60
const REQUIRED_MATCHING_OBSERVATIONS = 3

export type CloudflareDeployLog = (message: string) => void

let cachedWorkersSubdomain: { accountId: string; subdomain: string } | undefined

export async function getAccountWorkersSubdomain(
  accountId: string,
  apiToken: string
): Promise<string> {
  if (cachedWorkersSubdomain?.accountId === accountId) {
    return cachedWorkersSubdomain.subdomain
  }

  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/subdomain`,
    { headers: { Authorization: `Bearer ${apiToken}` } }
  )
  if (!response.ok) {
    throw new Error(
      `failed to read workers subdomain for account ${accountId}: ${response.status} ${(await response.text()).slice(0, 200)}`
    )
  }

  const body = (await response.json()) as { result?: { subdomain?: string } }
  const subdomain = body.result?.subdomain
  if (!subdomain) {
    throw new Error(
      `workers subdomain response for account ${accountId} had no result.subdomain`
    )
  }

  cachedWorkersSubdomain = { accountId, subdomain }
  return subdomain
}

export async function waitForWorkerReady({
  url,
  expectedVersion,
  workerName,
  log,
  intervalMs = DEFAULT_INTERVAL_MS,
  maxAttempts = DEFAULT_MAX_ATTEMPTS,
}: {
  url: string
  expectedVersion: string
  workerName: string
  log: CloudflareDeployLog
  intervalMs?: number
  maxAttempts?: number
}): Promise<void> {
  log(`[cloudflare] polling ${url} for readiness...`)
  let lastError: string | undefined
  let matchingObservations = 0

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const [rootResponse, versionResponse] = await Promise.all([
        fetch(url, { redirect: 'manual' }),
        fetch(`${url}/version.json`, {
          redirect: 'manual',
          headers: {
            'cache-control': 'no-cache',
            pragma: 'no-cache',
          },
        }),
      ])
      const versionBody: unknown =
        versionResponse.status === 200 ? await versionResponse.json() : null

      if (
        rootResponse.status >= 200 &&
        rootResponse.status < 400 &&
        typeof versionBody === 'object' &&
        versionBody !== null &&
        'version' in versionBody &&
        versionBody.version === expectedVersion
      ) {
        matchingObservations++
        if (matchingObservations === REQUIRED_MATCHING_OBSERVATIONS) {
          log(
            `[cloudflare] worker responded ${rootResponse.status} consistently after ${attempt} attempt(s)`
          )
          return
        }
        lastError = `matching version observed ${matchingObservations}/${REQUIRED_MATCHING_OBSERVATIONS} times`
      } else {
        matchingObservations = 0
        lastError = `root ${rootResponse.status}; version ${versionResponse.status} ${JSON.stringify(versionBody)}`
      }
    } catch (error) {
      matchingObservations = 0
      lastError = error instanceof Error ? error.message : String(error)
    }

    if (attempt < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, intervalMs))
    }
  }

  throw new Error(
    `worker ${workerName} did not become ready within ${(maxAttempts * intervalMs) / 1000}s; last: ${lastError}`
  )
}
