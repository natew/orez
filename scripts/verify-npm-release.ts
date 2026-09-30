import { createHash } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'

export const npmReleaseRegistry = 'https://registry.npmjs.org'

export async function verifyNpmRelease(
  packages: readonly { name: string; version: string }[],
  {
    registry = npmReleaseRegistry,
    tag,
    timeoutMs = 15 * 60 * 1000,
    retryDelayMs = 5000,
  }: { registry?: string; tag?: string; timeoutMs?: number; retryDelayMs?: number } = {}
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  const pending = new Map(packages.map((pkg) => [pkg, 'not checked']))
  console.info(
    `Verifying ${packages.length} package versions and tarballs on ${registry}...`
  )
  while (pending.size > 0) {
    await Promise.all(
      [...pending.keys()].map(async (pkg) => {
        const id = `${pkg.name}@${pkg.version}`
        try {
          // bypass cached misses, and check the index npm uses to resolve installs.
          const url = new URL(encodeURIComponent(pkg.name), `${registry}/`)
          url.searchParams.set('release-check', String(Date.now()))
          const request = {
            headers: {
              'Cache-Control': 'no-cache',
              Accept: 'application/vnd.npm.install-v1+json',
            },
            signal: AbortSignal.timeout(
              Math.max(1, Math.min(30_000, deadline - Date.now()))
            ),
          }
          const metadata = await fetch(url, request)
          if (!metadata.ok)
            throw new Error(`package index returned HTTP ${metadata.status}`)
          const index = (await metadata.json()) as {
            'dist-tags'?: Record<string, string>
            versions?: Record<
              string,
              {
                name?: string
                version?: string
                dist?: { tarball?: string; shasum?: string }
              }
            >
          }
          const version = index.versions?.[pkg.version]
          if (version?.name !== pkg.name || version.version !== pkg.version) {
            throw new Error('missing from the package index')
          }
          if (tag && index['dist-tags']?.[tag] !== pkg.version) {
            throw new Error(`${tag} points to ${index['dist-tags']?.[tag] ?? 'nothing'}`)
          }
          if (!version.dist?.tarball || !version.dist.shasum) {
            throw new Error('package index has no tarball or checksum')
          }
          const tarballUrl = new URL(version.dist.tarball)
          if (tarballUrl.origin !== new URL(registry).origin) {
            throw new Error(`tarball uses a different registry: ${tarballUrl.origin}`)
          }
          tarballUrl.searchParams.set('release-check', String(Date.now()))
          const tarball = await fetch(tarballUrl, request)
          if (!tarball.ok) throw new Error(`tarball returned HTTP ${tarball.status}`)
          const actual = createHash('sha1')
            .update(Buffer.from(await tarball.arrayBuffer()))
            .digest('hex')
          if (actual !== version.dist.shasum) throw new Error('tarball checksum mismatch')
          pending.delete(pkg)
          console.info(`Verified ${id}`)
        } catch (error) {
          pending.set(pkg, error instanceof Error ? error.message : String(error))
        }
      })
    )
    if (pending.size === 0) return
    const remainingMs = deadline - Date.now()
    if (remainingMs <= 0) {
      throw new Error(
        `npm release not available after ${timeoutMs / 1000}s:\n${[...pending].map(([pkg, reason]) => `  ${pkg.name}@${pkg.version}: ${reason}`).join('\n')}`
      )
    }
    await setTimeout(Math.min(retryDelayMs, remainingMs))
  }
}
