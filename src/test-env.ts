import { afterAll, beforeAll } from 'vitest'

/**
 * pin ZERO_APP_PUBLICATIONS for the duration of a test file.
 *
 * `bun test` runs every file in one shared process, so env mutated by an
 * earlier file leaks into later ones. the publication env leaks from two
 * places: integration suites that set it directly, and startOrez's
 * managed-publication fallback (`getManagedPublicationConfig` sets
 * ZERO_APP_PUBLICATIONS=orez_<appId>_public process-wide). a leaked value
 * makes installChangeTracking scope triggers to a publication that does not
 * exist on a freshly created test database — it then installs no triggers
 * and every change-tracking assertion fails. suites that install change
 * tracking must declare the publication env they assume.
 */
export function usePublicationsEnv(value: string | undefined): void {
  let saved: string | undefined
  beforeAll(() => {
    saved = process.env.ZERO_APP_PUBLICATIONS
    if (value === undefined) delete process.env.ZERO_APP_PUBLICATIONS
    else process.env.ZERO_APP_PUBLICATIONS = value
  })
  afterAll(() => {
    if (saved === undefined) delete process.env.ZERO_APP_PUBLICATIONS
    else process.env.ZERO_APP_PUBLICATIONS = saved
  })
}
