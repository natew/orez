/**
 * orez/realtime — streaming fields, an ephemeral typed field overlay beside
 * Zero.
 *
 * The implementation lives in orez-lite so that a Cloudflare worker, a browser
 * bundle, and this package all share one copy. This entrypoint exists because
 * applications on the full `orez` package should not have to add a second
 * dependency to reach it.
 *
 * usage:
 *   import { defineStreamingFields, createLocalRealtime } from 'orez/realtime'
 */

export * from 'orez-lite/realtime'
