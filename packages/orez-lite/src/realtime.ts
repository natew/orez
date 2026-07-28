/**
 * Streaming fields: an ephemeral typed value beside the durable Zero row.
 *
 * The implementation lives in orez-sync-executor because two packages need it
 * and only one of them can be the owner. orez-lite depends on sync-cf-host (for
 * `orez-lite/cloudflare/sync`) and sync-cf-host's Durable Object hosts the hub,
 * so owning it here would make orez-lite and sync-cf-host import each other:
 * a fresh checkout could not build, and the release ordering, which is
 * topological over workspace dependencies, would refuse the cycle outright.
 *
 * sync-executor sits below both and has no workspace dependencies of its own,
 * so it is the one place both can reach. This re-export keeps
 * `orez-lite/realtime` working unchanged for everything already importing it.
 *
 * See docs/streaming-fields.md.
 */
export * from 'orez-sync-executor/realtime'
