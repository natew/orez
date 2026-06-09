/**
 * idle-hibernation decision for a ZeroCacheDO running the zero-cache embed.
 *
 * the deployed `ZeroCacheDO` (a bundled string in soot's cloudflareDoDeploy.ts,
 * awkward to unit-test) watches its embed's `connectionCount` on a periodic
 * alarm and, once no sync client is connected, tears the embed down so the DO
 * can be evicted and stops accruing GB-s. this module holds the one piece of
 * non-obvious logic — the grace window — as a pure function so it can live
 * outside the string and be tested here.
 *
 * see plans/cf-do-idle-hibernation.md.
 */

/** default alarm cadence: how often an active DO re-checks for idleness. */
export const ZERO_CACHE_IDLE_CHECK_MS = 30_000

/**
 * default grace window: how long connectionCount must stay 0 before teardown.
 * guards against tearing down an embed that just booted and hasn't seen its
 * first WS connect yet, and against a brief gap between a reload's disconnect
 * and reconnect.
 */
export const ZERO_CACHE_IDLE_GRACE_MS = 30_000

export interface IdleHibernationState {
  /** live sync WebSocket sessions (embed.connectionCount). */
  connectionCount: number
  /** ms since the DO last handled a request (Date.now() - lastActiveAt). */
  idleMs: number
  /** grace window in ms. */
  graceMs: number
}

/**
 * true when the DO should stop the embed and let itself hibernate: no sync
 * client connected AND idle past the grace window.
 */
export function shouldHibernateIdleZeroCache(state: IdleHibernationState): boolean {
  return state.connectionCount === 0 && state.idleMs >= state.graceMs
}
