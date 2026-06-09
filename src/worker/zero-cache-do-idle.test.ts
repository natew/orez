import { describe, expect, it } from 'vitest'

import {
  shouldHibernateIdleZeroCache,
  ZERO_CACHE_IDLE_GRACE_MS,
} from './zero-cache-do-idle.js'

const grace = ZERO_CACHE_IDLE_GRACE_MS

describe('shouldHibernateIdleZeroCache', () => {
  it('hibernates when no client connected and idle past the grace window', () => {
    expect(
      shouldHibernateIdleZeroCache({ connectionCount: 0, idleMs: grace, graceMs: grace })
    ).toBe(true)
    expect(
      shouldHibernateIdleZeroCache({
        connectionCount: 0,
        idleMs: grace + 5_000,
        graceMs: grace,
      })
    ).toBe(true)
  })

  it('never hibernates while a sync client is connected', () => {
    expect(
      shouldHibernateIdleZeroCache({
        connectionCount: 1,
        idleMs: grace * 100,
        graceMs: grace,
      })
    ).toBe(false)
    expect(
      shouldHibernateIdleZeroCache({
        connectionCount: 3,
        idleMs: grace * 100,
        graceMs: grace,
      })
    ).toBe(false)
  })

  it('waits out the grace window before tearing down a freshly idle embed', () => {
    // a just-booted embed (or a reload gap) is idle with 0 connections but must
    // not be torn down until grace elapses, or every page reload pays a cold
    // start.
    expect(
      shouldHibernateIdleZeroCache({ connectionCount: 0, idleMs: 0, graceMs: grace })
    ).toBe(false)
    expect(
      shouldHibernateIdleZeroCache({
        connectionCount: 0,
        idleMs: grace - 1,
        graceMs: grace,
      })
    ).toBe(false)
  })
})
