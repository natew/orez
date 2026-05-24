import { describe, expect, it } from 'vitest'

import { handleDisabledBrowserAdminRequest } from './browser-admin.js'

describe('disabled browser admin api', () => {
  it('ignores non-admin routes', () => {
    expect(
      handleDisabledBrowserAdminRequest({
        method: 'GET',
        url: '/sync/v1/connect',
      })
    ).toBeNull()
  })

  it('serves empty logs for disabled browser admin', () => {
    const response = handleDisabledBrowserAdminRequest({
      method: 'GET',
      url: '/__orez/api/logs?limit=100',
    })

    expect(response?.status).toBe(200)
    expect(response?.headers['content-type']).toBe('application/json')
    expect(JSON.parse(response?.body ?? '')).toEqual({
      entries: [],
      cursor: 0,
      admin: 'disabled',
    })
  })

  it('serves status for disabled browser admin', () => {
    const response = handleDisabledBrowserAdminRequest({
      method: 'GET',
      url: 'http://localhost:7849/__orez/api/status',
    })

    expect(response?.status).toBe(200)
    expect(JSON.parse(response?.body ?? '')).toEqual({
      ready: true,
      admin: 'disabled',
    })
  })

  it('handles preflight and disallows writes', () => {
    expect(
      handleDisabledBrowserAdminRequest({
        method: 'OPTIONS',
        url: '/__orez/api/logs',
      })?.status
    ).toBe(200)

    const response = handleDisabledBrowserAdminRequest({
      method: 'POST',
      url: '/__orez/api/actions/restart-zero',
    })

    expect(response?.status).toBe(405)
    expect(JSON.parse(response?.body ?? '')).toEqual({
      error: 'method not allowed',
      admin: 'disabled',
    })
  })

  it('keeps unknown admin routes explicit', () => {
    const response = handleDisabledBrowserAdminRequest({
      method: 'GET',
      url: '/__orez/api/actions/restart-zero',
    })

    expect(response?.status).toBe(404)
    expect(JSON.parse(response?.body ?? '')).toEqual({
      error: 'not found',
      admin: 'disabled',
    })
  })
})
