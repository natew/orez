import { test, expect } from '@playwright/test'

import { navigateTo, waitForApp } from './helpers'

test.describe('connectivity', () => {
  test('server responds on root', async ({ request }) => {
    const response = await request.get('/')
    expect(response.status()).toBe(200)
    expect(response.headers()['content-type']).toContain('text/html')
  })

  test('health endpoint returns ok', async ({ request }) => {
    const response = await request.get('/api/health')
    expect(response.status()).toBe(200)
    const body = await response.json()
    expect(body.status).toBe('ok')
    expect(body.timestamp).toBeTruthy()
  })

  test('todos API returns array', async ({ request }) => {
    const response = await request.get('/api/todos')
    expect(response.status()).toBe(200)
    const body = await response.json()
    expect(Array.isArray(body)).toBe(true)
  })

  test('404 for unknown routes', async ({ request }) => {
    const response = await request.get('/api/nonexistent')
    expect(response.status()).toBe(404)
  })

  test('page loads with app container', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    const title = await page.textContent('h1')
    expect(title).toBe('orez demo')
  })
})
