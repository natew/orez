import { expect, test } from 'bun:test'

import { createOperationBoundDropFetch, observedSyncFetch } from './observed-fetch.js'

test('observer failure emits no duplicate transport terminal', async () => {
  const phases: string[] = []
  const wrapped = observedSyncFetch(
    (observation) => {
      phases.push(observation.phase)
      if (observation.phase === 'terminal') throw new Error('collector failed')
    },
    (async () => Response.json({ ok: true })) as typeof fetch
  )
  await expect(
    wrapped('http://localhost/push', {
      method: 'POST',
      body: JSON.stringify({ value: 1 }),
    })
  ).rejects.toThrow('collector failed')
  expect(phases).toEqual(['invoke', 'terminal'])
})

test('separate wrappers retain provenance with overlapping request ids', async () => {
  const seen: string[] = []
  const transport = (async () => Response.json({ ok: true })) as typeof fetch
  const stock = observedSyncFetch(
    (observation) => seen.push(`stock:${observation.request}:${observation.phase}`),
    transport
  )
  const harness = observedSyncFetch(
    (observation) => seen.push(`harness:${observation.request}:${observation.phase}`),
    transport
  )
  await Promise.all([
    stock('http://localhost/push', { method: 'POST', body: '{}' }),
    harness('http://localhost/push', { method: 'POST', body: '{}' }),
  ])
  expect(seen).toContain('stock:1:invoke')
  expect(seen).toContain('stock:1:terminal')
  expect(seen).toContain('harness:1:invoke')
  expect(seen).toContain('harness:1:terminal')
})

test('response body stream failure is one terminal transport error', async () => {
  const observations: Array<{ phase: string; error?: unknown }> = []
  const stream = new ReadableStream({
    start(controller) {
      controller.error(new Error('body stream destroyed'))
    },
  })
  const wrapped = observedSyncFetch(
    (observation) => observations.push(observation),
    (async () => new Response(stream)) as typeof fetch
  )
  await expect(
    wrapped('http://localhost/push', { method: 'POST', body: '{}' })
  ).rejects.toThrow('body stream destroyed')
  expect(observations.map(({ phase }) => phase)).toEqual(['invoke', 'terminal'])
  expect(observations[1]!.error).toBeInstanceOf(Error)
})

test('operation-bound drop token is exact and one-shot', async () => {
  const consumed: string[] = []
  let responseToken: string | undefined = 'token-1'
  const controller = createOperationBoundDropFetch(
    (token) => consumed.push(token),
    (async () =>
      new Response('{}', {
        headers: responseToken ? { 'x-orez-drop-token': responseToken } : {},
      })) as typeof fetch
  )
  controller.arm('token-1')
  await expect(
    controller.fetch('http://localhost/push', { method: 'POST', body: '{}' })
  ).rejects.toThrow('operation-bound post-commit response loss')
  expect(consumed).toEqual(['token-1'])
  await expect(
    controller.fetch('http://localhost/push', { method: 'POST', body: '{}' })
  ).rejects.toThrow('unarmed or reused')

  responseToken = undefined
  controller.arm('token-2')
  await expect(
    controller.fetch('http://localhost/push', { method: 'POST', body: '{}' })
  ).rejects.toThrow('missing its drop token')

  const wrong = createOperationBoundDropFetch(
    () => {},
    (async () =>
      new Response('{}', { headers: { 'x-orez-drop-token': 'wrong' } })) as typeof fetch
  )
  wrong.arm('expected')
  await expect(
    wrong.fetch('http://localhost/push', { method: 'POST', body: '{}' })
  ).rejects.toThrow('does not match')
})
