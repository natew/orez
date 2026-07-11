import { expect, test } from 'bun:test'

import { observedSyncFetch } from './observed-fetch.js'

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
