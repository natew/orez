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
