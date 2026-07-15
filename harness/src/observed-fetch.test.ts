import { expect, test } from 'bun:test'

import {
  createOperationBoundDropFetch,
  createPullQuiescenceFetch,
  observedSyncFetch,
  PullAbortedByQuiesceControllerError,
} from './observed-fetch.js'

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

test('pull quiescence controller aborts only its pending pulls', async () => {
  const transport = (async (input, init) => {
    const url = new URL(typeof input === 'string' ? input : input.toString())
    if (url.pathname.endsWith('/push')) return Response.json({ pushed: true })
    if (url.searchParams.has('complete')) return Response.json({ pulled: true })
    return await new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener('abort', () => reject(new Error('aborted')), {
        once: true,
      })
    })
  }) as typeof fetch
  const controller = createPullQuiescenceFetch(transport)
  const completed = await controller.fetch('http://localhost/pull?complete=1')
  expect(await completed.json()).toEqual({ pulled: true })
  const first = controller.fetch('http://localhost/pull?id=1')
  const second = controller.fetch('http://localhost/pull?id=2')
  const aborted = Promise.allSettled([first, second])
  expect(controller.pendingPullCount()).toBe(2)
  expect(controller.abortPendingPulls()).toBe(2)
  expect(controller.abortPendingPulls()).toBe(0)
  const results = await aborted
  for (const result of results) {
    expect(result).toMatchObject({ status: 'rejected' })
    if (result.status === 'rejected')
      expect(result.reason).toBeInstanceOf(PullAbortedByQuiesceControllerError)
  }
  expect(controller.pendingPullCount()).toBe(0)
  expect(controller.abortPendingPulls()).toBe(0)
  await expect(controller.fetch('http://localhost/pull?late=1')).rejects.toThrow(
    'after quiescence controller sealed'
  )
  expect(await (await controller.fetch('http://localhost/push')).json()).toEqual({
    pushed: true,
  })

  const caller = new AbortController()
  const callerPull = controller.fetch('http://localhost/pull?caller=1', {
    signal: caller.signal,
  })
  const callerResult = Promise.allSettled([callerPull])
  caller.abort()
  expect(controller.abortPendingPulls()).toBe(0)
  const [callerSettled] = await callerResult
  expect(callerSettled).toMatchObject({ status: 'rejected' })
  if (callerSettled!.status === 'rejected') {
    expect(callerSettled.reason).toBeInstanceOf(Error)
    expect(callerSettled.reason).not.toBeInstanceOf(PullAbortedByQuiesceControllerError)
  }
})
