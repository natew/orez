import { expect, test, vi } from 'vitest'

import { NativeSync } from './sync.js'

test('native allocation failures back off across wakes and reconnects', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
  const allocationError = new Error('std::bad_alloc (op=beginPull, requestBytes=1024)')
  const push = vi
    .fn()
    .mockResolvedValue(undefined)
    .mockResolvedValueOnce(undefined)
    .mockRejectedValueOnce(allocationError)
  const pull = vi.fn().mockResolvedValue(undefined).mockRejectedValueOnce(allocationError)
  const report = vi.fn()
  let wake: (() => void) | undefined
  const sync = new NativeSync(
    { push, pull },
    {
      subscribeWake(request) {
        wake = request
        return () => {
          wake = undefined
        }
      },
    },
    report
  )

  try {
    await vi.advanceTimersByTimeAsync(0)
    expect(push).toHaveBeenCalledTimes(1)
    expect(pull).toHaveBeenCalledTimes(1)
    expect(report).toHaveBeenCalledWith(allocationError)

    await vi.advanceTimersByTimeAsync(400)
    wake?.()
    sync.setOnline(false)
    sync.setOnline(true)
    sync.request()
    await vi.advanceTimersByTimeAsync(599)
    expect(push).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(push).toHaveBeenCalledTimes(2)
    expect(pull).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(400)
    sync.setActive(false)
    sync.setActive(true)
    wake?.()
    await vi.advanceTimersByTimeAsync(1599)
    expect(push).toHaveBeenCalledTimes(2)
    await vi.advanceTimersByTimeAsync(1)
    expect(sync.state.name).toBe('connected')
    expect(push).toHaveBeenCalledTimes(3)
    expect(report).toHaveBeenCalledTimes(2)

    wake?.()
    await vi.advanceTimersByTimeAsync(0)
    expect(push).toHaveBeenCalledTimes(4)
  } finally {
    sync.stop()
    vi.useRealTimers()
  }
})
