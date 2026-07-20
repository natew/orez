import type { DeferredEffect, DeferredEffectOptions, EffectScheduler } from './types.js'

export type EffectAttempt = {
  readonly defer: (effect: DeferredEffect, options?: DeferredEffectOptions) => void
  close(): void
  entries(): readonly { readonly effect: DeferredEffect; readonly barrier: boolean }[]
}

export function createEffectAttempt(): EffectAttempt {
  const effects: { effect: DeferredEffect; barrier: boolean }[] = []
  let open = true
  return {
    defer(effect, options) {
      if (!open) throw new Error('cannot defer an effect after the transaction attempt')
      effects.push({ effect, barrier: options?.barrier === true })
    },
    close() {
      open = false
    },
    entries() {
      return effects
    },
  }
}

export async function runCommittedEffects(
  entries: readonly { readonly effect: DeferredEffect; readonly barrier: boolean }[],
  scheduler: EffectScheduler
): Promise<void> {
  const barriers = entries.filter((entry) => entry.barrier)
  await Promise.all(barriers.map(({ effect }) => Promise.resolve().then(effect)))

  const background = entries.filter((entry) => !entry.barrier)
  if (background.length === 0) return
  const promise = Promise.allSettled(
    background.map(({ effect }) => Promise.resolve().then(effect))
  ).then((results) => {
    for (const result of results) {
      if (result.status === 'rejected') scheduler.report(result.reason)
    }
  })
  await scheduler.runBackground(promise)
}
