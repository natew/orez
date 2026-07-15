export type DeferredEffect = () => void | Promise<void>

export type PostCommitEffects = {
  /** discard effects from a storage-transaction attempt before starting another */
  beginAttempt(): void
  defer(effect: DeferredEffect): void
  /** run the committed attempt's effects in order without rejecting */
  runAfterCommit(onError: (error: unknown) => void): Promise<void>
}

/**
 * collect external work during a retryable transaction and run only the effects
 * belonging to the attempt that commits.
 */
export function createPostCommitEffects(): PostCommitEffects {
  const effects: DeferredEffect[] = []

  return {
    beginAttempt() {
      effects.length = 0
    },
    defer(effect) {
      effects.push(effect)
    },
    async runAfterCommit(onError) {
      const committed = effects.splice(0)
      for (const effect of committed) {
        try {
          await effect()
        } catch (error) {
          try {
            onError(error)
          } catch {
            // reporting must not turn a committed effect failure into a rejection
          }
        }
      }
    },
  }
}
