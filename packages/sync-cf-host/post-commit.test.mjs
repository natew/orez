import { describe, expect, it, mock } from 'bun:test'

import { createPostCommitEffects } from './src/post-commit.ts'

describe('post-commit effects', () => {
  it('keeps only the latest storage-transaction attempt', async () => {
    const pending = createPostCommitEffects()
    const ran = []

    pending.beginAttempt()
    pending.defer(() => ran.push('abandoned'))
    pending.beginAttempt()
    pending.defer(() => ran.push('committed'))

    await pending.runAfterCommit(() => {})

    expect(ran).toEqual(['committed'])
  })

  it('reports effect failures, continues in order, and never rejects', async () => {
    const pending = createPostCommitEffects()
    const error = new Error('delivery failed')
    const onError = mock(() => {
      throw new Error('logger failed')
    })
    const ran = []

    pending.beginAttempt()
    pending.defer(() => ran.push('first'))
    pending.defer(() => {
      throw error
    })
    pending.defer(async () => ran.push('last'))

    await expect(pending.runAfterCommit(onError)).resolves.toBeUndefined()
    expect(onError).toHaveBeenCalledWith(error)
    expect(ran).toEqual(['first', 'last'])
    await pending.runAfterCommit(onError)
    expect(ran).toEqual(['first', 'last'])
  })
})
