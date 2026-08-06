import { isInZeroMutation, mutatorContext } from './helpers/mutatorContext'

import type {
  HumanReadable,
  Query,
  RunOptions,
  Schema as ZeroSchema,
} from '@rocicorp/zero'

export type { RunOptions }

export type ZeroRunner = <TReturn>(
  query: Query<any, ZeroSchema, TReturn>,
  options?: RunOptions
) => Promise<HumanReadable<TReturn>>

let runner: ZeroRunner | null = null

export function setRunner(r: ZeroRunner | null) {
  runner = r
}

export function getRunner(instance?: { runner: ZeroRunner | null }): ZeroRunner {
  // only the server has a real async context, so only there can "am I inside a
  // mutator" be answered for the CALLER rather than for the page. on the client
  // isInZeroMutation() is always false by construction (see asyncContext.ts):
  // ordinary async code that happens to overlap a mutator must not be handed
  // that mutator's transaction, and a client mutator that needs a transactional
  // read uses its own `tx.run(zql...)`.
  if (isInZeroMutation()) {
    return (q, o) => mutatorContext().tx.run(q, o)
  }

  return getAmbientRunner(instance)
}

export function getAmbientRunner(instance?: { runner: ZeroRunner | null }): ZeroRunner {
  // a mounted instance's own runner wins; otherwise the ambient runner
  // (single-instance client, or the server transaction runner)
  if (instance?.runner) {
    return instance.runner
  }

  if (!runner) {
    throw new Error(
      'Zero runner not initialized. Ensure ProvideZero is mounted, connectHeadless() was called, or server bindings are active.'
    )
  }

  return runner
}
