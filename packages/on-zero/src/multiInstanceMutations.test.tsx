// @vitest-environment jsdom
//
// mutation lifecycle across combineZeroClients, with two real Zero instances
// mounted as soot mounts them: control OUTER, project INNER. binding the
// acknowledgement helpers to ONE instance made that instance's recovery cancel
// every other instance's in-flight and queued mutations (soot incident: a
// control remint threw StaleGenerationError out of finishTurn and stalled
// scheduling for 8 minutes). acknowledgement has to follow the instance that
// issued the mutation.

import { createSchema, string, table } from '@rocicorp/zero'
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, test } from 'vitest'

import { createZeroClient } from './createZeroClient'
import { isStaleGenerationError } from './helpers/mutationLifecycle'
import { onMutationError } from './helpers/useMutation'
import { combineZeroClients, createZeroClientWithDirectQueries } from './multi'

import type { MutatorContext } from './types'
import type { ReactNode } from 'react'

declare global {
  // eslint-disable-next-line no-var
  var IS_REACT_ACT_ENVIRONMENT: boolean | undefined
}
globalThis.IS_REACT_ACT_ENVIRONMENT = true

const userTable = table('user').columns({ id: string(), name: string() }).primaryKey('id')
const taskTable = table('task')
  .columns({ id: string(), title: string() })
  .primaryKey('id')
const schema = createSchema({ tables: [userTable, taskTable] })

// every mutator body appends here as it runs on the optimistic client pass, so
// a test can tell "the write ran" from "the write was cancelled"
const writes: string[] = []

const control = createZeroClientWithDirectQueries({
  schema,
  models: {
    mutCtlUser: {
      mutate: {
        seed: async (ctx: MutatorContext, row?: { id: string; name: string }) => {
          writes.push(`control:${row!.id}`)
          await (ctx.tx.mutate as any).user.upsert(row)
        },
      },
    },
  },
  groupedQueries: {},
  instanceName: 'mutations-control',
})

const project = createZeroClient({
  schema,
  models: {
    mutPrjTask: {
      mutate: {
        seed: async (ctx: MutatorContext, row?: { id: string; title: string }) => {
          writes.push(`project:${row!.id}`)
          await (ctx.tx.mutate as any).task.upsert(row)
        },
      },
    },
  },
  groupedQueries: {},
  instanceName: 'mutations-project',
})

const combined = combineZeroClients(control, project)

const controlWrite = (id: string) =>
  (combined.zero.mutate as any).mutCtlUser.seed({ id, name: id })
const projectWrite = (id: string) =>
  (combined.zero.mutate as any).mutPrjTask.seed({ id, title: id })

let root: Root
let container: HTMLElement
let stopErrorSink: () => void

beforeEach(() => {
  writes.length = 0
  // closing an instance rejects its in-flight server promise; the app-level
  // sink is what would toast it, and these tests assert on the lifecycle
  stopErrorSink = onMutationError(() => {})
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
})

afterEach(async () => {
  await act(async () => root.unmount())
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 20))
  })
  container.remove()
  stopErrorSink()
})

const render = (ui: ReactNode) => act(async () => root.render(ui))
const tick = () => act(async () => new Promise((resolve) => setTimeout(resolve, 0)))

const App = ({
  controlUser,
  projectUser,
}: {
  controlUser: string
  projectUser: string
}) => (
  <control.ProvideZero server={null} userID={controlUser}>
    <project.ProvideZero server={null} userID={projectUser}>
      <div />
    </project.ProvideZero>
  </control.ProvideZero>
)

async function mount(controlUser: string, projectUser: string) {
  await render(<App controlUser={controlUser} projectUser={projectUser} />)
  await act(async () => {
    await Promise.all([control.waitForZero(), project.waitForZero()])
  })
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

// settlement state without awaiting: a pending acknowledgement is the point
function track(promise: Promise<unknown>) {
  const state: { status: 'pending' | 'resolved' | 'rejected'; error?: unknown } = {
    status: 'pending',
  }
  promise.then(
    () => {
      state.status = 'resolved'
    },
    (error: unknown) => {
      state.status = 'rejected'
      state.error = error
    }
  )
  return state
}

test('recovering one instance cancels only that instance mutation acknowledgement', async () => {
  await mount('ctl-1', 'prj-1')

  // no server is configured, so both server acknowledgements stay in flight
  const controlAck = track(
    combined.awaitMutationServer(controlWrite('c1'), 'control write', 2000)
  )
  const projectAck = track(
    combined.awaitMutationServer(projectWrite('p1'), 'project write', 2000)
  )
  await tick()
  expect(controlAck.status).toBe('pending')
  expect(projectAck.status).toBe('pending')

  // control rotates its instance (recovery / remint / identity change)
  await mount('ctl-2', 'prj-1')
  await tick()

  expect(controlAck.status).toBe('rejected')
  expect(isStaleGenerationError(controlAck.error)).toBe(true)
  expect(projectAck.status).toBe('pending')

  // and the project acknowledgement is still fenced by its OWN instance
  await mount('ctl-2', 'prj-2')
  await tick()
  expect(projectAck.status).toBe('rejected')
  expect(isStaleGenerationError(projectAck.error)).toBe(true)
})

test('a mutation issued before a rotation never settles on the replacement', async () => {
  await mount('ctl-3', 'prj-3')

  const mutation = controlWrite('c2')
  await mount('ctl-4', 'prj-3')

  // the client that could settle this call is gone; waiting out the timeout on
  // the replacement instance is the failure this replaces
  await expect(
    combined.awaitMutationClient(mutation, 'control write', 2000)
  ).rejects.toSatisfy(isStaleGenerationError)
})

test('a per-instance helper hands a foreign mutation to its owner', async () => {
  await mount('ctl-13', 'prj-13')

  // the wrong client to settle a project write: it must pass it to project
  // rather than fence it on its own generation
  const ack = track(
    control.awaitMutationServer(projectWrite('p6'), 'project write', 5000)
  )
  await tick()

  await mount('ctl-14', 'prj-13')
  await tick()
  expect(ack.status).toBe('pending')

  await mount('ctl-14', 'prj-14')
  await tick()
  expect(ack.status).toBe('rejected')
  expect(isStaleGenerationError(ack.error)).toBe(true)
})

test('a create that awaits before writing is not pinned to its queued generation', async () => {
  await mount('ctl-11', 'prj-11')

  const release = deferred<void>()
  const queued = combined.enqueueBackgroundMutation(
    'control read then write',
    async () => {
      await release.promise
      return controlWrite('c7')
    }
  )

  // documented boundary: the pin covers the synchronous window in which
  // create() calls zero.mutate, so a create that awaits first writes on
  // whichever instance is live when it fires and is acknowledged there
  await mount('ctl-12', 'prj-11')
  release.resolve()
  await act(async () => {
    await queued
  })

  expect(writes).toEqual(['control:c7'])
})

test('a fenced instance drops only its own queued background writes', async () => {
  await mount('ctl-5', 'prj-5')

  const blocker = deferred<void>()
  const blocking = combined.enqueueBackgroundMutation('blocker', () => blocker.promise)
  const queuedControl = combined.enqueueBackgroundMutation('control queued', () =>
    controlWrite('c3')
  )
  const queuedProject = combined.enqueueBackgroundMutation('project queued', () =>
    projectWrite('p3')
  )

  // control recovers while both writes are still behind the blocker
  await mount('ctl-6', 'prj-5')
  blocker.resolve()
  await act(async () => {
    await Promise.all([blocking, queuedControl, queuedProject])
  })

  // the control write is dropped rather than replayed onto the fresh control
  // client, and the project write is untouched by control's recovery
  expect(writes).toEqual(['project:p3'])
  await expect(queuedControl).resolves.toBeUndefined()
  await expect(queuedProject).resolves.toBeUndefined()
})

test('queued writes survive a rotation of an instance they do not touch', async () => {
  await mount('ctl-7', 'prj-7')

  const blocker = deferred<void>()
  const blocking = combined.enqueueBackgroundMutation('blocker', () => blocker.promise)
  const queuedProject = combined.enqueueBackgroundMutation(
    'project queued',
    () => projectWrite('p4'),
    { settle: 'client' }
  )

  await mount('ctl-8', 'prj-7')
  blocker.resolve()
  await act(async () => {
    await Promise.all([blocking, queuedProject])
  })

  expect(writes).toEqual(['project:p4'])
})

test('the combined queue stays one serial queue across instances', async () => {
  await mount('ctl-9', 'prj-9')

  const blocker = deferred<void>()
  const blocking = combined.enqueueBackgroundMutation('blocker', () => blocker.promise)
  // same coalesce key from two different instances: one queue means the older
  // write is superseded, two queues would run both
  const superseded = combined.enqueueBackgroundMutation(
    'control coalesced',
    () => controlWrite('c5'),
    { coalesceKey: 'row-1' }
  )
  const latest = combined.enqueueBackgroundMutation(
    'project coalesced',
    () => projectWrite('p5'),
    { coalesceKey: 'row-1' }
  )
  const trailing = combined.enqueueBackgroundMutation('control trailing', () =>
    controlWrite('c6')
  )

  blocker.resolve()
  await act(async () => {
    await Promise.all([blocking, superseded, latest, trailing])
  })

  expect(writes).toEqual(['project:p5', 'control:c6'])
})
