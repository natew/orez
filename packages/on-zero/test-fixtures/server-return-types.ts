import { string, table } from 'limesync/zero'
import { mutations, serverWhere } from 'on-zero'
import { createZeroServerBindings, type ZeroServerExecutor } from 'on-zero/server'

import type { Schema as ZeroSchema } from 'limesync/zero'
import type { Config, MutatorContext } from 'on-zero'

type AppAuthData = {
  email: string
  id: string
  role: 'admin' | undefined
}

type AppAsyncAction =
  | { type: 'project.provisionNamespace'; projectId: string }
  | { type: 'project.invalidateAccess'; projectId: string }

declare module 'on-zero' {
  interface Config {
    authData: AppAuthData
    asyncAction: AppAsyncAction
  }
}

type Models = {
  project: {
    mutate: {
      create: (context: MutatorContext, input: { id: string }) => Promise<void>
    }
  }
}

type Actions = Record<string, never>

const bindings = createZeroServerBindings<ZeroSchema, Models, Actions>({
  schema: {} as ZeroSchema,
  models: {} as Models,
  createServerActions: () => ({}),
})

declare const executor: ZeroServerExecutor<ZeroSchema>
const server = bindings.server(executor)

const authData: Config['authData'] = {
  email: 'admin@example.com',
  id: 'admin',
  role: 'admin',
}
void bindings.resolveQuery('project|byID', [{ id: 'project' }], authData)
void server.mutate.project.create({ id: 'project' }, { authData })
void server.transaction(authData, async (tx) => tx.location)
void server.query(authData, () => null as never)

// @ts-expect-error server APIs accept application auth, not sync claims
void server.query({ userID: authData.id }, () => null as never)

declare const mutationContext: MutatorContext
mutationContext.server?.enqueueAction({
  type: 'project.provisionNamespace',
  projectId: 'project',
})
// @ts-expect-error async actions are a closed discriminated union
mutationContext.server?.enqueueAction({ type: 'project.unknown', projectId: 'project' })

// @ts-expect-error mutation argument types remain enforced through the server facade
void server.mutate.project.create({ id: 42 }, { authData })

const record = table('account').columns({ id: string() }).primaryKey('id')
const permission = serverWhere('account', () => true)
const open = mutations(record, permission, {})
void open.insert(mutationContext, { id: 'record' })
const named = mutations('account', permission, {})
void named.upsert(mutationContext, { id: 'record' })
const closed = mutations(
  record,
  permission,
  {
    save: async (_ctx, input: { id: string }) => {
      void input
    },
  },
  { crud: false }
)
void closed.save(mutationContext, { id: 'record' })
// @ts-expect-error opt-out exposes only explicitly declared handlers
void closed.insert(mutationContext, { id: 'record' })
const namedClosed = mutations('account', permission, {}, { crud: false })
// @ts-expect-error named table opt-out also omits default handlers
void namedClosed.upsert(mutationContext, { id: 'record' })
const customCRUD = mutations(
  record,
  permission,
  {
    insert: async (_ctx, input: { label: string }) => {
      void input
    },
  },
  { crud: false }
)
void customCRUD.insert(mutationContext, { label: 'custom' })
// @ts-expect-error custom CRUD retains its own argument contract
void customCRUD.insert(mutationContext, { id: 'record' })
