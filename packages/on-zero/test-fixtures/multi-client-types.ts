import { createSchema, string, table } from '@rocicorp/zero'
import { createZeroClients, type ZeroInstanceManifestEntry } from 'on-zero/multi'

import type { MutatorContext, ZeroEventsEmitter } from 'on-zero'

const schema = createSchema({
  tables: [
    table('account').columns({ id: string() }).primaryKey('id'),
    table('message').columns({ id: string() }).primaryKey('id'),
  ],
})

declare module 'on-zero' {
  interface Config {
    schema: typeof schema
  }
}

type AccountModels = {
  account: {
    mutate: {
      insert: (context: MutatorContext, row: { id: string }) => Promise<void>
    }
  }
}

type MessageModels = {
  message: {
    mutate: {
      insert: (
        context: MutatorContext,
        row: { id: string; body: string }
      ) => Promise<void>
    }
  }
}

declare const account: ZeroInstanceManifestEntry<typeof schema, AccountModels>
declare const message: ZeroInstanceManifestEntry<typeof schema, MessageModels>

const { clients, combined } = createZeroClients({ default: account, project: message })
const publicEvents: ZeroEventsEmitter = combined.zeroEvents

void publicEvents

void clients.default.zero.mutate.account.insert({ id: 'account-1' })
void clients.project.zero.mutate.message.insert({ id: 'message-1', body: 'hello' })
void combined.zero.mutate.account.insert({ id: 'account-1' })
void combined.zero.mutate.message.insert({ id: 'message-1', body: 'hello' })

// @ts-expect-error the combined facade preserves each namespace's row type
void combined.zero.mutate.message.insert({ id: 'message-1', body: 42 })
// @ts-expect-error namespaces outside the instance manifests are not exposed
void combined.zero.mutate.unknown.insert({ id: 'unknown-1' })
