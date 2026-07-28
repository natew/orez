// The harness Zero schema and its streaming manifest.
//
// Split out because both runtimes need them and only one of them can load
// 'cloudflare:workers': harness-config reaches the host, so a Bun-side lane
// importing it would fail on that import alone.

import { defineStreamingFields } from 'orez-lite/realtime'

import type { Schema } from '@rocicorp/zero'

export const harnessSchema = {
  tables: {
    user: {
      name: 'user',
      columns: { id: { type: 'string' }, name: { type: 'string' } },
      primaryKey: ['id'],
    },
    project: {
      name: 'project',
      columns: {
        id: { type: 'string' },
        ownerId: { type: 'string' },
        name: { type: 'string' },
      },
      primaryKey: ['id'],
    },
    member: {
      name: 'member',
      columns: {
        id: { type: 'string' },
        projectId: { type: 'string' },
        userId: { type: 'string' },
      },
      primaryKey: ['id'],
    },
    task: {
      name: 'task',
      columns: {
        id: { type: 'string' },
        projectId: { type: 'string' },
        title: { type: 'string' },
        rank: { type: 'number' },
        done: { type: 'boolean' },
        meta: { type: 'json' },
        dueAt: { type: 'number' },
      },
      primaryKey: ['id'],
    },
    message: {
      name: 'message',
      columns: {
        id: { type: 'string' },
        serverId: { type: 'string' },
        channelId: { type: 'string' },
        creatorId: { type: 'string' },
        content: { type: 'string' },
        type: { type: 'string' },
        createdAt: { type: 'number' },
        order: { type: 'string' },
        meta: { type: 'json' },
      },
      primaryKey: ['id'],
    },
  },
  relationships: {},
} as const satisfies Schema

// One streaming field, on `task.title`, because the harness client fixture
// actually syncs task rows: a subscription is authorized against the client
// group's real durable membership, so the field has to live on a row a real
// client can hold. That makes this the whole cycle against a real Durable
// Object rather than a mock of it.
export const harnessStreaming = defineStreamingFields(harnessSchema, {
  task: {
    title: { maxBytes: 100_000, maxUpdatesPerSecond: 60, maxBytesPerSecond: 500_000 },
  },
})
