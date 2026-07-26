import { describe, expect, it } from 'vitest'

import { normalizeOrezLiteFeedBody } from './lite-worker.js'

describe('Orez Lite Cloudflare worker runtime', () => {
  it('projects public feed columns and converts the private mutation cursor', () => {
    const body = {
      tables: {
        'public.message': [{ id: 'm1', text: 'hello', internalScore: 42 }],
      },
      changes: [
        {
          watermark: '01',
          tableName: 'public.message',
          op: 'INSERT',
          rowData: { id: 'm2', text: 'world', internalScore: 99 },
          oldData: null,
        },
        {
          watermark: '02',
          tableName: '_zsync_clients',
          op: 'UPDATE',
          rowData: { id: 'private' },
          oldData: null,
        },
      ],
    }

    expect(
      normalizeOrezLiteFeedBody(
        {
          message: ['id', 'text'],
          syncCursor: ['id', 'watermark'],
        },
        body
      )
    ).toEqual({
      tables: {
        message: [{ id: 'm1', text: 'hello' }],
      },
      changes: [
        {
          watermark: '01',
          tableName: 'message',
          op: 'INSERT',
          rowData: { id: 'm2', text: 'world' },
          oldData: null,
        },
        {
          watermark: '02',
          tableName: 'syncCursor',
          op: 'INSERT',
          rowData: { id: 'zero-http', watermark: '02' },
          oldData: null,
        },
      ],
    })
  })
})
