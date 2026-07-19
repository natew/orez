import { randomBytes as nodeRandomBytes } from 'node:crypto'

import { describe, expect, test, vi } from 'vitest'

import {
  createEncryptedColumnCodec,
  generateX25519KeyPair,
  hpkeSeal,
  unwrapContentKey,
  wrapContentKey,
} from './encrypted-column-codec.js'
import { runEncryptionConformance } from './encrypted-column-conformance.js'

import type {
  EncryptedColumnManifest,
  EncryptionKeyring,
  PullResponse,
  PushRequest,
} from './payload-codec.js'

const manifest = {
  version: 1,
  networkID: 'network-test',
  schemaID: 'schema-test',
  rowMutations: {
    'cloud.applyBatch': {
      argumentIndex: 0,
      format: 'orez-row-batch-v1',
    },
  },
  tables: {
    message: {
      serverName: 'message_record',
      primaryKey: ['id'],
      columns: {
        body: { serverName: 'body_cipher' },
        detail: { serverName: 'detail_cipher' },
      },
    },
  },
} as const satisfies EncryptedColumnManifest

const contentKey = Uint8Array.from({ length: 32 }, (_, index) => index)

describe('orez-e1 encrypted column codec', () => {
  test('matches the portable Orez column and RFC 9180 known-answer entry', async () => {
    await expect(runEncryptionConformance()).resolves.toEqual({
      codec: 'orez-e1',
      columnVector: 'pass',
      hpkeVector: 'RFC 9180 A.2.1',
    })
  })

  test('encrypts only declared row payload columns and round-trips string and JSON values', async () => {
    const codec = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(contentKey),
    })
    const body = pushRequest()
    const original = JSON.parse(JSON.stringify(body))

    const encoded = await codec.encodePush(body)

    expect(body).toEqual(original)
    expect(encoded).not.toBe(body)
    expect(encoded.mutations[1]).toBe(body.mutations[1])
    expect(encoded.mutations[1]).toEqual({
      type: 'custom',
      name: 'session.sendCommand',
      clientID: 'client-a',
      id: 8,
      args: [{ nested: { confidential: 'command payload stays explicit' } }],
    })

    const put = encodedPut(encoded)
    expect(put.id).toBe('message-1')
    expect(put.roomID).toBe('room-1')
    expect(put.body).toMatch(/^orez-e1\.3\.[A-Za-z0-9_-]{16}\./)
    expect(put.detail).toMatch(/^orez-e1\.3\.[A-Za-z0-9_-]{16}\./)
    expect(put.body).not.toBe(put.detail)
    expect(encodedRows(encoded)[1]).toEqual({
      seq: 42,
      op: 'del',
      table: 'message',
      key: { id: 'message-old' },
    })

    const pulled: PullResponse = {
      cookie: 4,
      lastMutationIDChanges: {},
      rowsPatch: [
        { op: 'clear' },
        {
          op: 'put',
          tableName: 'message_record',
          value: {
            id: 'message-1',
            room_id: 'room-1',
            body_cipher: put.body,
            detail_cipher: put.detail,
          },
        },
      ],
    }
    const decoded = await codec.decodePull(pulled)

    expect(pulled.rowsPatch[1]).not.toEqual(decoded.rowsPatch[1])
    expect(decoded.rowsPatch[1]).toEqual({
      op: 'put',
      tableName: 'message_record',
      value: {
        id: 'message-1',
        room_id: 'room-1',
        body_cipher: 'hello encrypted world',
        detail_cipher: { reactions: ['wave', 'spark'], count: 2 },
      },
    })
  })

  test('is byte-stable across retries and changes every cell when the mutation changes', async () => {
    const current = vi.fn(async () => ({ epoch: 3, key: contentKey }))
    const codec = createEncryptedColumnCodec({
      manifest,
      keyring: { current, get: async () => contentKey },
    })
    const body = pushRequest()

    const first = await codec.encodePush(body)
    const second = await codec.encodePush(body)
    const retry = await codec.encodePush(first)
    const changedMutation = await codec.encodePush(pushRequest(9))

    expect(first).toEqual(second)
    expect(retry).toBe(first)
    expect(current).toHaveBeenCalledTimes(3)
    expect(encodedPut(changedMutation).body).not.toBe(encodedPut(first).body)
    expect(encodedPut(changedMutation).detail).not.toBe(encodedPut(first).detail)
  })

  test('derives distinct nonces for every column and row in one mutation', async () => {
    const body = JSON.parse(JSON.stringify(pushRequest())) as PushRequest
    const mutation = body.mutations[0]
    const batch = mutation.args?.[0] as {
      rows: Array<Record<string, unknown>>
    }
    const firstRow = batch.rows[0]
    batch.rows[1] = {
      ...firstRow,
      seq: 42,
      value: {
        ...(firstRow.value as Record<string, unknown>),
        id: 'message-2',
      },
    }
    const codec = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(contentKey),
    })

    const encoded = await codec.encodePush(body)
    const values = encodedRows(encoded).map((row) => row.value as Record<string, unknown>)
    const nonces = [
      envelopeNonce(values[0].body),
      envelopeNonce(values[0].detail),
      envelopeNonce(values[1].body),
      envelopeNonce(values[1].detail),
    ]

    expect(new Set(nonces).size).toBe(4)
  })

  test('authenticates valid retry envelopes without requiring a current write key', async () => {
    const enrolled = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(contentKey),
    })
    const encoded = await enrolled.encodePush(pushRequest())
    const current = vi.fn(async () => undefined)
    const get = vi.fn(async () => contentKey)
    const noWriteKey = createEncryptedColumnCodec({
      manifest,
      keyring: { current, get },
    })

    await expect(noWriteKey.encodePush(encoded)).resolves.toBe(encoded)
    expect(current).not.toHaveBeenCalled()
    expect(get).toHaveBeenCalledWith(3)
  })

  test('rejects retry envelopes that cannot be authenticated', async () => {
    const enrolled = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(contentKey),
    })
    const encoded = await enrolled.encodePush(pushRequest())
    const forged = JSON.parse(JSON.stringify(encoded)) as PushRequest
    encodedPut(forged).body = tamperEnvelope(encodedPut(forged).body)
    const noWriteKey = createEncryptedColumnCodec({
      manifest,
      keyring: { current: async () => undefined, get: async () => contentKey },
    })

    await expect(noWriteKey.encodePush(forged)).rejects.toThrow('authentication failed')
  })

  test('rejects retry envelopes when their historical key is unavailable', async () => {
    const enrolled = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(contentKey),
    })
    const encoded = await enrolled.encodePush(pushRequest())
    const noHistoricalKey = createEncryptedColumnCodec({
      manifest,
      keyring: { current: async () => undefined, get: async () => undefined },
    })

    await expect(noHistoricalKey.encodePush(encoded)).rejects.toThrow(
      'no key is available for encoded retry'
    )
  })

  test('rejects malformed retry envelopes instead of encrypting them as plaintext', async () => {
    const malformed = pushRequest()
    encodedPut(malformed).body = 'orez-e1.3.invalid'
    const codec = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(contentKey),
    })

    await expect(codec.encodePush(malformed)).rejects.toThrow('envelope is malformed')
  })

  test('fails closed before transport when a plaintext row has no current key', async () => {
    const codec = createEncryptedColumnCodec({
      manifest,
      keyring: { current: async () => undefined, get: async () => undefined },
    })

    await expect(codec.encodePush(pushRequest())).rejects.toThrow(
      'no current encryption key is available'
    )
  })

  test('rejects conflicting duplicate custom mutation identities', async () => {
    const codec = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(contentKey),
    })
    const first = pushRequest().mutations[0]
    const conflicting = {
      ...first,
      args: [{ ...first.args?.[0], throughSeq: 99 }],
    }
    const body = {
      ...pushRequest(),
      mutations: [first, conflicting],
    } as PushRequest

    await expect(codec.encodePush(body)).rejects.toThrow(
      'conflicting duplicate custom mutations'
    )
  })

  test('leaves ciphertext unchanged for a missing historical key', async () => {
    const enrolled = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(contentKey),
    })
    const put = encodedPut(await enrolled.encodePush(pushRequest()))
    const ciphertextPull = pullWithBody(put.body)
    const noReadKey = createEncryptedColumnCodec({
      manifest,
      keyring: { current: async () => undefined, get: async () => undefined },
    })

    await expect(noReadKey.decodePull(ciphertextPull)).resolves.toBe(ciphertextPull)
  })

  test('reads logical push keys and renamed physical pull keys', async () => {
    const renamedPrimaryKeyManifest = {
      ...manifest,
      tables: {
        message: {
          ...manifest.tables.message,
          primaryKey: ['messageID'],
          primaryKeyServerNames: { messageID: 'message_id' },
        },
      },
    } as const satisfies EncryptedColumnManifest
    const codec = createEncryptedColumnCodec({
      manifest: renamedPrimaryKeyManifest,
      keyring: keyring(contentKey),
    })
    const body = pushRequest()
    const put = encodedPut(body)
    put.messageID = put.id
    delete put.id

    const encrypted = encodedPut(await codec.encodePush(body)).body
    const decoded = await codec.decodePull({
      cookie: 4,
      lastMutationIDChanges: {},
      rowsPatch: [
        {
          op: 'put',
          tableName: 'message_record',
          value: { message_id: 'message-1', body_cipher: encrypted },
        },
      ],
    })

    expect(decoded.rowsPatch[0]).toEqual({
      op: 'put',
      tableName: 'message_record',
      value: { message_id: 'message-1', body_cipher: 'hello encrypted world' },
    })
  })

  test('rejects modified ciphertext, wrong keys, and malformed envelopes', async () => {
    const codec = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(contentKey),
    })
    const encrypted = encodedPut(await codec.encodePush(pushRequest())).body as string
    const payloadStart = encrypted.lastIndexOf('.') + 1
    const replacement = encrypted[payloadStart] === 'A' ? 'B' : 'A'
    const modified =
      encrypted.slice(0, payloadStart) + replacement + encrypted.slice(payloadStart + 1)

    await expect(codec.decodePull(pullWithBody(modified))).rejects.toThrow(
      'authentication failed'
    )

    const wrongKeyCodec = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(new Uint8Array(32).fill(99)),
    })
    await expect(wrongKeyCodec.decodePull(pullWithBody(encrypted))).rejects.toThrow(
      'authentication failed'
    )

    await expect(codec.decodePull(pullWithBody('orez-e1.3.invalid'))).rejects.toThrow(
      'envelope is malformed'
    )
    await expect(
      codec.decodePull(pullWithBody('edge plaintext injection'))
    ).rejects.toThrow('plaintext in encrypted column body')
    await expect(
      codec.decodePull({
        cookie: 4,
        unchanged: 'true',
      } as unknown as PullResponse)
    ).rejects.toThrow('invalid pull response')
  })

  test('rejects ambiguous manifests and encrypted primary keys at construction', () => {
    expect(() =>
      createEncryptedColumnCodec({
        manifest: {
          ...manifest,
          tables: {
            message: {
              ...manifest.tables.message,
              columns: { id: {} },
            },
          },
        },
        keyring: keyring(contentKey),
      })
    ).toThrow('cannot encrypt primary key')

    expect(() =>
      createEncryptedColumnCodec({
        manifest: {
          ...manifest,
          tables: {
            message: manifest.tables.message,
            duplicate: {
              ...manifest.tables.message,
              primaryKey: ['duplicate_id'],
            },
          },
        },
        keyring: keyring(contentKey),
      })
    ).toThrow('ambiguous physical table')

    expect(() =>
      createEncryptedColumnCodec({
        manifest: {
          ...manifest,
          tables: {
            message: {
              ...manifest.tables.message,
              primaryKeyServerNames: { missing: 'message_id' },
            },
          },
        },
        keyring: keyring(contentKey),
      })
    ).toThrow('maps unknown primary key missing')
  })

  test('derives a deterministic codec identity without key material', () => {
    const first = createEncryptedColumnCodec({
      manifest,
      keyring: keyring(contentKey),
    })
    const second = createEncryptedColumnCodec({
      manifest: JSON.parse(JSON.stringify(manifest)),
      keyring: keyring(new Uint8Array(32).fill(255)),
    })

    expect(first.id).toBe(second.id)
    expect(first.id).toBe(
      'orez-e1:network-test:schema-test:4KUOl_sd4WpwfoFkSXWiwGE048vkbpVkd34DFjHUUAE'
    )
    expect(first.id).not.toContain(Buffer.from(contentKey).toString('hex'))
  })
})

describe('RFC 9180 key wrapping', () => {
  test('wraps 32-byte content keys and authenticates enrollment context', () => {
    const recipient = generateX25519KeyPair(
      (length) => new Uint8Array(nodeRandomBytes(length))
    )
    const info = new TextEncoder().encode('network-test:epoch-3')
    const associatedData = new TextEncoder().encode('device-a:request-7')
    const wrapped = wrapContentKey({
      recipientPublicKey: recipient.publicKey,
      contentKey,
      info,
      associatedData,
      randomBytes: (length) => new Uint8Array(nodeRandomBytes(length)),
    })

    expect(
      unwrapContentKey({
        recipientPrivateKey: recipient.privateKey,
        ...wrapped,
        info,
        associatedData,
      })
    ).toEqual(contentKey)
    expect(() =>
      unwrapContentKey({
        recipientPrivateKey: recipient.privateKey,
        ...wrapped,
        info,
        associatedData: new TextEncoder().encode('device-b:request-7'),
      })
    ).toThrow()
  })

  test('fails closed when no secure randomBytes adapter is supplied', () => {
    expect(() => generateX25519KeyPair(undefined as never)).toThrow(
      'secure randomBytes adapter is required'
    )
    expect(() =>
      hpkeSeal({
        recipientPublicKey: new Uint8Array(32).fill(1),
        plaintext: new Uint8Array(32),
        randomBytes: undefined as never,
      })
    ).toThrow('secure randomBytes adapter is required')
  })
})

function keyring(key: Uint8Array): EncryptionKeyring {
  return {
    current: async () => ({ epoch: 3, key }),
    get: async (epoch) => (epoch === 3 ? key : undefined),
  }
}

function pushRequest(id = 7): PushRequest {
  return {
    clientGroupID: 'client-group-a',
    pushVersion: 1,
    requestID: `request-${id}`,
    mutations: [
      {
        type: 'custom',
        name: 'cloud.applyBatch',
        clientID: 'client-a',
        id,
        args: [
          {
            sourceID: 'machine-a',
            fromSeq: 41,
            throughSeq: 42,
            rows: [
              {
                seq: 41,
                op: 'put',
                table: 'message',
                value: {
                  id: 'message-1',
                  roomID: 'room-1',
                  body: 'hello encrypted world',
                  detail: { reactions: ['wave', 'spark'], count: 2 },
                },
              },
              {
                seq: 42,
                op: 'del',
                table: 'message',
                key: { id: 'message-old' },
              },
            ],
          },
        ],
      },
      {
        type: 'custom',
        name: 'session.sendCommand',
        clientID: 'client-a',
        id: id + 1,
        args: [{ nested: { confidential: 'command payload stays explicit' } }],
      },
    ],
  }
}

function encodedRows(body: PushRequest) {
  const batch = body.mutations[0].args?.[0]
  if (!batch || typeof batch !== 'object' || Array.isArray(batch)) {
    throw new Error('missing encoded batch')
  }
  const rows = batch.rows
  if (!Array.isArray(rows)) throw new Error('missing encoded rows')
  return rows as Array<Record<string, unknown>>
}

function encodedPut(body: PushRequest) {
  const value = encodedRows(body)[0].value
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('missing encoded put')
  }
  return value as Record<string, unknown>
}

function pullWithBody(body: unknown): PullResponse {
  return {
    cookie: 4,
    lastMutationIDChanges: {},
    rowsPatch: [
      {
        op: 'put',
        tableName: 'message_record',
        value: { id: 'message-1', body_cipher: body },
      },
    ],
  } as PullResponse
}

function envelopeNonce(value: unknown) {
  if (typeof value !== 'string') throw new Error('missing envelope')
  const payload = value.slice(value.lastIndexOf('.') + 1)
  return Buffer.from(payload, 'base64url').subarray(0, 24).toString('hex')
}

function tamperEnvelope(value: unknown) {
  if (typeof value !== 'string') throw new Error('missing envelope')
  const payloadStart = value.lastIndexOf('.') + 1
  const replacement = value[payloadStart] === 'A' ? 'B' : 'A'
  return value.slice(0, payloadStart) + replacement + value.slice(payloadStart + 1)
}
