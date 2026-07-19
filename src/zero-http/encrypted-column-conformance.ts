import {
  createEncryptedColumnCodec,
  deriveX25519KeyPair,
  hpkeOpen,
  hpkeSeal,
} from './encrypted-column-codec.js'

import type {
  EncryptedColumnManifest,
  PullResponse,
  PushRequest,
} from './payload-codec.js'

export type EncryptionConformanceResult = {
  readonly codec: 'orez-e1'
  readonly columnVector: 'pass'
  readonly hpkeVector: 'RFC 9180 A.2.1'
}

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

const expectedCodecID =
  'orez-e1:network-test:schema-test:4KUOl_sd4WpwfoFkSXWiwGE048vkbpVkd34DFjHUUAE'
const expectedColumnEnvelope =
  'orez-e1.3.QHCGUMBeRxu6O1-3.JcHJvjKg96SntU4Pt99PhB236MEWjrltO5klR6_Ome8iP6agq3ERGk2GiaL15prxFIxDUWfgekJWLFYK-TY8'
const expectedRecipientPrivateKey =
  '8057991eef8f1f1af18f4a9491d16a1ce333f695d4db8e38da75975c4478e0fb'
const expectedRecipientPublicKey =
  '4310ee97d88cc1f088a5576c77ab0cf5c3ac797f3d95139c6c84b5429c59662a'
const expectedEncapsulatedKey =
  '1afa08d3dec047a643885163f1180476fa7ddb54c6a8029ea33f95796bf2ac4a'
const expectedHPKECiphertext =
  '1c5250d8034ec2b784ba2cfd69dbdb8af406cfe3ff938e131f0def8c8b60b4db21993c62ce81883d2dd1b51a28'

export async function runEncryptionConformance(): Promise<EncryptionConformanceResult> {
  const contentKey = Uint8Array.from({ length: 32 }, (_, index) => index)
  const codec = createEncryptedColumnCodec({
    manifest,
    keyring: {
      current: async () => ({ epoch: 3, key: contentKey }),
      get: async (epoch) => (epoch === 3 ? contentKey : undefined),
    },
  })
  assertEqual('codec identity', codec.id, expectedCodecID)

  const encoded = await codec.encodePush(columnVectorPush())
  const batch = encoded.mutations[0].args?.[0]
  if (!isRecord(batch) || !Array.isArray(batch.rows)) {
    throw new Error('orez encryption conformance: encoded row batch is missing')
  }
  const firstRow = batch.rows[0]
  if (!isRecord(firstRow) || !isRecord(firstRow.value)) {
    throw new Error('orez encryption conformance: encoded row is missing')
  }
  const envelope = firstRow.value.body
  assertEqual('column envelope', envelope, expectedColumnEnvelope)

  const decoded = await codec.decodePull({
    cookie: 1,
    lastMutationIDChanges: {},
    rowsPatch: [
      {
        op: 'put',
        tableName: 'message_record',
        value: { id: 'message-1', body_cipher: envelope },
      },
    ],
  } as PullResponse)
  if (decoded.unchanged) {
    throw new Error('orez encryption conformance: pull unexpectedly stayed unchanged')
  }
  const patch = decoded.rowsPatch[0]
  if (!isRecord(patch) || !isRecord(patch.value)) {
    throw new Error('orez encryption conformance: decoded row is missing')
  }
  assertEqual('column round trip', patch.value.body_cipher, 'hello encrypted world')

  const recipient = deriveX25519KeyPair(
    hex('1ac01f181fdf9f352797655161c58b75c656a6cc2716dcb66372da835542e1df')
  )
  assertEqual(
    'recipient private key',
    toHex(recipient.privateKey),
    expectedRecipientPrivateKey
  )
  assertEqual(
    'recipient public key',
    toHex(recipient.publicKey),
    expectedRecipientPublicKey
  )

  const inputKeyMaterial = hex(
    '909a9b35d3dc4713a5e72a4da274b55d3d3821a37e5d099e74a647db583a904b'
  )
  const info = hex('4f6465206f6e2061204772656369616e2055726e')
  const plaintext = hex('4265617574792069732074727574682c20747275746820626561757479')
  const associatedData = hex('436f756e742d30')
  const sealed = hpkeSeal({
    recipientPublicKey: recipient.publicKey,
    plaintext,
    info,
    associatedData,
    randomBytes: () => inputKeyMaterial,
  })
  assertEqual('encapsulated key', toHex(sealed.encapsulatedKey), expectedEncapsulatedKey)
  assertEqual('HPKE ciphertext', toHex(sealed.ciphertext), expectedHPKECiphertext)
  const opened = hpkeOpen({
    recipientPrivateKey: recipient.privateKey,
    encapsulatedKey: sealed.encapsulatedKey,
    ciphertext: sealed.ciphertext,
    info,
    associatedData,
  })
  if (!equalBytes(opened, plaintext)) {
    throw new Error('orez encryption conformance: HPKE round trip failed')
  }

  return {
    codec: 'orez-e1',
    columnVector: 'pass',
    hpkeVector: 'RFC 9180 A.2.1',
  }
}

function columnVectorPush(): PushRequest {
  return {
    mutations: [
      {
        type: 'custom',
        name: 'cloud.applyBatch',
        clientID: 'client-a',
        id: 7,
        args: [
          {
            sourceID: 'machine-a',
            fromSeq: 41,
            throughSeq: 41,
            rows: [
              {
                seq: 41,
                op: 'put',
                table: 'message',
                value: {
                  id: 'message-1',
                  body: 'hello encrypted world',
                },
              },
            ],
          },
        ],
      },
    ],
  }
}

function assertEqual(label: string, actual: unknown, expected: unknown) {
  if (actual !== expected) {
    throw new Error(`orez encryption conformance: ${label} did not match`)
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function hex(value: string) {
  const bytes = new Uint8Array(value.length / 2)
  for (let index = 0; index < bytes.length; index++) {
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16)
  }
  return bytes
}

function toHex(value: Uint8Array) {
  let result = ''
  for (const byte of value) result += byte.toString(16).padStart(2, '0')
  return result
}

function equalBytes(left: Uint8Array, right: Uint8Array) {
  if (left.length !== right.length) return false
  let difference = 0
  for (let index = 0; index < left.length; index++) {
    difference |= left[index] ^ right[index]
  }
  return difference === 0
}
