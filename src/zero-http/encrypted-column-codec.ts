import { chacha20poly1305, xchacha20poly1305 } from '@noble/ciphers/chacha.js'
import { x25519 } from '@noble/curves/ed25519.js'
import { expand, extract, hkdf } from '@noble/hashes/hkdf.js'
import { hmac } from '@noble/hashes/hmac.js'
import { sha256 } from '@noble/hashes/sha2.js'

import type {
  EncryptedColumnManifest,
  EncryptedRowBatch,
  EncryptionKeyring,
  JSONObject,
  JSONValue,
  PayloadCodec,
  PullResponse,
  PushMutation,
  PushRequest,
} from './payload-codec.js'

export type {
  EncryptedColumnManifest,
  EncryptedRowBatch,
  EncryptionKeyring,
  JSONObject,
  JSONPrimitive,
  JSONValue,
  PayloadCodec,
  PullResponse,
  PushMutation,
  PushRequest,
} from './payload-codec.js'

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
const emptyBytes = new Uint8Array(0)
const envelopePrefix = 'orez-e1.'
const base64URLAlphabet =
  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'

const hpkeKEMID = 0x0020
const hpkeKDFID = 0x0001
const hpkeAEADID = 0x0003
const hpkeKEMSuiteID = concatBytes(text('KEM'), integerToBytes(hpkeKEMID, 2))
const hpkeSuiteID = concatBytes(
  text('HPKE'),
  integerToBytes(hpkeKEMID, 2),
  integerToBytes(hpkeKDFID, 2),
  integerToBytes(hpkeAEADID, 2)
)

export type RandomBytes = (length: number) => Uint8Array

export type X25519KeyPair = {
  readonly privateKey: Uint8Array
  readonly publicKey: Uint8Array
}

export type HPKECiphertext = {
  readonly encapsulatedKey: Uint8Array
  readonly ciphertext: Uint8Array
}

type NormalizedColumn = {
  readonly logicalName: string
  readonly physicalName: string
}

type NormalizedPrimaryKey = {
  readonly logicalName: string
  readonly physicalName: string
}

type NormalizedTable = {
  readonly logicalName: string
  readonly physicalName: string
  readonly primaryKey: readonly NormalizedPrimaryKey[]
  readonly columns: readonly NormalizedColumn[]
}

type NormalizedManifest = {
  readonly networkID: string
  readonly schemaID: string
  readonly rowMutations: ReadonlyMap<
    string,
    { readonly argumentIndex: number; readonly format: 'orez-row-batch-v1' }
  >
  readonly tablesByLogicalName: ReadonlyMap<string, NormalizedTable>
  readonly tablesByPhysicalName: ReadonlyMap<string, NormalizedTable>
}

type ParsedEnvelope = {
  readonly epoch: number
  readonly mutationTag: string
  readonly payload: Uint8Array
}

export function createEncryptedColumnCodec(options: {
  readonly manifest: EncryptedColumnManifest
  readonly keyring: EncryptionKeyring
}): PayloadCodec {
  if (
    !options.keyring ||
    typeof options.keyring.current !== 'function' ||
    typeof options.keyring.get !== 'function'
  ) {
    throw new Error('orez-e1 requires an encryption keyring')
  }

  const manifest = normalizeManifest(options.manifest)
  const manifestHash = base64URLEncode(
    sha256(text(canonicalJSON(options.manifest as JSONValue)))
  )
  const id = `orez-e1:${manifest.networkID}:${manifest.schemaID}:${manifestHash}`

  return Object.freeze({
    id,
    async encodePush(body: PushRequest): Promise<PushRequest> {
      assertPushRequest(body)
      rejectConflictingDuplicateMutations(body.mutations)

      let mutations: PushMutation[] | undefined
      let currentKeyPromise:
        | Promise<{ readonly epoch: number; readonly key: Uint8Array } | undefined>
        | undefined
      const currentKey = () => {
        currentKeyPromise ??= options.keyring.current().then(validateCurrentKey)
        return currentKeyPromise
      }
      const epochKeys = new Map<number, Promise<Uint8Array | undefined>>()
      const getEpochKey = (epoch: number) => {
        let promise = epochKeys.get(epoch)
        if (!promise) {
          promise = options.keyring.get(epoch).then((key) => validateEpochKey(epoch, key))
          epochKeys.set(epoch, promise)
        }
        return promise
      }

      for (let index = 0; index < body.mutations.length; index++) {
        const mutation = body.mutations[index]
        if (mutation.type !== 'custom' || typeof mutation.name !== 'string') continue
        const rowMutation = manifest.rowMutations.get(mutation.name)
        if (!rowMutation) continue

        const encoded = await encodeRowMutation(
          mutation,
          rowMutation.argumentIndex,
          manifest,
          currentKey,
          getEpochKey
        )
        if (encoded === mutation) continue
        mutations ??= [...body.mutations]
        mutations[index] = encoded
      }

      return mutations ? ({ ...body, mutations } as PushRequest) : body
    },
    async decodePull(response: PullResponse): Promise<PullResponse> {
      assertPullResponse(response)
      if (response.unchanged) return response

      const epochKeys = new Map<number, Promise<Uint8Array | undefined>>()
      const getEpochKey = (epoch: number) => {
        let promise = epochKeys.get(epoch)
        if (!promise) {
          promise = options.keyring.get(epoch).then((key) => validateEpochKey(epoch, key))
          epochKeys.set(epoch, promise)
        }
        return promise
      }

      let rowsPatch: JSONValue[] | undefined
      for (let index = 0; index < response.rowsPatch.length; index++) {
        const patch = response.rowsPatch[index]
        if (!isJSONObject(patch) || patch.op !== 'put') continue
        if (typeof patch.tableName !== 'string') continue
        const table = manifest.tablesByPhysicalName.get(patch.tableName)
        if (!table || !isJSONObject(patch.value)) continue

        const decodedValue = await decodePullValue(
          patch.value,
          table,
          manifest,
          getEpochKey
        )
        if (decodedValue === patch.value) continue
        rowsPatch ??= [...response.rowsPatch]
        rowsPatch[index] = { ...patch, value: decodedValue }
      }

      return rowsPatch ? ({ ...response, rowsPatch } as PullResponse) : response
    },
  })
}

export function deriveX25519KeyPair(inputKeyMaterial: Uint8Array): X25519KeyPair {
  assertBytes('X25519 input key material', inputKeyMaterial, 32)
  const dkpPRK = hpkeLabeledExtract(
    hpkeKEMSuiteID,
    emptyBytes,
    'dkp_prk',
    inputKeyMaterial
  )
  const privateKey = hpkeLabeledExpand(hpkeKEMSuiteID, dkpPRK, 'sk', emptyBytes, 32)
  return { privateKey, publicKey: x25519.getPublicKey(privateKey) }
}

export function generateX25519KeyPair(randomBytes: RandomBytes): X25519KeyPair {
  return deriveX25519KeyPair(readSecureRandom(randomBytes, 32))
}

export function hpkeSeal(options: {
  readonly recipientPublicKey: Uint8Array
  readonly plaintext: Uint8Array
  readonly info?: Uint8Array
  readonly associatedData?: Uint8Array
  readonly randomBytes: RandomBytes
}): HPKECiphertext {
  assertBytes('HPKE recipient public key', options.recipientPublicKey, 32)
  assertBytes('HPKE plaintext', options.plaintext)
  const info = options.info ?? emptyBytes
  const associatedData = options.associatedData ?? emptyBytes
  assertBytes('HPKE info', info)
  assertBytes('HPKE associated data', associatedData)

  const ephemeral = deriveX25519KeyPair(readSecureRandom(options.randomBytes, 32))
  const context = hpkeSenderContext(
    ephemeral.privateKey,
    ephemeral.publicKey,
    options.recipientPublicKey,
    info
  )
  return {
    encapsulatedKey: ephemeral.publicKey,
    ciphertext: chacha20poly1305(context.key, context.baseNonce, associatedData).encrypt(
      options.plaintext
    ),
  }
}

export function hpkeOpen(options: {
  readonly recipientPrivateKey: Uint8Array
  readonly encapsulatedKey: Uint8Array
  readonly ciphertext: Uint8Array
  readonly info?: Uint8Array
  readonly associatedData?: Uint8Array
}): Uint8Array {
  assertBytes('HPKE recipient private key', options.recipientPrivateKey, 32)
  assertBytes('HPKE encapsulated key', options.encapsulatedKey, 32)
  assertBytes('HPKE ciphertext', options.ciphertext)
  const info = options.info ?? emptyBytes
  const associatedData = options.associatedData ?? emptyBytes
  assertBytes('HPKE info', info)
  assertBytes('HPKE associated data', associatedData)

  const context = hpkeRecipientContext(
    options.recipientPrivateKey,
    options.encapsulatedKey,
    info
  )
  return chacha20poly1305(context.key, context.baseNonce, associatedData).decrypt(
    options.ciphertext
  )
}

export function wrapContentKey(options: {
  readonly recipientPublicKey: Uint8Array
  readonly contentKey: Uint8Array
  readonly info: Uint8Array
  readonly associatedData: Uint8Array
  readonly randomBytes: RandomBytes
}): HPKECiphertext {
  assertBytes('network content key', options.contentKey, 32)
  return hpkeSeal({
    recipientPublicKey: options.recipientPublicKey,
    plaintext: options.contentKey,
    info: options.info,
    associatedData: options.associatedData,
    randomBytes: options.randomBytes,
  })
}

export function unwrapContentKey(options: {
  readonly recipientPrivateKey: Uint8Array
  readonly encapsulatedKey: Uint8Array
  readonly ciphertext: Uint8Array
  readonly info: Uint8Array
  readonly associatedData: Uint8Array
}): Uint8Array {
  const contentKey = hpkeOpen(options)
  assertBytes('unwrapped network content key', contentKey, 32)
  return contentKey
}

async function encodeRowMutation(
  mutation: PushMutation,
  argumentIndex: number,
  manifest: NormalizedManifest,
  currentKey: () => Promise<
    { readonly epoch: number; readonly key: Uint8Array } | undefined
  >,
  getEpochKey: (epoch: number) => Promise<Uint8Array | undefined>
): Promise<PushMutation> {
  if (!Array.isArray(mutation.args) || argumentIndex >= mutation.args.length) {
    throw codecError(manifest, `row mutation ${mutation.name} is missing its row batch`)
  }
  const batch = validateRowBatch(
    mutation.args[argumentIndex],
    manifest,
    mutation.name ?? ''
  )
  const mutationTag = createMutationTag(mutation.clientID, mutation.id)
  let rows:
    | (EncryptedRowBatch['rows'] extends readonly (infer Row)[] ? Row[] : never)
    | undefined
  let changed = false

  for (let index = 0; index < batch.rows.length; index++) {
    const row = batch.rows[index]
    if (row.op !== 'put') continue
    const table = manifest.tablesByLogicalName.get(row.table)
    if (!table) continue
    const primaryKeyTuple = readPrimaryKeyTuple(row.value, table, manifest, 'logical')
    let value: JSONObject = row.value

    for (const column of table.columns) {
      if (!Object.hasOwn(value, column.logicalName)) continue
      const originalValue = value[column.logicalName]
      if (typeof originalValue === 'string' && originalValue.startsWith(envelopePrefix)) {
        const existing = parseEnvelope(originalValue)
        if (existing.mutationTag !== mutationTag) {
          throw cellError(
            manifest,
            table,
            column,
            existing.epoch,
            mutationTag,
            'encoded value has a different mutation tag'
          )
        }
        const key = await getEpochKey(existing.epoch)
        if (!key) {
          throw cellError(
            manifest,
            table,
            column,
            existing.epoch,
            mutationTag,
            'no key is available for encoded retry'
          )
        }
        decryptEnvelopeValue(existing, key, manifest, table, column, primaryKeyTuple)
        continue
      }

      const writable = await currentKey()
      if (!writable) {
        throw cellError(
          manifest,
          table,
          column,
          undefined,
          mutationTag,
          'no current encryption key is available'
        )
      }
      const plaintext = text(canonicalJSON(originalValue))
      const associatedData = createAssociatedData(
        manifest,
        table,
        column,
        primaryKeyTuple,
        writable.epoch,
        mutationTag
      )
      const { dataKey, nonceKey } = deriveColumnKeys(writable.key, manifest.networkID)
      const nonce = deriveColumnNonce(nonceKey, associatedData, plaintext)
      const ciphertext = xchacha20poly1305(dataKey, nonce, associatedData).encrypt(
        plaintext
      )
      // the derived nonce stays inside the binary payload so a reader can open
      // the aead, then re-derive and verify it against the authenticated plaintext.
      const envelope = `orez-e1.${writable.epoch}.${mutationTag}.${base64URLEncode(
        concatBytes(nonce, ciphertext)
      )}`
      value = { ...value, [column.logicalName]: envelope }
      changed = true
    }

    if (value !== row.value) {
      rows ??= [...batch.rows]
      rows[index] = { ...row, value }
    }
  }

  if (!changed) return mutation
  const nextBatch: EncryptedRowBatch = { ...batch, rows: rows ?? batch.rows }
  const args = [...mutation.args]
  args[argumentIndex] = nextBatch
  return { ...mutation, args } as PushMutation
}

async function decodePullValue(
  originalValue: JSONObject,
  table: NormalizedTable,
  manifest: NormalizedManifest,
  getEpochKey: (epoch: number) => Promise<Uint8Array | undefined>
): Promise<JSONObject> {
  let value = originalValue
  let primaryKeyTuple: readonly JSONValue[] | undefined

  for (const column of table.columns) {
    if (!Object.hasOwn(originalValue, column.physicalName)) continue
    const encoded = originalValue[column.physicalName]
    if (typeof encoded !== 'string' || !encoded.startsWith(envelopePrefix)) {
      throw codecError(
        manifest,
        `table ${table.logicalName} has plaintext in encrypted column ${column.logicalName}`
      )
    }
    const envelope = parseEnvelope(encoded)
    const key = await getEpochKey(envelope.epoch)
    if (!key) continue
    primaryKeyTuple ??= readPrimaryKeyTuple(originalValue, table, manifest, 'physical')
    const decoded = decryptEnvelopeValue(
      envelope,
      key,
      manifest,
      table,
      column,
      primaryKeyTuple
    )
    value = { ...value, [column.physicalName]: decoded }
  }

  return value
}

function normalizeManifest(input: EncryptedColumnManifest): NormalizedManifest {
  if (!isJSONObject(input) || input.version !== 1) {
    throw new Error('orez-e1 manifest version must be 1')
  }
  if (
    typeof input.networkID !== 'string' ||
    !input.networkID ||
    typeof input.schemaID !== 'string' ||
    !input.schemaID
  ) {
    throw new Error('orez-e1 manifest requires networkID and schemaID')
  }
  canonicalJSON(input as JSONValue)
  if (!isJSONObject(input.rowMutations) || !isJSONObject(input.tables)) {
    throw new Error('orez-e1 manifest requires rowMutations and tables')
  }

  const rowMutations = new Map<
    string,
    { readonly argumentIndex: number; readonly format: 'orez-row-batch-v1' }
  >()
  for (const [name, config] of Object.entries(input.rowMutations)) {
    if (
      !name ||
      !isJSONObject(config) ||
      !Number.isSafeInteger(config.argumentIndex) ||
      config.argumentIndex < 0 ||
      config.format !== 'orez-row-batch-v1'
    ) {
      throw new Error(`orez-e1 manifest has an invalid row mutation ${name}`)
    }
    rowMutations.set(name, {
      argumentIndex: config.argumentIndex,
      format: 'orez-row-batch-v1',
    })
  }

  const tablesByLogicalName = new Map<string, NormalizedTable>()
  const tablesByPhysicalName = new Map<string, NormalizedTable>()
  for (const [logicalName, spec] of Object.entries(input.tables)) {
    if (!logicalName || !isJSONObject(spec)) {
      throw new Error('orez-e1 manifest has an invalid table')
    }
    const physicalName = spec.serverName === undefined ? logicalName : spec.serverName
    if (typeof physicalName !== 'string' || !physicalName) {
      throw new Error(`orez-e1 manifest table ${logicalName} has an invalid serverName`)
    }
    if (!Array.isArray(spec.primaryKey) || spec.primaryKey.length === 0) {
      throw new Error(`orez-e1 manifest table ${logicalName} requires a primary key`)
    }
    const primaryKeyNames = spec.primaryKey.map((column) => {
      if (typeof column !== 'string' || !column) {
        throw new Error(
          `orez-e1 manifest table ${logicalName} has an invalid primary key`
        )
      }
      return column
    })
    if (new Set(primaryKeyNames).size !== primaryKeyNames.length) {
      throw new Error(`orez-e1 manifest table ${logicalName} has duplicate primary keys`)
    }
    const primaryKeyServerNames = spec.primaryKeyServerNames
    if (primaryKeyServerNames !== undefined && !isJSONObject(primaryKeyServerNames)) {
      throw new Error(
        `orez-e1 manifest table ${logicalName} has invalid primary key server names`
      )
    }
    for (const name of Object.keys(primaryKeyServerNames ?? {})) {
      if (!primaryKeyNames.includes(name)) {
        throw new Error(
          `orez-e1 manifest table ${logicalName} maps unknown primary key ${name}`
        )
      }
    }
    const primaryKey = primaryKeyNames.map((primaryKeyName) => {
      const mapped = primaryKeyServerNames?.[primaryKeyName]
      const physicalPrimaryKey = mapped === undefined ? primaryKeyName : mapped
      if (typeof physicalPrimaryKey !== 'string' || !physicalPrimaryKey) {
        throw new Error(
          `orez-e1 manifest table ${logicalName} has an invalid primary key serverName`
        )
      }
      return { logicalName: primaryKeyName, physicalName: physicalPrimaryKey }
    })
    const physicalPrimaryKeys = primaryKey.map(({ physicalName }) => physicalName)
    if (new Set(physicalPrimaryKeys).size !== physicalPrimaryKeys.length) {
      throw new Error(
        `orez-e1 manifest table ${logicalName} has ambiguous physical primary keys`
      )
    }

    if (!isJSONObject(spec.columns)) {
      throw new Error(`orez-e1 manifest table ${logicalName} requires columns`)
    }
    const physicalColumns = new Set(physicalPrimaryKeys)
    const columns: NormalizedColumn[] = []
    for (const [columnName, columnSpec] of Object.entries(spec.columns)) {
      if (!columnName || !isJSONObject(columnSpec)) {
        throw new Error(`orez-e1 manifest table ${logicalName} has an invalid column`)
      }
      if (primaryKeyNames.includes(columnName)) {
        throw new Error(
          `orez-e1 manifest cannot encrypt primary key ${logicalName}.${columnName}`
        )
      }
      const physicalColumn =
        columnSpec.serverName === undefined ? columnName : columnSpec.serverName
      if (typeof physicalColumn !== 'string' || !physicalColumn) {
        throw new Error(
          `orez-e1 manifest column ${logicalName}.${columnName} has an invalid serverName`
        )
      }
      if (physicalColumns.has(physicalColumn)) {
        throw new Error(
          `orez-e1 manifest has ambiguous physical column ${physicalName}.${physicalColumn}`
        )
      }
      physicalColumns.add(physicalColumn)
      columns.push({ logicalName: columnName, physicalName: physicalColumn })
    }
    if (columns.length === 0) {
      throw new Error(`orez-e1 manifest table ${logicalName} has no encrypted columns`)
    }

    const table = { logicalName, physicalName, primaryKey, columns }
    if (tablesByPhysicalName.has(physicalName)) {
      throw new Error(`orez-e1 manifest has ambiguous physical table ${physicalName}`)
    }
    tablesByLogicalName.set(logicalName, table)
    tablesByPhysicalName.set(physicalName, table)
  }

  return {
    networkID: input.networkID,
    schemaID: input.schemaID,
    rowMutations,
    tablesByLogicalName,
    tablesByPhysicalName,
  }
}

function validateRowBatch(
  value: JSONValue | undefined,
  manifest: NormalizedManifest,
  mutationName: string
): EncryptedRowBatch {
  if (!isJSONObject(value)) {
    throw codecError(manifest, `row mutation ${mutationName} has an invalid row batch`)
  }
  canonicalJSON(value)
  if (
    typeof value.sourceID !== 'string' ||
    !isSequence(value.fromSeq) ||
    !isSequence(value.throughSeq) ||
    !Array.isArray(value.rows)
  ) {
    throw codecError(manifest, `row mutation ${mutationName} has an invalid row batch`)
  }
  for (const row of value.rows) {
    if (
      !isJSONObject(row) ||
      !isSequence(row.seq) ||
      typeof row.table !== 'string' ||
      (row.op !== 'put' && row.op !== 'del') ||
      (row.op === 'put' && !isJSONObject(row.value)) ||
      (row.op === 'del' && !isJSONObject(row.key))
    ) {
      throw codecError(manifest, `row mutation ${mutationName} has an invalid row batch`)
    }
  }
  return value as EncryptedRowBatch
}

function rejectConflictingDuplicateMutations(mutations: readonly PushMutation[]) {
  const canonicalByIdentity = new Map<string, string>()
  for (const mutation of mutations) {
    if (mutation.type !== 'custom') continue
    assertMutationIdentity(mutation)
    const identity = `${mutation.clientID}\u0000${mutation.id}`
    const canonical = canonicalJSON(mutation)
    const previous = canonicalByIdentity.get(identity)
    if (previous !== undefined && previous !== canonical) {
      throw new Error('orez-e1 push contains conflicting duplicate custom mutations')
    }
    canonicalByIdentity.set(identity, canonical)
  }
}

function assertPushRequest(value: PushRequest) {
  if (!isJSONObject(value) || !Array.isArray(value.mutations)) {
    throw new Error('orez-e1 received an invalid push request')
  }
  for (const mutation of value.mutations) {
    if (!isJSONObject(mutation)) {
      throw new Error('orez-e1 received an invalid push mutation')
    }
  }
}

function assertPullResponse(value: PullResponse) {
  if (!isJSONObject(value)) {
    throw new Error('orez-e1 received an invalid pull response')
  }
  if (value.unchanged === true) {
    if (value.cookie === null || typeof value.cookie === 'number') return
    throw new Error('orez-e1 received an invalid pull response')
  }
  if (
    (value.unchanged !== undefined && value.unchanged !== false) ||
    typeof value.cookie !== 'number' ||
    !isJSONObject(value.lastMutationIDChanges) ||
    !Array.isArray(value.rowsPatch)
  ) {
    throw new Error('orez-e1 received an invalid pull response')
  }
}

function assertMutationIdentity(mutation: PushMutation) {
  if (
    typeof mutation.clientID !== 'string' ||
    !Number.isSafeInteger(mutation.id) ||
    mutation.id < 0
  ) {
    throw new Error('orez-e1 custom mutation has an invalid identity')
  }
}

function readPrimaryKeyTuple(
  value: JSONObject,
  table: NormalizedTable,
  manifest: NormalizedManifest,
  names: 'logical' | 'physical'
): readonly JSONValue[] {
  return table.primaryKey.map((column) => {
    const name = names === 'logical' ? column.logicalName : column.physicalName
    if (!Object.hasOwn(value, name)) {
      throw codecError(
        manifest,
        `table ${table.logicalName} is missing primary key ${column.logicalName}`
      )
    }
    return value[name]
  })
}

function decryptEnvelopeValue(
  envelope: ParsedEnvelope,
  key: Uint8Array,
  manifest: NormalizedManifest,
  table: NormalizedTable,
  column: NormalizedColumn,
  primaryKeyTuple: readonly JSONValue[]
): JSONValue {
  const associatedData = createAssociatedData(
    manifest,
    table,
    column,
    primaryKeyTuple,
    envelope.epoch,
    envelope.mutationTag
  )
  const { dataKey, nonceKey } = deriveColumnKeys(key, manifest.networkID)
  const nonce = envelope.payload.subarray(0, 24)
  const ciphertext = envelope.payload.subarray(24)

  try {
    const plaintext = xchacha20poly1305(dataKey, nonce, associatedData).decrypt(
      ciphertext
    )
    const decodedText = decodeCanonicalUTF8(plaintext)
    const decoded = JSON.parse(decodedText) as JSONValue
    if (canonicalJSON(decoded) !== decodedText) {
      throw new Error('plaintext is not canonical JSON')
    }
    const expectedNonce = deriveColumnNonce(nonceKey, associatedData, plaintext)
    if (!equalBytes(nonce, expectedNonce)) throw new Error('derived nonce mismatch')
    return decoded
  } catch {
    throw cellError(
      manifest,
      table,
      column,
      envelope.epoch,
      envelope.mutationTag,
      'authentication failed'
    )
  }
}

function createMutationTag(clientID: string, mutationID: number): string {
  if (!clientID || !Number.isSafeInteger(mutationID) || mutationID < 0) {
    throw new Error('orez-e1 custom mutation has an invalid identity')
  }
  return base64URLEncode(
    sha256(concatBytes(lengthPrefix(text(clientID)), integerToBytes(mutationID, 8)))
  ).slice(0, 16)
}

function createAssociatedData(
  manifest: NormalizedManifest,
  table: NormalizedTable,
  column: NormalizedColumn,
  primaryKeyTuple: readonly JSONValue[],
  epoch: number,
  mutationTag: string
): Uint8Array {
  return concatLengthPrefixed([
    text('orez-e1'),
    text(manifest.networkID),
    text(manifest.schemaID),
    integerToBytes(epoch, 8),
    text(mutationTag),
    text(table.logicalName),
    text(canonicalJSON(primaryKeyTuple)),
    text(column.logicalName),
  ])
}

function deriveColumnKeys(contentKey: Uint8Array, networkID: string) {
  assertBytes('network content key', contentKey, 32)
  const salt = text(networkID)
  return {
    dataKey: hkdf(sha256, contentKey, salt, text('orez-e1/data'), 32),
    nonceKey: hkdf(sha256, contentKey, salt, text('orez-e1/nonce'), 32),
  }
}

function deriveColumnNonce(
  nonceKey: Uint8Array,
  associatedData: Uint8Array,
  plaintext: Uint8Array
) {
  return hmac(sha256, nonceKey, concatBytes(associatedData, sha256(plaintext))).slice(
    0,
    24
  )
}

function parseEnvelope(value: string): ParsedEnvelope {
  const match = /^orez-e1\.(0|[1-9][0-9]*)\.([A-Za-z0-9_-]{16})\.([A-Za-z0-9_-]+)$/.exec(
    value
  )
  if (!match) throw new Error('orez-e1 envelope is malformed')
  const epoch = Number(match[1])
  if (!Number.isSafeInteger(epoch)) throw new Error('orez-e1 envelope epoch is invalid')
  const payload = base64URLDecode(match[3])
  if (payload.length < 40) throw new Error('orez-e1 envelope ciphertext is truncated')
  return { epoch, mutationTag: match[2], payload }
}

function hpkeSenderContext(
  ephemeralPrivateKey: Uint8Array,
  encapsulatedKey: Uint8Array,
  recipientPublicKey: Uint8Array,
  info: Uint8Array
) {
  const dh = validatedX25519(ephemeralPrivateKey, recipientPublicKey)
  const sharedSecret = hpkeExtractAndExpand(
    dh,
    concatBytes(encapsulatedKey, recipientPublicKey)
  )
  return hpkeKeySchedule(sharedSecret, info)
}

function hpkeRecipientContext(
  recipientPrivateKey: Uint8Array,
  encapsulatedKey: Uint8Array,
  info: Uint8Array
) {
  const recipientPublicKey = x25519.getPublicKey(recipientPrivateKey)
  const dh = validatedX25519(recipientPrivateKey, encapsulatedKey)
  const sharedSecret = hpkeExtractAndExpand(
    dh,
    concatBytes(encapsulatedKey, recipientPublicKey)
  )
  return hpkeKeySchedule(sharedSecret, info)
}

function hpkeExtractAndExpand(dh: Uint8Array, kemContext: Uint8Array) {
  const eaePRK = hpkeLabeledExtract(hpkeKEMSuiteID, emptyBytes, 'eae_prk', dh)
  return hpkeLabeledExpand(hpkeKEMSuiteID, eaePRK, 'shared_secret', kemContext, 32)
}

function hpkeKeySchedule(sharedSecret: Uint8Array, info: Uint8Array) {
  const pskIDHash = hpkeLabeledExtract(hpkeSuiteID, emptyBytes, 'psk_id_hash', emptyBytes)
  const infoHash = hpkeLabeledExtract(hpkeSuiteID, emptyBytes, 'info_hash', info)
  const keyScheduleContext = concatBytes(new Uint8Array([0]), pskIDHash, infoHash)
  const secret = hpkeLabeledExtract(hpkeSuiteID, sharedSecret, 'secret', emptyBytes)
  return {
    key: hpkeLabeledExpand(hpkeSuiteID, secret, 'key', keyScheduleContext, 32),
    baseNonce: hpkeLabeledExpand(
      hpkeSuiteID,
      secret,
      'base_nonce',
      keyScheduleContext,
      12
    ),
  }
}

function hpkeLabeledExtract(
  suiteID: Uint8Array,
  salt: Uint8Array,
  label: string,
  inputKeyMaterial: Uint8Array
) {
  return extract(
    sha256,
    concatBytes(text('HPKE-v1'), suiteID, text(label), inputKeyMaterial),
    salt
  )
}

function hpkeLabeledExpand(
  suiteID: Uint8Array,
  prk: Uint8Array,
  label: string,
  info: Uint8Array,
  length: number
) {
  return expand(
    sha256,
    prk,
    concatBytes(integerToBytes(length, 2), text('HPKE-v1'), suiteID, text(label), info),
    length
  )
}

function validatedX25519(privateKey: Uint8Array, publicKey: Uint8Array) {
  let shared: Uint8Array
  try {
    shared = x25519.getSharedSecret(privateKey, publicKey)
  } catch {
    throw new Error('HPKE X25519 validation failed')
  }
  if (shared.every((byte) => byte === 0)) {
    throw new Error('HPKE X25519 validation failed')
  }
  return shared
}

function readSecureRandom(randomBytes: RandomBytes, length: number) {
  if (typeof randomBytes !== 'function') {
    throw new Error('secure randomBytes adapter is required')
  }
  const bytes = randomBytes(length)
  assertBytes('secure randomBytes result', bytes, length)
  return new Uint8Array(bytes)
}

function validateCurrentKey(
  value: { readonly epoch: number; readonly key: Uint8Array } | undefined
) {
  if (value === undefined) return undefined
  if (!isSequence(value.epoch)) throw new Error('orez-e1 current epoch is invalid')
  assertBytes('network content key', value.key, 32)
  return { epoch: value.epoch, key: new Uint8Array(value.key) }
}

function validateEpochKey(epoch: number, key: Uint8Array | undefined) {
  if (key === undefined) return undefined
  assertBytes(`network content key for epoch ${epoch}`, key, 32)
  return new Uint8Array(key)
}

function assertBytes(
  name: string,
  value: unknown,
  length?: number
): asserts value is Uint8Array {
  if (
    !(value instanceof Uint8Array) ||
    (length !== undefined && value.length !== length)
  ) {
    throw new Error(
      length === undefined ? `${name} must be bytes` : `${name} must be ${length} bytes`
    )
  }
}

function isSequence(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0
}

function isJSONObject(value: unknown): value is JSONObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function canonicalJSON(value: JSONValue): string {
  return canonicalJSONInner(value, new Set<object>())
}

function canonicalJSONInner(value: unknown, ancestors: Set<object>): string {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') {
    return JSON.stringify(value)
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value))
      throw new Error('orez-e1 only accepts finite JSON numbers')
    return JSON.stringify(value)
  }
  if (typeof value !== 'object') throw new Error('orez-e1 only accepts JSON values')
  if (ancestors.has(value)) throw new Error('orez-e1 JSON value is cyclic')
  ancestors.add(value)
  let encoded: string
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index++) {
      if (!Object.hasOwn(value, index)) throw new Error('orez-e1 JSON array is sparse')
    }
    encoded = `[${value.map((item) => canonicalJSONInner(item, ancestors)).join(',')}]`
  } else {
    encoded = `{${Object.keys(value)
      .sort()
      .map(
        (key) =>
          `${JSON.stringify(key)}:${canonicalJSONInner(
            (value as Record<string, unknown>)[key],
            ancestors
          )}`
      )
      .join(',')}}`
  }
  ancestors.delete(value)
  return encoded
}

function decodeCanonicalUTF8(bytes: Uint8Array) {
  const decoded = textDecoder.decode(bytes)
  if (!equalBytes(text(decoded), bytes)) throw new Error('plaintext is not UTF-8')
  return decoded
}

function text(value: string) {
  return textEncoder.encode(value)
}

function concatLengthPrefixed(parts: readonly Uint8Array[]) {
  return concatBytes(...parts.map(lengthPrefix))
}

function lengthPrefix(value: Uint8Array) {
  return concatBytes(integerToBytes(value.length, 4), value)
}

function integerToBytes(value: number | bigint, width: number) {
  let remaining = typeof value === 'bigint' ? value : BigInt(value)
  if (remaining < 0) throw new Error('cannot encode a negative integer')
  const bytes = new Uint8Array(width)
  for (let index = width - 1; index >= 0; index--) {
    bytes[index] = Number(remaining & 0xffn)
    remaining >>= 8n
  }
  if (remaining !== 0n) throw new Error('integer does not fit in requested width')
  return bytes
}

function concatBytes(...arrays: readonly Uint8Array[]) {
  const result = new Uint8Array(arrays.reduce((sum, array) => sum + array.length, 0))
  let offset = 0
  for (const array of arrays) {
    result.set(array, offset)
    offset += array.length
  }
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

function base64URLEncode(bytes: Uint8Array) {
  let result = ''
  for (let index = 0; index < bytes.length; index += 3) {
    const first = bytes[index]
    const second = index + 1 < bytes.length ? bytes[index + 1] : 0
    const third = index + 2 < bytes.length ? bytes[index + 2] : 0
    const combined = (first << 16) | (second << 8) | third
    result += base64URLAlphabet[(combined >>> 18) & 63]
    result += base64URLAlphabet[(combined >>> 12) & 63]
    if (index + 1 < bytes.length) result += base64URLAlphabet[(combined >>> 6) & 63]
    if (index + 2 < bytes.length) result += base64URLAlphabet[combined & 63]
  }
  return result
}

function base64URLDecode(value: string) {
  if (!value || value.length % 4 === 1 || /[^A-Za-z0-9_-]/.test(value)) {
    throw new Error('invalid base64url')
  }
  const result = new Uint8Array(Math.floor((value.length * 6) / 8))
  let accumulator = 0
  let bits = 0
  let outputIndex = 0
  for (const character of value) {
    accumulator = (accumulator << 6) | base64URLAlphabet.indexOf(character)
    bits += 6
    if (bits >= 8) {
      bits -= 8
      result[outputIndex++] = (accumulator >>> bits) & 0xff
    }
  }
  if (base64URLEncode(result) !== value) throw new Error('non-canonical base64url')
  return result
}

function codecError(manifest: NormalizedManifest, message: string) {
  return new Error(`orez-e1 schema ${manifest.schemaID}: ${message}`)
}

function cellError(
  manifest: NormalizedManifest,
  table: NormalizedTable,
  column: NormalizedColumn,
  epoch: number | undefined,
  mutationTag: string,
  message: string
) {
  const epochDetail = epoch === undefined ? '' : ` epoch ${epoch}`
  return codecError(
    manifest,
    `${table.logicalName}.${column.logicalName}${epochDetail} mutation ${mutationTag.slice(
      0,
      8
    )}: ${message}`
  )
}
