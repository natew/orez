export type JSONPrimitive = null | boolean | number | string

export type JSONValue =
  | JSONPrimitive
  | readonly JSONValue[]
  | { readonly [key: string]: JSONValue }

export type JSONObject = Readonly<Record<string, JSONValue>>

export type PushMutation = JSONObject & {
  readonly type: string
  readonly clientID: string
  readonly id: number
  readonly name?: string
  readonly args?: readonly JSONValue[]
}

export type PushRequest = JSONObject & {
  readonly mutations: readonly PushMutation[]
}

export type GotQueryPatchOp =
  | { readonly op: 'clear' }
  | { readonly op: 'put' | 'del'; readonly hash: string }

export type ServerGotQueries = {
  readonly version: number
  readonly patch: readonly GotQueryPatchOp[]
}

export type PullResponse =
  | {
      readonly [key: string]: JSONValue | undefined
      readonly cookie: number
      readonly lastMutationIDChanges: Readonly<Record<string, number>>
      readonly rowsPatch: readonly JSONValue[]
      readonly unchanged?: false
      readonly gotQueries?: ServerGotQueries
    }
  | {
      readonly [key: string]: JSONValue | undefined
      readonly cookie: number | null
      readonly unchanged: true
      readonly gotQueries?: ServerGotQueries
    }

export interface PayloadCodec {
  /** stable configuration identity used to detect conflicting transports. */
  readonly id: string

  /** called exactly once for each serialized /push attempt. */
  encodePush(body: PushRequest): Promise<PushRequest>

  /** called for every successful /pull response before any poke is emitted. */
  decodePull(response: PullResponse): Promise<PullResponse>
}

export type EncryptedColumnManifest = {
  readonly version: 1
  readonly networkID: string
  readonly schemaID: string
  readonly rowMutations: Readonly<
    Record<
      string,
      {
        readonly argumentIndex: number
        readonly format: 'orez-row-batch-v1'
      }
    >
  >
  readonly tables: Readonly<
    Record<
      string,
      {
        readonly serverName?: string
        readonly primaryKey: readonly string[]
        /** physical names for renamed primary-key columns. */
        readonly primaryKeyServerNames?: Readonly<Record<string, string>>
        readonly columns: Readonly<
          Record<
            string,
            {
              readonly serverName?: string
            }
          >
        >
      }
    >
  >
}

export type EncryptedRowBatch = {
  readonly sourceID: string
  readonly fromSeq: number
  readonly throughSeq: number
  readonly rows: readonly (
    | {
        readonly seq: number
        readonly op: 'put'
        readonly table: string
        readonly value: JSONObject
      }
    | {
        readonly seq: number
        readonly op: 'del'
        readonly table: string
        readonly key: JSONObject
      }
  )[]
}

export interface EncryptionKeyring {
  /** current writable epoch and its 32-byte network content key. */
  current(): Promise<{ readonly epoch: number; readonly key: Uint8Array } | undefined>

  /** key for a readable current or historical epoch, or undefined. */
  get(epoch: number): Promise<Uint8Array | undefined>
}

export const identityPayloadCodec: PayloadCodec = Object.freeze({
  id: 'orez-identity-v1',
  async encodePush(body: PushRequest) {
    return body
  },
  async decodePull(response: PullResponse) {
    return response
  },
})
