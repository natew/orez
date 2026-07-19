# Transparent column encryption

Status: draft for coordinator review

## Goal

Keep selected row payload columns confidential from the Cloudflare Durable Object and every other edge component while preserving stock `@rocicorp/zero` behavior on clients. Encryption and decryption happen in orez's HTTP transport. The sync engine stores and forwards opaque strings and never receives content keys.

This design protects payload content. Identifiers, routing fields, state, sequence numbers, timestamps, query shapes, and access patterns remain visible to the edge.

## Fixed architecture

1. `src/zero-http/transport.ts` owns the row payload codec.
   - Encode selected values after Zero has constructed a push and immediately before `POST /push`.
   - Decode selected values immediately after `fetchPull` receives and validates the JSON response, before `pull()` can call `emitPoke`.
   - Stock `@rocicorp/zero` sees plaintext on an enrolled client and does not know that encryption exists.
2. The sync server runs in the Cloudflare Durable Object. `sync-wasm` and `sync-core` never hold network content keys. Rust enforces that encrypted columns are opaque projection values.
3. Web, Expo Hermes, GPUI Hermes, and the headless desktop writer use one TypeScript codec built from portable JavaScript primitives.
4. History rows use transparent column encryption. Command mutations keep a separate explicit end-to-end command envelope because their arguments are arbitrary application-defined objects.
5. Each device has a dedicated X25519 encryption keypair. It is independent from the existing Ed25519 identity keypair. A per-network content key is wrapped to each enrolled device. The edge stores public keys and wrapped keys only.

## Threat model and limits

The P0 target is an honest-but-curious or compromised edge data plane that can inspect Durable Object storage, request bodies, responses, and logs. It can observe clear metadata and ciphertext, but cannot recover protected payloads without an enrolled device key.

Authenticated encryption detects ciphertext modification on clients that have the relevant key. The edge can still drop, replay, reorder, or deny service. Existing pairing and membership authenticate which device is being enrolled; this design does not claim Byzantine consensus or hide traffic analysis.

Revocation is forward-looking. A revoked device loses access to new epochs, but a content key or plaintext it learned before revocation cannot be recalled. A compromised enrolled client can read every column for every key epoch available to it.

The following features are intentionally unavailable on encrypted columns:

- equality, range, full-text, or aggregate filtering
- ordering and cursors
- joins and relationship correlation
- primary, secondary, or unique indexes
- server-side visibility decisions

Applications keep every field needed for those operations clear and encrypt projection-only payload fields such as message bodies, summaries, titles, and JSON detail objects.

## Public TypeScript contract

Add the codec contract beside the HTTP transport types. The transport depends only on `PayloadCodec`; the encryption implementation is a separate module.

```ts
export interface PayloadCodec {
  /** Stable configuration identity used to detect conflicting transports. */
  readonly id: string

  /** Called exactly once for each serialized /push attempt. */
  encodePush(body: PushRequest): Promise<PushRequest>

  /** Called for every successful /pull response before any poke is emitted. */
  decodePull(response: PullResponse): Promise<PullResponse>
}

export type EncryptedColumnManifest = {
  version: 1
  networkID: string
  schemaID: string

  /** Custom mutations whose selected argument is a canonical row batch. */
  rowMutations: Readonly<
    Record<
      string,
      {
        argumentIndex: number
        format: 'orez-row-batch-v1'
      }
    >
  >

  tables: Readonly<
    Record<
      string,
      {
        /** Physical server name. Defaults to the logical table name. */
        serverName?: string
        /** Logical clear-text primary-key columns in canonical order. */
        primaryKey: readonly string[]
        /** Physical names for renamed primary-key columns. */
        primaryKeyServerNames?: Readonly<Record<string, string>>
        columns: Readonly<
          Record<
            string,
            {
              /** Physical server name. Defaults to the logical column name. */
              serverName?: string
            }
          >
        >
      }
    >
  >
}

export type EncryptedRowBatch = {
  sourceID: string
  fromSeq: number
  throughSeq: number
  rows: readonly (
    | {
        seq: number
        op: 'put'
        table: string
        value: Readonly<Record<string, JSONValue>>
      }
    | {
        seq: number
        op: 'del'
        table: string
        /** Complete logical primary key. */
        key: Readonly<Record<string, JSONValue>>
      }
  )[]
}

export interface EncryptionKeyring {
  /** Current writable epoch and its 32-byte network content key. */
  current(): Promise<{ epoch: number; key: Uint8Array } | undefined>

  /** Key for a readable current or historical epoch, or undefined. */
  get(epoch: number): Promise<Uint8Array | undefined>
}

export function createEncryptedColumnCodec(options: {
  manifest: EncryptedColumnManifest
  keyring: EncryptionKeyring
}): PayloadCodec
```

`JSONValue` is orez's existing JSON value type or an equivalent shared protocol type. The public API must not accept `any`.

The manifest is generated or declared beside the application schema. It is the single mapping for both directions:

- Push row batches use logical table and column names.
- Pull patches use physical `tableName` and physical column names.
- The codec builds reverse physical-to-logical maps once during construction and rejects ambiguous mappings.
- Every encrypted table must declare its complete clear primary key. The codec rejects a manifest that also marks a primary-key column as encrypted.

Command mutations do not appear in `rowMutations`. Their mutator implementation owns a versioned end-to-end command envelope and explicitly chooses which arguments are confidential. The generic codec must never recursively encrypt arbitrary mutation arguments.

### Transport options

Extend `HttpPullTransportOptions`:

```ts
export interface HttpPullTransportOptions {
  // existing options
  payloadCodec?: PayloadCodec
}
```

Normalize an omitted codec to a module-level identity codec. Internal transport code always calls one codec path.

`ensureHttpPullTransport` currently caches the transport by origin. Retain one transport per origin and record its codec ID. A second install for the same origin with a different codec ID is a configuration error. Silently retaining the first codec could send plaintext or decrypt with the wrong network key.

The encryption codec ID is deterministic, for example:

```text
orez-e1:<networkID>:<schemaID>:<manifest-sha256>
```

It contains no key material or key fingerprint.

## Exact `transport.ts` flow

### Push

Keep `ZeroHttpSocket.send` and `enqueuePush` as the serialization boundary. Change the private push method from:

```ts
const response = await this.#postJSON('/push', body)
```

to the semantic equivalent of:

```ts
const encodedBody = await this.#state.options.payloadCodec.encodePush(body)
const response = await this.#postJSON('/push', encodedBody)
```

Encoding stays inside the existing `pushChain`. Concurrent sends therefore cannot race key-epoch selection or reorder POSTs. The input object must remain unchanged because Zero can retain it for retry and diagnostics; the codec uses copy-on-write for only the mutation, argument, row, and value objects it changes.

`encodePush` examines mutations in the request:

1. Ignore non-custom mutations.
2. Look up `mutation.name` in `manifest.rowMutations`.
3. Validate the selected argument as `orez-row-batch-v1`.
4. For every `put`, find the logical table and each declared encrypted column.
5. Require the complete clear primary key in the row value.
6. Replace only declared payload values with authenticated envelopes.
7. Leave `del` rows and every clear column unchanged.

If no current write key exists, a row mutation that contains plaintext for an encrypted column fails before the network request. A client must never downgrade to plaintext. A mutation value that is already a valid `orez-e1` envelope is authenticated with its historical key before it is left byte-for-byte unchanged. A missing key or failed authentication rejects the push. This permits persisted Zero queues and reconstructed writer batches to retry a previously encoded mutation safely without accepting forged ciphertext.

### Pull

Change `fetchPull` so its successful response path is semantically:

```ts
const response = await this.#postJSON<PullResponse>('/pull', body)
return this.#state.options.payloadCodec.decodePull(response)
```

This call belongs in `fetchPull`, rather than only in the periodic `pull()` caller. Initial sync, wake-triggered pull, recovery pull, and any future direct caller then share the same guarantee. `pull()` continues to apply query bookkeeping and call `emitPoke` only after `fetchPull` returns the decoded response.

`decodePull` walks `rowsPatch` put operations:

1. Resolve the physical table and physical column through the manifest's reverse map.
2. Require every present declared encrypted physical column to be a string beginning with the exact `orez-e1.` prefix, then parse it.
3. Look up the content key by the envelope epoch.
4. Read clear primary-key values using their physical manifest names, then reconstruct authenticated associated data using their logical identity and the envelope mutation tag.
5. Authenticate, decrypt, parse the canonical JSON plaintext, and restore the original string or JSON value.

When the keyring has no key for an epoch, return the ciphertext string unchanged. This is the intentional no-key view. Reject a present declared encrypted column containing plaintext, malformed envelope data, or a value that fails authentication or canonical parsing. Feeding an unverified value to Zero would hide corruption and could persist a false plaintext value in the client cache. Legacy plaintext migration requires an explicit, separately authenticated versioned mode and is never the steady-state fallback.

The codec also decodes any equivalent row-patch field used by the current pull protocol. Tests must exercise every pull response variant that can reach `emitPoke`; there cannot be a second unwrapped response path.

## Ciphertext format and retry safety

### Column envelope

Both Zero `string` and `json` encrypted columns carry a string at the server:

```text
orez-e1.<epoch>.<mutation-tag>.<ciphertext-base64url>
```

The encrypted payload contains the original logical value encoded as canonical JSON. Canonical JSON preserves the distinction between a JSON string and structured JSON. After base64url decoding, the binary payload is `derived-nonce (24 bytes) || XChaCha20-Poly1305 ciphertext and tag`. A reader extracts the nonce to open the AEAD, then re-derives it from the authenticated plaintext and rejects any mismatch. Carrying the deterministic nonce makes the envelope independently decryptable without adding randomness or weakening the nonce derivation rule below.

P0 supports encrypted Zero columns whose declared logical type is `string` or `json`. The Rust schema guard rejects encrypted `number`, `boolean`, or `null` columns. This restriction avoids server-side type coercion of ciphertext. A later schema design can separate logical client types from the physical opaque storage type if other logical types become necessary.

### Algorithms

Use one portable implementation based on direct dependencies:

- `@noble/curves` for X25519
- `@noble/hashes` for SHA-256, HMAC-SHA-256, and HKDF-SHA-256
- `@noble/ciphers` for XChaCha20-Poly1305 column encryption and ChaCha20-Poly1305 in the HPKE suite

Pin direct package versions and audit their bundle output. Do not depend on transitive noble installations.

`hpke-js` is unsuitable for P0 because its implementation depends on WebCrypto, which is absent in React Native Hermes and GPUI Hermes. Key wrapping uses the standard RFC 9180 suite DHKEM(X25519, HKDF-SHA256), HKDF-SHA256, and ChaCha20Poly1305, implemented once in the orez crypto module with noble primitives and verified against the RFC test vectors. The implementation must use the RFC labeled extract/expand schedule and context construction exactly; an ad hoc sealed-box construction must not be called HPKE.

### Per-column key and nonce derivation

For epoch content key `CK_epoch`, derive independent subkeys with HKDF-SHA256 and fixed versioned labels:

```text
data-key  = HKDF(CK_epoch, salt = networkID, info = "orez-e1/data", 32)
nonce-key = HKDF(CK_epoch, salt = networkID, info = "orez-e1/nonce", 32)
```

Canonical associated data is a length-prefixed binary encoding of:

```text
"orez-e1"
networkID
schemaID
epoch
mutation-tag
logical table name
canonical primary-key tuple
logical column name
```

Length-prefixing is required. Delimiter-joined strings can be ambiguous.

The mutation tag is `base64url(SHA-256(lengthPrefix(clientID) || uint64be(mutation.id)))[0..16]`. It discloses neither identifier directly. Derive the 24-byte XChaCha nonce as:

```text
HMAC-SHA256(
  nonce-key,
  associated-data || SHA-256(canonical-plaintext)
)[0..24]
```

This produces a stable envelope when Zero retries the same mutation body. It also prevents nonce reuse if a buggy caller reuses a mutation ID with different plaintext. Table, primary key, and column identity prevent nonce reuse between cells in a multi-row mutation.

Deterministic ciphertext reveals when the same mutation, cell, and plaintext are replayed. That leakage is accepted for retry idempotency. Separate mutations use distinct mutation tags and therefore distinct ciphertext.

The codec must reject duplicate custom mutation identities inside one push when their canonical bodies differ. It must never log plaintext, content keys, derived keys, nonces, decrypted values, or complete envelopes. Errors may include schema ID, logical table, logical column, epoch, and a truncated mutation tag.

## Portable randomness and Hermes support

The crypto primitives were executed in the real GPUI Hermes fixture runner using deterministic inputs. X25519, HKDF-SHA256, and XChaCha20-Poly1305 completed successfully:

```text
CRYPTO_HERMES_PROBE PASS shared=32 key=32 ciphertext=38 random=undefined
```

This confirms noble's required language and typed-array features work in the desktop Hermes runtime. The remaining gap is secure randomness. GPUI Hermes currently has no `globalThis.crypto`, `crypto.getRandomValues`, or Expo native-module bridge.

Before enabling enrollment or encryption in GPUI, add a host-provided `crypto.getRandomValues` implementation in `react-native-gpui` backed by the Rust operating-system CSPRNG. Install it in the Hermes preamble before application code runs. It must:

- accept only integer typed arrays allowed by the Web Crypto contract
- fill the exact viewed byte range
- enforce the 65,536-byte call limit
- throw on host RNG failure
- never use `Math.random` or a deterministic fallback

Orez owns one `randomBytes(length)` adapter used by key generation, HPKE, and any randomized protocol operation:

- Web: `globalThis.crypto.getRandomValues`
- Expo Hermes: `expo-crypto.getRandomBytes`
- GPUI Hermes: the new host `globalThis.crypto.getRandomValues`

The embedder selects the adapter explicitly at initialization. Missing secure randomness is a startup error for enrollment and key creation. Column encryption uses derived nonces, but key generation and HPKE encapsulation still require secure randomness.

Add conformance tests that execute the same known-answer crypto vectors in Node, browser, Expo Hermes, and GPUI Hermes. The GPUI test must also call the real host random adapter and prove two independent samples differ; deterministic fixture inputs alone do not validate enrollment readiness.

## Desktop daemon writer path

History originates in the Rust `agentbus` daemon, while the only encryption implementation is the TypeScript transport codec. The daemon therefore must not write cloud rows directly and must not implement Rust column encryption.

### Components

1. The daemon projects each history change once into a canonical row operation and appends it to a durable file-backed cloud outbox.
2. A dedicated headless JavaScript Zero writer sidecar reads the daemon outbox over an authenticated loopback API.
3. The sidecar calls one canonical Zero custom mutator, `cloud.applyBatch`, using an `orez-row-batch-v1` argument.
4. That stock Zero client uses `ensureHttpPullTransport` with the same `createEncryptedColumnCodec` instance used by visible clients.
5. The Cloudflare mutator applies rows idempotently and records the source cursor.
6. The sidecar acknowledges the daemon outbox only after the mutation's `.server` promise resolves.

The sidecar is a daemon-supervised process and is independent of the visible desktop window, so closing the GUI does not stop history upload. Add its entry point as `gui/server/src/cloud-writer.ts`, package it in the installed GUI runtime, and add a small `src/commands/cloud_writer.rs` supervisor using the existing detached-child pattern. `src/serve.rs` ensures that supervisor is running whenever a cloud network is configured.

Do not attach the writer lifecycle to `agentbus orez start`. `src/commands/orez.rs` currently supervises an optional local PGlite, zero-cache, and on-zero stack, while the cloud topology has the one sync server in the Durable Object. At cutover, the headless cloud writer is the only daemon history publishing process; the local `src/pg_writer.rs` to PGlite path is retired from production cloud publishing.

The sidecar imports the canonical Agentbus schema, `cloud.applyBatch` mutator, manifest, and orez codec modules. It must not contain its own row projection or crypto rules. The visible desktop client and writer share those modules.

### Loopback protocol

Add an authenticated daemon endpoint with this semantic contract:

```text
GET /api/cloud/rows?after=<acked-seq>&limit=<n>

{
  "sourceID": "<stable machine id>",
  "fromSeq": 41,
  "throughSeq": 80,
  "rows": [ ...canonical row puts and deletes... ]
}
```

Refactor the row construction currently embedded in `src/pg_writer.rs` into one canonical `CloudRowOp` projection module. Every public history writer constructs `CloudRowOp` first. During migration the local PG serializer may consume that same value, but it cannot build a second projection. The cloud path appends the value to `~/.agentbus/cloud-outbox/<networkID>/<sourceID>.jsonl` before reporting the mirror operation complete. Records are length-framed or newline-safe canonical JSON, checksummed, flushed, and `fsync`ed. Files and atomic acknowledgement metadata use mode `0600`.

The daemon assigns a monotonically increasing outbox sequence after local projection. Each record contains a complete canonical logical row operation, not SQL and not an already encrypted value. Periodic reconciliation emits authoritative upserts and deletes through the same projector, covering a daemon crash between an authoritative local state change and outbox append. The stable `sourceID` is the machine ID. The endpoint uses the daemon's existing local bearer authentication and is unavailable on unauthenticated remote routes.

The sidecar passes the response unchanged as the selected argument to `cloud.applyBatch`. The transport codec encrypts declared columns immediately before POST. No plaintext cloud payload bypasses this route.

### Idempotency and recovery

`cloud.applyBatch` stores a clear cursor keyed by `sourceID` and treats row entries as belonging to source sequence numbers in `[fromSeq, throughSeq]`:

- `throughSeq <= storedCursor`: acknowledge as an already applied replay.
- `fromSeq == storedCursor + 1`: apply the batch transactionally, then advance the cursor.
- any gap or overlapping partial range: reject and require the sidecar to restart from the returned cursor.

Every row carries its source `seq`, and the batch bounds equal the minimum and maximum row sequences. Sequences are strictly increasing. This makes partial overlap unambiguous and lets the server discard an applied prefix before validating the remaining contiguous suffix.

Zero identifies the custom mutation with its own `clientID` and mutation `id`. The transport's deterministic envelope makes every retry of that mutation byte-stable. If the server applies the mutation and the sidecar crashes before acknowledging the daemon, the next sidecar submission may receive a new Zero mutation ID and new ciphertext. `cloud.applyBatch` still deduplicates it by `sourceID` and source sequence before applying rows. Transport retry identity and application replay identity solve different failure windows and both are required.

The daemon outbox remains the upload authority until it records the acknowledgement. A persistent Zero KV store reduces re-upload after a process restart but is not the only durability layer. There is no direct Rust-to-Durable-Object history writer after this migration.

### Commands

Agentbus command mutators such as message send or session control have arbitrary arguments and trigger daemon RPC. They do not use `cloud.applyBatch` and are excluded from the transparent manifest. Each command protocol defines a versioned E2E envelope containing its confidential arguments, recipient/context binding, nonce, and authentication tag. Clear command routing fields must be explicitly documented. The server forwards or stores the envelope without generic recursive transformation.

## Rust opacity guard

The server guard makes a schema with unsafe encrypted-column usage fail closed before serving traffic. It does not encrypt or decrypt.

### Schema metadata

Extend `packages/sync-cf-host/src/types.ts`:

```ts
type ZeroColumn = {
  type: ZeroColumnType
  serverName?: string
  encrypted?: true
}
```

`encrypted` is omitted for ordinary columns. It is part of the schema sent to Rust and must agree with the client manifest during deployment. Deployment tooling compares the manifest hash or schema ID and refuses a mismatch.

In `crates/sync-core/src/schema.rs`:

- parse `encrypted` in `Tables::from_zero_schema`
- retain encrypted logical and physical column sets on `TableSpec`
- reject an encrypted primary-key member
- reject encrypted logical types other than `string` and `json`
- reject duplicate or ambiguous logical/physical mappings
- provide a single `resolve_column(table, column)` helper used by every query guard

### Physical indexes and uniqueness

Application DDL can create indexes that do not appear in `ZeroSchemaConfig`. During engine schema initialization, after application DDL exists and before the schema is accepted, inspect SQLite metadata using `pragma_index_list` and `pragma_index_info` for each encrypted table. Reject every primary, unique, partial, expression, or ordinary index whose indexed columns include an encrypted physical column.

An encrypted column may still reside in the table and be selected as row output. The guard must not create a hidden index for it.

### Query AST

Add one recursive encrypted-column usage validator under `crates/sync-core/src/query/`. It consumes parsed query AST plus `Tables` and returns a structured schema/query error. Call the same validator immediately after parsing and before compilation or persistence in both paths:

- query-aware desired-query registration, reached through `qpull.rs::apply_desired_patch` and `register_query`
- transaction query compilation, including the `engine_compile_query` entry point

The visitor rejects encrypted column references in:

- every comparison or predicate operand, including RHS column references
- `orderBy`
- start/end cursors and cursor ordering keys
- joins and every parent/child correlation field in related subqueries
- grouping, aggregation, or distinct keys if those constructs are added

Projection is allowed. A row or related row may return an encrypted column as a value after all routing, filtering, correlation, and ordering rely only on clear columns.

The guard must resolve server and logical names through `TableSpec`; comparing raw strings is insufficient. Unknown columns continue through the existing unknown-column error path.

### Visibility filters

The host currently represents visibility as SQL plus parameters. Add an explicit, required list of referenced columns to each configured filter:

```ts
type VisibilityFilter = {
  sql: string
  params: readonly JSONValue[]
  columns: readonly { table: string; column: string }[]
}
```

Include this metadata in the visibility wire object assembled in `packages/sync-cf-host/src/host.ts`. Rust resolves the declared references through `Tables` and rejects an encrypted column before `engine_handle_pull` can install or evaluate the filter.

This is a trusted configuration invariant. Do not attempt substring matching against SQL. The config builder should generate both SQL and column references from one structured expression so they cannot drift. Raw SQL visibility without complete column metadata is invalid when the schema contains any encrypted column.

### Guard error contract

Errors identify schema ID, table, column, and forbidden use such as `primary-key`, `index`, `predicate`, `order`, `cursor`, `correlation`, or `visibility`. They never include row values. Schema/config violations fail Durable Object initialization or deployment validation. Per-query violations return a stable client error and are never persisted.

## Device keys and enrollment

### Machine key storage

Extend `src/machine.rs` `Machine` with a serde-defaulted encryption keypair record:

```text
encryption_keypair:
  version: 1
  algorithm: X25519
  public_key: base64url(32 bytes)
  private_key: base64url(32 bytes)
```

Generate it with the operating-system CSPRNG on first enrollment and atomically rewrite `machine.json`. Preserve the file's current `0600` permissions. Never derive it from the Ed25519 identity key, machine token, hostname, or machine ID. Existing machines without the field remain readable and generate the field only when encryption enrollment begins.

Browser and GPUI embedders implement the same `DeviceKeyStore` contract with platform-appropriate secure persistence. Expo stores the private key in Keychain/Keystore via SecureStore. Private keys never enter AsyncStorage, ordinary Zero rows, logs, QR payloads, or edge storage.

### Pairing protocol changes

Reuse the existing authenticated machine and phone pairing flow:

- `src/federation.rs` `PairRequestBody` carries the device X25519 public key alongside machine ID, label, and Ed25519 public key.
- `PeerRecord` persists the X25519 public key and enrollment state.
- `PairResponseBody` returns network ID, enrollment request ID, current content-key epoch, and pending/active state. It never returns a plaintext content key.
- `src/commands/network.rs` join sends the local machine's encryption public key.
- `src/serve.rs` `pair_handler` binds that public key to the same one-shot pairing token and peer record as the identity key.
- The phone connect flow generates its X25519 keypair before its post-QR enrollment request and sends only the public key.

Bind enrollment to the existing authenticated pairing transcript. For machines, sign a domain-separated hash of network ID, machine ID, X25519 public key, and pairing challenge with the existing Ed25519 identity key. Verify before accepting the encryption key. Phone enrollment uses the daemon-authenticated pairing channel and shows an out-of-band fingerprint if the product requires protection against an actively substituting edge.

### Content-key grant

The Durable Object stores:

- network ID and current epoch
- active and pending device records with X25519 public keys
- one RFC 9180 HPKE wrapped content key per active device and epoch
- enrollment acknowledgements and revocation state

It never stores an unwrapped content key or device private key.

Enrollment proceeds as follows:

1. The new device creates its X25519 keypair and submits its public key through pairing.
2. The edge records a pending enrollment request.
3. An already enrolled client fetches pending requests, verifies membership and the pairing-bound key fingerprint, and unwraps the current network content key locally.
4. That client HPKE-seals the content key to the pending device's public key with associated data containing network ID, epoch, recipient device ID, recipient key fingerprint, and enrollment request ID.
5. The edge stores the wrapped blob.
6. The new device fetches and unwraps it, proves possession by signing or MACing an enrollment acknowledgement derived from the content key, and becomes active.

If no enrolled device remains, the network key cannot be recovered from the edge. Recovery requires an explicit user-held recovery secret or creation of a new network epoch that cannot decrypt old history. P0 must choose and document one recovery policy before rollout; it must not add an edge escrow key silently.

### Rotation and revocation

Content-key rotation creates epoch `N+1` on an enrolled client:

1. Generate a fresh random 32-byte content key.
2. Wrap it independently to every active device's current X25519 public key.
3. Upload the complete recipient set and atomically activate the epoch after required wraps are present.
4. Writers switch to the new epoch. Readers retain old keys needed for existing rows.
5. A background client migration may rewrite old ciphertext under the new epoch.
6. Retire an old epoch only after migration and retention policy permit all old wrapped keys to be removed.

Revoking a device removes its membership and wrapped-key access, then forces a new epoch whose recipient set excludes it. Deleting only the old wrapped blob is insufficient because the device may already possess the old content key.

Rotating a device key uses add-before-remove: enroll the new X25519 key, wrap and acknowledge every retained epoch it needs, then revoke the old device key. Ed25519 identity rotation and X25519 encryption rotation remain separate operations.

## Deployment invariants

Deployment requires one versioned bundle containing:

- Zero logical/physical schema
- `EncryptedColumnManifest`
- Rust `encrypted` column metadata
- visibility column references
- codec version and supported key epochs

The deployment tool computes and compares the schema/manifest identity used by clients and the Durable Object. A mismatch stops deployment. A server must not accept a schema that marks a column encrypted while the client manifest leaves it clear, or the inverse.

Before enabling encryption for an existing plaintext column:

1. Deploy clients that can read both plaintext and `orez-e1` values but refuse new plaintext writes once a key is enrolled.
2. Enroll device keys and distribute epoch 1.
3. Activate the server opacity guard and encrypted manifest together.
4. Rewrite existing plaintext values from an enrolled client.
5. Verify raw storage contains no plaintext payload before declaring the network Cloudflare-blind.

Steady-state decode recognizes plaintext only for explicitly scheduled migration. Remove that migration mode after completion. It must not remain as an automatic fallback.

## Acceptance tests

### Transport codec

- A push containing a declared canonical row mutation encrypts only declared columns and leaves all routing fields clear.
- A command mutation with arbitrary nested arguments is unchanged by the transparent codec.
- Encoding the identical Zero `clientID`, mutation `id`, row, and plaintext twice produces byte-identical envelopes.
- Changing the mutation ID produces different ciphertext for the same cell and plaintext.
- Two columns or rows in one mutation never reuse a nonce.
- An already encoded mutation is unchanged on retry.
- Missing current write key rejects plaintext push before fetch.
- Pull decoding occurs before the first `emitPoke` for initial, periodic, wake, and recovery pulls.
- A device without an epoch key receives the unchanged ciphertext envelope.
- A device with the wrong key or modified ciphertext gets an authentication error and no poke.
- String and structured JSON values round-trip with their original logical types.

### Stock Zero and query behavior

- An enrolled stock Zero client writes plaintext through a custom row mutator and reads the same plaintext through a normal query.
- Clear-column equality filters, ordering, cursors, and relationships continue to work while their projected encrypted payloads decrypt on the client.
- A no-key stock Zero client can sync the same rows and sees ciphertext strings without transport failure.
- Zero's persisted mutation retry and client restart preserve byte-stable envelopes.

### Cloudflare blindness

Run an integration test against the real Durable Object host:

1. Write distinctive string and JSON canary plaintext from an enrolled client.
2. Inspect raw Durable Object SQLite rows and any change-log tables before client decode.
3. Capture `/push` after transport encoding and `/pull` before transport decoding.
4. Inspect structured logs and thrown errors.

The canary must appear nowhere in raw storage, HTTP edge payloads, or logs. Only `orez-e1` envelopes may occupy protected physical columns. The enrolled client must still query the row and recover the exact canary plaintext.

### Rust guard

Behavioral schema/query tests must prove rejection for an encrypted column used as:

- a primary key
- a number or boolean logical column in P0
- an ordinary, partial, expression, or unique index member
- either side of a predicate column reference
- an order or cursor key
- either side of relationship correlation
- a visibility-filter reference

A positive integration test proves the same encrypted column is accepted in projection and delivered unchanged as ciphertext by Rust.

### Writer durability

- Stop the sidecar after daemon outbox commit and before Zero mutation creation; restart uploads the row.
- Stop it after local mutation creation and before `/push`; Zero retries the same envelope.
- Stop it after server commit and before daemon acknowledgement; source sequence deduplication prevents a duplicate logical application.
- Close the visible desktop GUI while the daemon and writer sidecar continue to upload history.
- Verify no Rust process constructs ciphertext and no direct daemon cloud-write route remains.

### Enrollment lifecycle

- Pairing binds a new X25519 public key to the existing device identity and network.
- Edge storage contains only public keys and HPKE-wrapped content keys.
- A newly enrolled device unwraps the current epoch and decrypts a row.
- Rotation moves new writes to `N+1` while retained clients still decrypt epoch `N` rows.
- Revocation excludes the device from `N+1`; it cannot decrypt new rows.
- Device-key rotation completes add-before-remove without losing old-row access.
- With every enrolled device removed and no recovery secret, the edge cannot recover the network content key.

### Runtime matrix

Run the same codec vectors and a real encrypt/push/pull/decrypt flow in:

- a supported browser
- Expo Hermes on iOS or Android
- GPUI Hermes with the OS-backed random host function
- Node for the headless writer sidecar

Passing deterministic noble vectors in GPUI Hermes is already confirmed. Full acceptance remains blocked until the GPUI host supplies secure randomness and the test exercises it.

## Implementation order

1. Land the GPUI Hermes secure-random host primitive and runtime conformance tests.
2. Add schema encryption metadata and Rust fail-closed guards.
3. Add the transport codec contract, noble implementation, vectors, and transport integration tests.
4. Add key storage, pairing enrollment, wrapped-key registry, rotation, and revocation.
5. Add the daemon outbox, headless JS writer, and idempotent `cloud.applyBatch` path; remove direct cloud history writes.
6. Migrate selected existing columns with a staged plaintext-to-ciphertext rewrite.
7. Run the complete acceptance matrix and inspect raw Durable Object storage before enabling the feature by default.

Each stage fails closed when its required key, schema identity, or randomness capability is absent. Production has one history upload path and one crypto implementation.
