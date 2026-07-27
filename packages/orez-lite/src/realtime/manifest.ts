// The streaming-field manifest: which columns may stream, in what mode, under
// what bounds.
//
// It stays PARALLEL to the Zero schema rather than being a property on it.
// Zero derives its client schema and compatibility hash from the stock column
// shape, so an orez-specific property there would change Zero's schema identity
// or depend on how its runtime treats unknown keys. The manifest takes the
// schema as input, validates every table and column against it, and produces a
// separate object.

import { canonicalTopic } from './protocol.js'

import type { RealtimeKeyValue, RealtimeTopic } from './protocol.js'

export type ZeroSchemaShape = {
  readonly tables: Readonly<
    Record<
      string,
      {
        readonly columns: Readonly<Record<string, { readonly type: string }>>
        readonly primaryKey: readonly string[]
      }
    >
  >
}

// `append` is only meaningful for a string column: it concatenates. Every other
// Zero column type replaces. The mode is inferred from the column's declared
// type so a field cannot pick an encoding its values do not support, and an
// explicit `mode: 'replace'` opts a string column out when its producer
// rewrites rather than extends.
export type FieldMode = 'append' | 'replace'

export type StreamingFieldOptions = {
  // hard ceiling on the accumulated value; the producer errors past it
  readonly maxBytes: number
  // coalescing bound on how often a complete value reaches the wire
  readonly maxUpdatesPerSecond: number
  // coalescing bound on delivered bytes. In append mode this bounds NEW bytes,
  // so a long generation does not slow down as its value grows.
  readonly maxBytesPerSecond: number
  // force replace mode on a string column whose producer rewrites the value
  readonly mode?: FieldMode
  // required when the declared TypeScript value is narrower than the Zero
  // runtime type (any json column, and any custom-typed column). Zero's schema
  // carries no runtime validator for those, and an unvalidated value would
  // reach the UI having bypassed PayloadCodec.decodePull.
  readonly validate?: (value: unknown) => boolean
}

export type StreamingFieldSpec = StreamingFieldOptions & {
  readonly table: string
  readonly field: string
  readonly mode: FieldMode
  readonly columnType: string
  readonly primaryKey: readonly string[]
}

export type StreamingManifest = {
  readonly fields: ReadonlyMap<string, StreamingFieldSpec>
  // schema identity, compared against the host's at startup so a client and a
  // host built from different schema revisions fail loudly instead of streaming
  // into a column the other side does not have
  readonly schemaKey: string
}

// Everything a consumer or producer needs for one row's field: which row, and
// what the manifest says about the column. Carrying the spec alongside the
// topic means no call site has to look the field up again, and a hook or writer
// cannot be handed a topic whose field is not declared.
export type StreamingFieldHandle = {
  readonly topic: RealtimeTopic
  readonly spec: StreamingFieldSpec
}

// A callable handle factory per declared field: `streaming.message.content({id})`
export type StreamingFieldRef<Key, Value> = ((key: Key) => StreamingFieldHandle) & {
  readonly spec: StreamingFieldSpec
  // phantom carrier so the hook can infer the field's value type
  readonly __value?: Value
}

const APPENDABLE_TYPES = new Set(['string'])
const VALIDATOR_REQUIRED_TYPES = new Set(['json'])

function schemaIdentity(
  schema: ZeroSchemaShape,
  fields: readonly StreamingFieldSpec[]
): string {
  // identity covers exactly what a mismatch would break: the streamed columns,
  // their type, their mode, and the key ordering topics are built from.
  return fields
    .map((spec) => {
      const table = schema.tables[spec.table]!
      return `${spec.table}.${spec.field}:${spec.columnType}:${spec.mode}:${table.primaryKey.join(',')}`
    })
    .sort()
    .join('|')
}

export type StreamingFieldDeclaration = Readonly<
  Record<string, Readonly<Record<string, StreamingFieldOptions>>>
>

export function defineStreamingFields<Declaration extends StreamingFieldDeclaration>(
  schema: ZeroSchemaShape,
  declaration: Declaration
): {
  readonly manifest: StreamingManifest
} & {
  readonly [Table in keyof Declaration]: {
    readonly [Field in keyof Declaration[Table]]: StreamingFieldRef<
      Record<string, RealtimeKeyValue>,
      unknown
    >
  }
} {
  const specs: StreamingFieldSpec[] = []
  const tables: Record<string, Record<string, StreamingFieldRef<never, unknown>>> = {}

  for (const [table, fields] of Object.entries(declaration)) {
    const schemaTable = schema.tables[table]
    if (!schemaTable) {
      throw new TypeError(`streaming field table '${table}' is not in the Zero schema`)
    }
    if (schemaTable.primaryKey.length === 0) {
      throw new TypeError(`streaming field table '${table}' has no primary key`)
    }
    const refs: Record<string, StreamingFieldRef<never, unknown>> = {}

    for (const [field, options] of Object.entries(fields)) {
      const column = schemaTable.columns[field]
      if (!column) {
        throw new TypeError(
          `streaming field '${table}.${field}' is not in the Zero schema`
        )
      }
      if (schemaTable.primaryKey.includes(field)) {
        throw new TypeError(
          `streaming field '${table}.${field}' is a primary key column; a streamed key would change the row's identity mid-generation`
        )
      }
      validateBounds(table, field, options)

      const mode =
        options.mode ?? (APPENDABLE_TYPES.has(column.type) ? 'append' : 'replace')
      if (mode === 'append' && !APPENDABLE_TYPES.has(column.type)) {
        throw new TypeError(
          `streaming field '${table}.${field}' is a ${column.type} column; append mode concatenates and only applies to string columns`
        )
      }
      if (VALIDATOR_REQUIRED_TYPES.has(column.type) && !options.validate) {
        throw new TypeError(
          `streaming field '${table}.${field}' is a ${column.type} column and must supply validate(); Zero carries no runtime validator for it and realtime values bypass PayloadCodec.decodePull`
        )
      }

      const spec: StreamingFieldSpec = {
        ...options,
        table,
        field,
        mode,
        columnType: column.type,
        primaryKey: schemaTable.primaryKey,
      }
      specs.push(spec)

      const ref = ((key: Record<string, RealtimeKeyValue>): StreamingFieldHandle => {
        for (const column of spec.primaryKey) {
          if (key[column] === undefined) {
            throw new TypeError(
              `streaming topic '${table}.${field}' is missing primary key column '${column}'`
            )
          }
        }
        return { topic: { table, key, field }, spec }
      }) as unknown as StreamingFieldRef<never, unknown>
      Object.defineProperty(ref, 'spec', { value: spec, enumerable: true })
      refs[field] = ref
    }
    tables[table] = refs
  }

  const fieldMap = new Map<string, StreamingFieldSpec>()
  for (const spec of specs) fieldMap.set(`${spec.table}.${spec.field}`, spec)

  return {
    ...tables,
    manifest: { fields: fieldMap, schemaKey: schemaIdentity(schema, specs) },
  } as never
}

function validateBounds(
  table: string,
  field: string,
  options: StreamingFieldOptions
): void {
  const bounds = [
    ['maxBytes', options.maxBytes],
    ['maxUpdatesPerSecond', options.maxUpdatesPerSecond],
    ['maxBytesPerSecond', options.maxBytesPerSecond],
  ] as const
  for (const [name, value] of bounds) {
    if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
      throw new TypeError(`streaming field '${table}.${field}' needs a positive ${name}`)
    }
  }
}

// Host-side lookup: resolve a topic against the manifest, returning the spec
// and the canonical wire identity, or a reason the topic is not streamable.
// Every host runs this before it will fan anything out, so a producer bug or a
// crafted subscribe frame cannot create a topic outside the manifest.
export function resolveTopic(
  manifest: StreamingManifest,
  topic: RealtimeTopic
):
  | { readonly spec: StreamingFieldSpec; readonly id: string }
  | { readonly reason: string } {
  const spec = manifest.fields.get(`${topic.table}.${topic.field}`)
  if (!spec) return { reason: `'${topic.table}.${topic.field}' is not a streaming field` }
  for (const column of spec.primaryKey) {
    const value = topic.key[column]
    if (value === undefined) {
      return { reason: `topic is missing primary key column '${column}'` }
    }
  }
  return { spec, id: canonicalTopic(spec.primaryKey, topic) }
}
