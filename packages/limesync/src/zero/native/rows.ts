import type { Format, JSONValue } from './types.js'

type Entry = {
  id: number
  key: string
  value: unknown
  refs: number
  children: readonly Entry[]
}

// a watch's hold on its result: the interned top-level values it references
export type Held = readonly Entry[]

export const EMPTY_ROWS: readonly never[] = Object.freeze([])

function hasRelationships(format?: Format): format is Format & {
  relationships: Record<string, Format>
} {
  if (!format?.relationships) return false
  for (const _ in format.relationships) return true
  return false
}

function rowFormat(format?: Format): Format | undefined {
  return hasRelationships(format) ? { relationships: format.relationships } : undefined
}

function isRow(value: JSONValue): value is { readonly [key: string]: JSONValue } {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

// every watch reading an unchanged row gets the same frozen object, so
// consumers compare rows by identity. an entry is counted once per reference
// from a watch's current result or from a parent entry, and leaves at zero.
export class RowTable {
  private entries = new Map<string, Entry>()
  private nextID = 0

  get size(): number {
    return this.entries.size
  }

  // builds a watch result from a full value. the caller owns the returned hold.
  result(value: JSONValue, format?: Format): { value: unknown; held: Held } {
    if (Array.isArray(value)) {
      const rows = rowFormat(format)
      return this.list(value.map((row) => this.row(row, rows)))
    }
    if (format?.singular && value === null) return { value: undefined, held: [] }
    const entry = this.row(value, format)
    return { value: entry.value, held: [entry] }
  }

  // builds a list result from a patch whose entries are indexes into the
  // previous result or new rows. the caller owns the returned hold.
  patch(
    patch: readonly (number | JSONValue)[],
    previous: Held | undefined,
    format?: Format
  ): { value: readonly unknown[]; held: Held } {
    if (!previous) throw new Error('Native Zero patch arrived before its first result')
    for (const entry of patch) {
      if (typeof entry === 'number' && entry >= previous.length)
        throw new Error('Native Zero patch references a missing row')
    }
    const rows = rowFormat(format)
    return this.list(
      patch.map((entry) => {
        if (typeof entry !== 'number') return this.row(entry, rows)
        const row = previous[entry]!
        row.refs++
        return row
      })
    )
  }

  release(held: Held) {
    for (const entry of held) this.drop(entry)
  }

  clear() {
    this.entries.clear()
  }

  private list(held: Entry[]): { value: readonly unknown[]; held: Held } {
    return {
      value: held.length ? Object.freeze(held.map((entry) => entry.value)) : EMPTY_ROWS,
      held,
    }
  }

  private drop(entry: Entry) {
    if (--entry.refs > 0) return
    this.entries.delete(entry.key)
    for (const child of entry.children) this.drop(child)
  }

  // the caller owns one reference to the returned entry, and hands its
  // references to the children over to it
  private take(key: string, children: readonly Entry[], build: () => unknown): Entry {
    const existing = this.entries.get(key)
    if (existing) {
      // an equal key names the same children, which the entry already holds
      for (const child of children) this.drop(child)
      existing.refs++
      return existing
    }
    const entry: Entry = { id: ++this.nextID, key, value: build(), refs: 1, children }
    this.entries.set(key, entry)
    return entry
  }

  private row(value: JSONValue, format?: Format): Entry {
    if (!isRow(value))
      return this.take(JSON.stringify(value), [], () =>
        Array.isArray(value) ? deepFreeze(value) : value
      )
    if (!hasRelationships(format))
      return this.take(JSON.stringify(value) + '\0', [], () => deepFreeze(value))
    const relationships = format.relationships
    const columns: Record<string, JSONValue> = {}
    const children: Entry[] = []
    const related: [string, unknown][] = []
    let relatedKey = ''
    for (const name in value) {
      if (!Object.hasOwn(value, name)) continue
      const field = value[name]!
      if (!Object.hasOwn(relationships, name)) {
        columns[name] = field
        continue
      }
      const child = this.relation(field, relationships[name]!)
      relatedKey += JSON.stringify(name)
      if (child) {
        children.push(child)
        related.push([name, child.value])
        relatedKey += child.id
      } else {
        related.push([name, undefined])
        relatedKey += '-'
      }
    }
    // json escapes control characters, so the separator cannot occur in columns
    return this.take(JSON.stringify(columns) + '\0' + relatedKey, children, () => {
      const row: Record<string, unknown> = { ...value }
      for (const [name, child] of related) row[name] = child
      return deepFreeze(row)
    })
  }

  // a singular relationship to no row reads undefined, like a singular query
  private relation(value: JSONValue, format: Format): Entry | undefined {
    if (format.singular && value === null) return undefined
    if (!Array.isArray(value)) return this.row(value, format)
    const rows = rowFormat(format)
    const children = value.map((row) => this.row(row, rows))
    let key = '['
    for (const child of children) key += child.id + ','
    return this.take(key, children, () =>
      children.length ? Object.freeze(children.map((child) => child.value)) : EMPTY_ROWS
    )
  }
}

// json columns are shared with every reader of the row, so they freeze too
function deepFreeze<T extends object>(value: T): T {
  Object.freeze(value)
  for (const key in value) {
    const field: unknown = value[key]
    if (field && typeof field === 'object' && !Object.isFrozen(field)) deepFreeze(field)
  }
  return value
}
