export type SqlWireValue =
  | { kind: 'null' }
  | { kind: 'integer'; value: string }
  | { kind: 'real'; value: number }
  | { kind: 'text'; value: string }
  | { kind: 'blob'; value: number[] }

const I64_MIN = -(1n << 63n)
const I64_MAX = (1n << 63n) - 1n

function integerWireValue(value: bigint): SqlWireValue {
  if (value < I64_MIN || value > I64_MAX) {
    throw new TypeError(`SQL integer ${value} is outside the i64 range`)
  }
  return { kind: 'integer', value: value.toString() }
}

/**
 * encode one JavaScript SQLite binding for Orez's Rust host boundary.
 *
 * integers cross JSON as decimal strings so their i64 value is exact. values
 * with no unambiguous SQLite representation fail here instead of reaching the
 * host as lossy JSON.
 */
export function encodeSqlValue(value: unknown): SqlWireValue {
  if (value === null) return { kind: 'null' }
  if (typeof value === 'boolean') return integerWireValue(value ? 1n : 0n)
  if (typeof value === 'bigint') return integerWireValue(value)
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new TypeError('SQL numbers must be finite')
    }
    if (Number.isInteger(value)) {
      if (!Number.isSafeInteger(value)) {
        throw new TypeError('SQL integer numbers must be safe integers; use bigint')
      }
      return integerWireValue(BigInt(value))
    }
    return { kind: 'real', value }
  }
  if (typeof value === 'string') return { kind: 'text', value }
  if (value instanceof ArrayBuffer) {
    return { kind: 'blob', value: Array.from(new Uint8Array(value)) }
  }
  if (ArrayBuffer.isView(value)) {
    return {
      kind: 'blob',
      value: Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength)),
    }
  }
  throw new TypeError(`unsupported SQL binding: ${Object.prototype.toString.call(value)}`)
}

/** encode positional SQLite bindings for an Orez Rust sync-host request. */
export function encodeSqlParams(params: readonly unknown[] = []): SqlWireValue[] {
  return params.map(encodeSqlValue)
}
