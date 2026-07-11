export function assertLosslessJsonValue(
  value: unknown,
  path: string,
  ancestors = new WeakSet<object>()
): void {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return
  if (typeof value === 'number') {
    if (Object.is(value, -0)) throw new Error(`${path} contains negative zero`)
    if (Number.isFinite(value)) return
    throw new Error(`${path} contains a non-finite number`)
  }
  if (typeof value !== 'object') {
    throw new Error(`${path} contains non-JSON value ${typeof value}`)
  }
  if (ancestors.has(value)) throw new Error(`${path} contains a cycle`)
  ancestors.add(value)
  try {
    if (Array.isArray(value)) {
      const keys = Reflect.ownKeys(value)
      if (keys.some((key) => typeof key === 'symbol')) {
        throw new Error(`${path} array has a symbol key`)
      }
      for (let index = 0; index < value.length; index++) {
        if (!Object.hasOwn(value, index)) throw new Error(`${path} array is sparse`)
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index))!
        if (!descriptor.enumerable || !('value' in descriptor)) {
          throw new Error(
            `${path} array index ${index} is not an enumerable data property`
          )
        }
        assertLosslessJsonValue(descriptor.value, `${path}[${index}]`, ancestors)
      }
      for (const key of keys) {
        if (key === 'length') continue
        const index = Number(key)
        if (
          !Number.isInteger(index) ||
          index < 0 ||
          index >= value.length ||
          String(index) !== key
        ) {
          throw new Error(`${path} array has extra key ${String(key)}`)
        }
      }
      return
    }

    const prototype = Object.getPrototypeOf(value)
    if (prototype !== Object.prototype && prototype !== null) {
      throw new Error(
        `${path} contains non-plain object ${value.constructor?.name ?? 'unknown'}`
      )
    }
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key === 'symbol') throw new Error(`${path} object has a symbol key`)
      const descriptor = Object.getOwnPropertyDescriptor(value, key)!
      if (!descriptor.enumerable || !('value' in descriptor)) {
        throw new Error(`${path}.${key} is not an enumerable data property`)
      }
      assertLosslessJsonValue(descriptor.value, `${path}.${key}`, ancestors)
    }
  } finally {
    ancestors.delete(value)
  }
}
