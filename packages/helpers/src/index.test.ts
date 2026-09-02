import { beforeEach, describe, expect, test } from 'bun:test'

import {
  clearStorageDriver,
  createEmitter,
  createStorage,
  createStorageValue,
  randomId,
  setStorageDriver,
  slugify,
  time,
} from './index.js'

describe('@o/helpers', () => {
  const values = new Map<string, string>()

  beforeEach(() => {
    values.clear()
    clearStorageDriver()
    setStorageDriver({
      getItem: (key) => values.get(key),
      setItem: (key, value) => values.set(key, value),
      removeItem: (key) => values.delete(key),
      getAllKeys: () => [...values.keys()],
    })
  })

  test('keeps namespaced and single-value storage behavior', () => {
    const storage = createStorage<'name', string>('helpers-test')
    const value = createStorageValue<number>('helpers-value-test')

    storage.set('name', 'orez')
    value.set(42)

    expect(storage.get('name')).toBe('orez')
    expect(value.get()).toBe(42)
    expect(values.get('helpers-test:name')).toBe('"orez"')
  })

  test('emits values and retains the established utility results', () => {
    const emitter = createEmitter('helpers-emitter-test', 1)
    let observed = 0
    emitter.listen((value) => {
      observed = value
    })

    emitter.emit(2)

    expect(observed).toBe(2)
    expect(slugify(' Orez_helpers Example ')).toBe('orez-helpers-example')
    expect(time.ms.minutes(2)).toBe(120_000)
    expect(randomId()).toBeString()
  })
})
