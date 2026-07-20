import { dequal } from 'dequal'
import { useLayoutEffect, useState } from 'react'

type EmitterOptions<T> = {
  name: string
  silent?: boolean
  comparator?: (a: T, b: T) => boolean
}

type CreateEmitterOptions<T> = Omit<EmitterOptions<T>, 'name'>

export class Emitter<const T> {
  private listeners = new Set<(value: T) => void>()
  value: T
  options?: EmitterOptions<T>

  constructor(value: T, options?: EmitterOptions<T>) {
    this.value = value
    this.options = options
  }

  listen = (listener: (value: T) => void): (() => void) => {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  emit = (next: T): void => {
    if (process.env.NODE_ENV === 'development') {
      setCache(this, next)
    }
    const compare = this.options?.comparator
    if (compare) {
      if (this.value && compare(this.value, next)) {
        return
      }
    } else if (this.value === next && process.env.NODE_ENV === 'development') {
      console.warn(
        `[emitter] ${this.options?.name} no comparator option but received same value`,
      )
    }
    this.value = next
    this.listeners.forEach((listener) => listener(next))
  }
}

export function createEmitter<T>(
  name: string,
  defaultValue: T,
  options?: CreateEmitterOptions<T>,
): Emitter<T> {
  const existing = createOrUpdateCache(name, defaultValue) as T
  return new Emitter(existing || defaultValue, { name, ...options })
}

type EmitterValue<E extends Emitter<any>> = E extends Emitter<infer Value>
  ? Value
  : never

export function useEmitterValue<E extends Emitter<any>>(
  emitter: E,
  options?: { disable?: boolean },
): EmitterValue<E> {
  const disabled = options?.disable
  const [state, setState] = useState<EmitterValue<E>>(() => emitter.value)

  useLayoutEffect(() => {
    if (disabled) return

    if (emitter.value !== state) {
      setState(emitter.value)
    }

    return emitter.listen(setState)
  }, [disabled, emitter])

  return state
}

const hmrCache =
  process.env.NODE_ENV === 'development'
    ? new Map<string, { originalDefaultValue: unknown; currentValue: unknown }>()
    : null

function setCache(emitter: Emitter<any>, value: unknown) {
  const name = emitter.options?.name
  if (!name) return
  const cache = hmrCache?.get(name)
  if (!cache) return
  cache.currentValue = value
}

function createOrUpdateCache(name: string, defaultValueProp: unknown) {
  const existing = hmrCache?.get(name)
  const defaultValue = dequal(existing?.originalDefaultValue, defaultValueProp)
    ? existing?.currentValue
    : defaultValueProp

  if (!existing) {
    hmrCache?.set(name, {
      originalDefaultValue: defaultValueProp,
      currentValue: defaultValue,
    })
  }

  return defaultValue
}
