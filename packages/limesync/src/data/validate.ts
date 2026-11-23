// the shape checks generated syncedQueries.ts and syncedMutations.ts run on
// query and mutation args. each check is a standard schema, which is what
// zero's defineQuery and defineMutator accept.

type Issue = { message: string; path: PropertyKey[] }
type Result<T> = { value: T; issues?: undefined } | { issues: Issue[] }

export type Check<T> = {
  readonly kind: string
  readonly entries?: Record<string, Check<unknown>>
  readonly test: (value: unknown, path: PropertyKey[]) => Issue | undefined
  readonly '~standard': {
    readonly version: 1
    readonly vendor: 'limesync'
    readonly validate: (value: unknown) => Result<T>
    readonly types?: { readonly input: T; readonly output: T }
  }
}

type Infer<C> = C extends Check<infer T> ? T : never

function check<T>(
  kind: string,
  test: Check<T>['test'],
  entries?: Record<string, Check<unknown>>
): Check<T> {
  return {
    kind,
    entries,
    test,
    '~standard': {
      version: 1,
      vendor: 'limesync',
      validate(value) {
        const issue = test(value, [])
        return issue ? { issues: [issue] } : { value: value as T }
      },
    },
  }
}

const typeOf = (kind: string) => (value: unknown, path: PropertyKey[]) =>
  typeof value === kind ? undefined : { message: `expected ${kind}`, path }

export const string = () => check<string>('string', typeOf('string'))
export const number = () =>
  check<number>('number', (value, path) =>
    typeof value === 'number' && !Number.isNaN(value)
      ? undefined
      : { message: 'expected number', path }
  )
export const boolean = () => check<boolean>('boolean', typeOf('boolean'))
export const unknown = () => check<unknown>('unknown', () => undefined)
export const never = () => check<never>('never', (_, path) => ({ message: 'unexpected value', path }))
export const void_ = () => check<void>('void', typeOf('undefined'))
export const null_ = () =>
  check<null>('null', (value, path) =>
    value === null ? undefined : { message: 'expected null', path }
  )

export const literal = <const T extends string | number | boolean>(expected: T) =>
  check<T>('literal', (value, path) =>
    value === expected ? undefined : { message: `expected ${JSON.stringify(expected)}`, path }
  )

export const optional = <T>(inner: Check<T>) =>
  check<T | undefined>('optional', (value, path) =>
    value === undefined ? undefined : inner.test(value, path)
  )

export const nullable = <T>(inner: Check<T>) =>
  check<T | null>('nullable', (value, path) =>
    value === null ? undefined : inner.test(value, path)
  )

export const union = <const C extends readonly Check<unknown>[]>(options: C) =>
  check<Infer<C[number]>>('union', (value, path) =>
    options.some((option) => !option.test(value, path))
      ? undefined
      : { message: 'no union member matched', path }
  )

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

type Shape = Record<string, Check<unknown>>
type Optionals<S extends Shape> = {
  [K in keyof S]: undefined extends Infer<S[K]> ? K : never
}[keyof S]
type ObjectOf<S extends Shape> = {
  [K in Exclude<keyof S, Optionals<S>>]: Infer<S[K]>
} & { [K in Optionals<S>]?: Infer<S[K]> }

export const object = <const S extends Shape>(entries: S) =>
  check<{ [K in keyof ObjectOf<S>]: ObjectOf<S>[K] }>(
    'object',
    (value, path) => {
      if (!isRecord(value)) return { message: 'expected object', path }
      for (const key in entries) {
        const issue = entries[key]!.test(value[key], [...path, key])
        if (issue) return issue
      }
    },
    entries
  )

export const array = <T>(item: Check<T>) =>
  check<T[]>('array', (value, path) => {
    if (!Array.isArray(value)) return { message: 'expected array', path }
    for (let index = 0; index < value.length; index++) {
      const issue = item.test(value[index], [...path, index])
      if (issue) return issue
    }
  })

export const tuple = <const C extends readonly Check<unknown>[]>(items: C) =>
  check<{ -readonly [K in keyof C]: Infer<C[K]> }>('tuple', (value, path) => {
    if (!Array.isArray(value) || value.length !== items.length)
      return { message: `expected tuple of ${items.length}`, path }
    for (let index = 0; index < items.length; index++) {
      const issue = items[index]!.test(value[index], [...path, index])
      if (issue) return issue
    }
  })

export const record = <T>(_key: Check<string> | Check<number>, item: Check<T>) =>
  check<Record<string, T>>('record', (value, path) => {
    if (!isRecord(value)) return { message: 'expected object', path }
    for (const key in value) {
      const issue = item.test(value[key], [...path, key])
      if (issue) return issue
    }
  })

export function parse<T>(schema: Check<T>, value: unknown): T {
  const issue = schema.test(value, [])
  if (issue) {
    const at = issue.path.length ? ` at ${issue.path.map(String).join('.')}` : ''
    throw new TypeError(`invalid args${at}: ${issue.message}`)
  }
  return value as T
}
