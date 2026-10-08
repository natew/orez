import { getQueryName } from './queryRegistry'

export function assertQueryJson(queryName: string, value: unknown, path: string) {
  const ancestors = new Set<object>()
  function visit(value: unknown, path: string): void {
    if (
      value === null ||
      typeof value === 'string' ||
      typeof value === 'boolean' ||
      (typeof value === 'number' && Number.isFinite(value))
    )
      return
    const invalid = (reason: string): never => {
      throw new TypeError(
        `Query '${queryName}' argument '${path}' must be JSON: ${reason}`
      )
    }
    if (typeof value !== 'object') return invalid(typeof value)
    if (ancestors.has(value)) invalid('circular reference')
    if (
      !Array.isArray(value) &&
      Object.getPrototypeOf(value) !== Object.prototype &&
      Object.getPrototypeOf(value) !== null
    ) {
      invalid('expected a plain object or array')
    }
    ancestors.add(value)
    if (Array.isArray(value)) {
      for (let index = 0; index < value.length; index++)
        visit(value[index], `${path}[${index}]`)
    } else {
      for (const [key, entry] of Object.entries(value)) {
        // zero permits omitted optional object properties.
        if (entry !== undefined) visit(entry, `${path}.${key}`)
      }
    }
    ancestors.delete(value)
  }
  visit(value, path)
}

export function serializeQueryParams(fn: Function, params: unknown): string {
  if (params === undefined) return ''
  assertQueryJson(getQueryName(fn) ?? (fn.name || 'anonymous'), params, 'params')
  return JSON.stringify(params)
}
