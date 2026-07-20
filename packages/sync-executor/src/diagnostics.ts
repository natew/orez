export type PushMutationSummary = {
  readonly id: number | null
  readonly clientID: string | null
  readonly name: string | null
  readonly type: string | null
  readonly argSummary: string | null
}

export type PushRequestSummary = {
  readonly url: string
  readonly appID: string | null
  readonly schema: string | null
  readonly clientGroupID: string | null
  readonly requestID: string | null
  readonly pushVersion: number | null
  readonly mutationCount: number
  readonly mutations: readonly PushMutationSummary[]
  readonly parseError?: string
}

export type PushFailureSummary = {
  readonly kind: string
  readonly origin: 'request' | 'response'
  readonly reason: string | null
  readonly message: string | null
  readonly status: number | null
  readonly mutationIDs: readonly {
    readonly id: number | null
    readonly clientID: string | null
  }[]
}

export type PushMutationErrorSummary = {
  readonly id: number | null
  readonly clientID: string | null
  readonly error: string | null
  readonly message: string | null
  readonly detailsName: string | null
}

export type PushDiagnostic = {
  readonly request: PushRequestSummary
  readonly failure: PushFailureSummary | null
  readonly mutationErrors: readonly PushMutationErrorSummary[]
}

export type PushDiagnosticsOptions = {
  readonly argAllowlist?: readonly string[]
  callback(diagnostic: PushDiagnostic): void | Promise<void>
}

type RecordValue = Record<string, unknown>

function isRecord(value: unknown): value is RecordValue {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function stringValue(value: unknown): string | null {
  return typeof value === 'string' ? value : null
}

function numberValue(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function safeScalar(value: unknown): string | null {
  if (typeof value === 'string') return value
  if (typeof value === 'number' && Number.isFinite(value)) return String(value)
  if (typeof value === 'boolean') return String(value)
  return null
}

function summarizeMutation(
  value: unknown,
  argAllowlist: readonly string[]
): PushMutationSummary {
  if (!isRecord(value)) {
    return { id: null, clientID: null, name: null, type: null, argSummary: null }
  }
  const firstArg = Array.isArray(value.args) ? value.args[0] : null
  const argSummary = isRecord(firstArg)
    ? argAllowlist
        .flatMap((key) => {
          const scalar = safeScalar(firstArg[key])
          return scalar === null || scalar.length === 0 ? [] : `${key}=${scalar}`
        })
        .join(',') || null
    : null
  return {
    id: numberValue(value.id),
    clientID: stringValue(value.clientID),
    name: stringValue(value.name),
    type: stringValue(value.type),
    argSummary,
  }
}

export function summarizePushRequest(
  request: Request,
  bodyText: string,
  argAllowlist: readonly string[] = []
): PushRequestSummary {
  const url = new URL(request.url)
  const empty = {
    url: request.url,
    appID: url.searchParams.get('appID'),
    schema: url.searchParams.get('schema'),
    clientGroupID: null,
    requestID: null,
    pushVersion: null,
    mutationCount: 0,
    mutations: [],
  } satisfies PushRequestSummary
  if (!bodyText.trim()) return empty

  let body: unknown
  try {
    body = JSON.parse(bodyText)
  } catch (error) {
    return {
      ...empty,
      parseError: `failed to parse json: ${error instanceof Error ? error.message : String(error)}`,
    }
  }
  if (!isRecord(body)) return { ...empty, parseError: 'push body was not an object' }

  const mutations = Array.isArray(body.mutations)
    ? body.mutations.map((mutation) => summarizeMutation(mutation, argAllowlist))
    : []
  return {
    ...empty,
    clientGroupID: stringValue(body.clientGroupID),
    requestID: stringValue(body.requestID),
    pushVersion: numberValue(body.pushVersion),
    mutationCount: mutations.length,
    mutations,
  }
}

function mutationID(value: unknown) {
  return isRecord(value)
    ? { id: numberValue(value.id), clientID: stringValue(value.clientID) }
    : { id: null, clientID: null }
}

function responseFailure(
  response: unknown,
  status: number | null
): PushFailureSummary | null {
  if (!isRecord(response) || typeof response.error !== 'string') return null
  return {
    kind: response.error,
    origin: 'response',
    reason: response.error,
    message: stringValue(response.message),
    status,
    mutationIDs: Array.isArray(response.mutationIDs)
      ? response.mutationIDs.map(mutationID)
      : [],
  }
}

function mutationError(value: unknown): PushMutationErrorSummary | null {
  if (!isRecord(value) || !isRecord(value.id) || !isRecord(value.result)) return null
  const error = stringValue(value.result.error)
  if (!error) return null
  const details = isRecord(value.result.details) ? value.result.details : null
  return {
    id: numberValue(value.id.id),
    clientID: stringValue(value.id.clientID),
    error,
    message: stringValue(value.result.message),
    detailsName: details ? stringValue(details.name) : null,
  }
}

function responseMutationErrors(response: unknown): PushMutationErrorSummary[] {
  if (!isRecord(response) || !Array.isArray(response.mutations)) return []
  return response.mutations.flatMap((mutation) => {
    const summary = mutationError(mutation)
    return summary ? [summary] : []
  })
}

export async function reportPushDiagnostics(
  options: PushDiagnosticsOptions | undefined,
  input: {
    readonly request: Request
    readonly bodyText: string
    readonly response?: unknown
    readonly error?: unknown
    readonly status?: number
  }
): Promise<void> {
  if (!options) return
  const failure =
    input.error === undefined
      ? responseFailure(input.response, input.status ?? null)
      : {
          kind:
            input.error instanceof Error && input.error.name ? input.error.name : 'Error',
          origin: 'request' as const,
          reason: null,
          message:
            input.error instanceof Error ? input.error.message : String(input.error),
          status: input.status ?? null,
          mutationIDs: [],
        }
  const mutationErrors = responseMutationErrors(input.response)
  if (!failure && mutationErrors.length === 0) return
  try {
    await options.callback({
      request: summarizePushRequest(input.request, input.bodyText, options.argAllowlist),
      failure,
      mutationErrors,
    })
  } catch (error) {
    console.error('[sync-executor] diagnostics callback failed', error)
  }
}
