export interface HttpRequest {
  method: string
  url: string
  headers?: Record<string, string>
  body?: string | null
}

export interface HttpResponse {
  status: number
  headers: Record<string, string>
  body: string
}

const ADMIN_HEADERS = {
  'access-control-allow-origin': '*',
  'access-control-allow-methods': 'GET, OPTIONS',
  'access-control-allow-headers': '*',
  'content-type': 'application/json',
}

function jsonResponse(status: number, body: unknown): HttpResponse {
  return {
    status,
    headers: ADMIN_HEADERS,
    body: JSON.stringify(body),
  }
}

export function handleDisabledBrowserAdminRequest(
  request: HttpRequest
): HttpResponse | null {
  const url = new URL(request.url, 'http://localhost')
  if (!url.pathname.startsWith('/__orez/')) return null

  const method = request.method.toUpperCase()
  if (method === 'OPTIONS') {
    return { status: 200, headers: ADMIN_HEADERS, body: '' }
  }
  if (method !== 'GET') {
    return jsonResponse(405, { error: 'method not allowed', admin: 'disabled' })
  }

  if (url.pathname === '/__orez/api/logs') {
    return jsonResponse(200, { entries: [], cursor: 0, admin: 'disabled' })
  }

  if (url.pathname === '/__orez/api/status') {
    return jsonResponse(200, { ready: true, admin: 'disabled' })
  }

  return jsonResponse(404, { error: 'not found', admin: 'disabled' })
}
