export type ZeroHttpProbeResult = { ok: true } | { ok: false; reason: string }

export async function probeZeroCacheHttp(
  zeroPort: number,
  timeoutMs: number
): Promise<ZeroHttpProbeResult> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)

  try {
    const res = await fetch(`http://127.0.0.1:${zeroPort}/`, {
      signal: controller.signal,
    })

    // zero may return 404 on "/" while still being healthy.
    if (res.ok || res.status === 404) return { ok: true }
    return { ok: false, reason: `HTTP ${res.status}` }
  } catch (err: any) {
    return { ok: false, reason: err?.message || String(err) || 'request failed' }
  } finally {
    clearTimeout(timer)
  }
}
