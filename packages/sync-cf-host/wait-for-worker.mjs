// waits for a local wrangler worker to answer. each request is bounded
// because wrangler accepts a connection and never answers when its build
// failed, which would otherwise hold the test until the job times out.
export async function waitForWorker(
  url,
  { server, timeoutMs = 15_000, label = 'workerd', accept } = {}
) {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      const response = await fetch(url, {
        signal: AbortSignal.timeout(Math.max(1, Math.min(2000, deadline - Date.now()))),
      })
      if (response.ok && (!accept || (await accept(response)))) return
    } catch {}
    const exited = server && server.exitCode !== null
    if (exited || Date.now() >= deadline) {
      server?.kill()
      throw new Error(
        `${label} ${exited ? 'exited before it became ready' : 'did not become ready'}`
      )
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}
