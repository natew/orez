import { setTimeout } from 'node:timers/promises'

const once = process.argv.includes('--once')
const deadline = Date.now() + 40 * 60_000

while (Date.now() < deadline) {
  try {
    const response = await fetch('https://www.githubstatus.com/api/v2/components.json', {
      signal: AbortSignal.timeout(30_000),
    })
    if (!response.ok) throw new Error(`status request failed: ${response.status}`)
    const { components } = await response.json()
    const actions = components.find((component) => component.name === 'Actions')
    if (!actions) throw new Error('Actions component missing from status response')
    console.log(`[watch-actions] ${actions.status}`)
    if (actions.status === 'operational') process.exit(0)
  } catch (error) {
    console.log(`[watch-actions] ${String(error)}`)
  }
  if (once) process.exit(1)
  await setTimeout(180_000)
}

console.error('[watch-actions] recovery not observed within 40 minutes')
process.exit(2)
