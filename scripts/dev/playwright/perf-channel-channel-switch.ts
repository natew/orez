#!/usr/bin/env bun
/**
 * channel-to-channel switch performance measurement
 *
 * usage:
 *   bun scripts/dev/playwright/perf-channel-channel-switch.ts
 *   CYCLES=20 HEADLESS=1 bun scripts/dev/playwright/perf-channel-channel-switch.ts
 */

// resolve playwright from test-chat where it's installed
import { chromium } from '../../../test-chat/node_modules/playwright'

const CYCLES = Number(process.env.CYCLES || 10)
const URL = process.env.URL || 'http://localhost:8081?admin=tamagui'
const HEADLESS = process.env.HEADLESS === '1'

interface SwitchTiming {
  cycle: number
  direction: string
  total: number
}

function percentile(sorted: number[], p: number): number {
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, idx)]!
}

function fmt(ms: number): string {
  return `${ms.toFixed(1)}ms`
}

async function main() {
  console.log(`ttcc perf: ${CYCLES} cycles, ${URL}`)

  const browser = await chromium.launch({ headless: HEADLESS })
  const page = await (
    await browser.newContext({ viewport: { width: 1280, height: 800 } })
  ).newPage()

  await page.goto(URL, { waitUntil: 'domcontentloaded' })
  await page.waitForSelector('[data-testid="channel-main"]', { timeout: 60_000 })
  await page.waitForTimeout(3000) // let zero sync settle

  // find channel links in sidebar (x < 300, href pattern /server/channel)
  const channelHrefs = await page.evaluate(() => {
    const results: string[] = []
    document.querySelectorAll('a').forEach((a) => {
      if (a.offsetWidth === 0) return
      const rect = a.getBoundingClientRect()
      if (rect.x > 300) return
      const href = a.getAttribute('href') || ''
      // channel links: /server/channel-slug (2 segments, not special pages)
      const segs = href.split('/').filter(Boolean)
      if (
        segs.length === 2 &&
        !href.includes('-data') &&
        !href.includes('-flows') &&
        !href.includes('-apps') &&
        !href.includes('activity')
      ) {
        results.push(href)
      }
    })
    return results
  })

  if (channelHrefs.length < 2) {
    console.error(`need 2+ channels, found ${channelHrefs.length}: ${channelHrefs}`)
    await browser.close()
    process.exit(1)
  }

  // pick two channels with messages (skip server main channel)
  const hrefA = channelHrefs[1] || channelHrefs[0]!
  const hrefB = channelHrefs[2] || channelHrefs[channelHrefs.length - 1]!
  const linkA = page.locator(`a[href="${hrefA}"]`).first()
  const linkB = page.locator(`a[href="${hrefB}"]`).first()
  const nameA = (await linkA.textContent())?.trim() || hrefA
  const nameB = (await linkB.textContent())?.trim() || hrefB

  console.log(`channels: "${nameA}" <-> "${nameB}"\n`)

  // warm up
  await linkA.click()
  await waitForMessages(page)
  await page.waitForTimeout(500)

  const timings: SwitchTiming[] = []

  for (let i = 0; i < CYCLES; i++) {
    timings.push(await measure(page, linkB, i, `${nameA} -> ${nameB}`))
    timings.push(await measure(page, linkA, i, `${nameB} -> ${nameA}`))
  }

  // report
  const totals = timings.map((t) => t.total).sort((a, b) => a - b)
  console.log('\n' + '='.repeat(50))
  console.log(`switches: ${timings.length}`)
  console.log(`min:  ${fmt(totals[0]!)}`)
  console.log(`p50:  ${fmt(percentile(totals, 50))}`)
  console.log(`avg:  ${fmt(totals.reduce((s, v) => s + v, 0) / totals.length)}`)
  console.log(`p95:  ${fmt(percentile(totals, 95))}`)
  console.log(`max:  ${fmt(totals[totals.length - 1]!)}`)
  console.log('='.repeat(50))

  await browser.close()
}

async function waitForMessages(page: any) {
  await page.waitForFunction(
    () => {
      const scroller = document.querySelector('.virtuoso-scroller')
      if (!scroller) return false
      // virtuoso renders message items with data-testid="message-item"
      return scroller.querySelectorAll('[data-testid="message-item"]').length > 0
    },
    { timeout: 15_000 }
  )
}

async function measure(
  page: any,
  target: any,
  cycle: number,
  dir: string
): Promise<SwitchTiming> {
  const t0 = await page.evaluate(() => performance.now())
  await target.click()
  await waitForMessages(page)
  const t4 = await page.evaluate(() => performance.now())
  const total = t4 - t0
  process.stdout.write(`  ${String(cycle).padStart(2)} ${dir.padEnd(28)} ${fmt(total)}\n`)
  await page.waitForTimeout(100)
  return { cycle, direction: dir, total }
}

main().catch((err) => {
  console.error('fatal:', err)
  process.exit(1)
})
