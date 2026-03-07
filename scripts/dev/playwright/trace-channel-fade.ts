#!/usr/bin/env bun
import { mkdirSync } from 'node:fs'

/**
 * capture rapid screenshots during channel switch to see the fade
 */
import { chromium } from '../../../test-chat/node_modules/playwright'

async function main() {
  mkdirSync('/tmp/fade-frames', { recursive: true })

  const browser = await chromium.launch({ headless: true })
  const page = await (
    await browser.newContext({ viewport: { width: 1280, height: 800 } })
  ).newPage()

  await page.goto('http://localhost:8081?admin=tamagui', {
    waitUntil: 'domcontentloaded',
  })
  await page.waitForSelector('[data-testid="channel-main"]', { timeout: 60000 })
  await page.waitForTimeout(3000)

  // find channels
  const channelHrefs = await page.evaluate(() => {
    const results: string[] = []
    document.querySelectorAll('a').forEach((a) => {
      if (a.offsetWidth === 0) return
      const rect = a.getBoundingClientRect()
      if (rect.x > 300) return
      const href = a.getAttribute('href') || ''
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

  const hrefA = channelHrefs.find((h) => h.includes('takeout')) || channelHrefs[1]!
  const hrefB = channelHrefs.find((h) => h.includes('-one')) || channelHrefs[2]!
  console.log(`channels: ${hrefA} <-> ${hrefB}`)

  // warm up
  await page.click(`a[href="${hrefA}"]`)
  await page.waitForTimeout(2000)

  // use CDP to start rapid screenshotting via Page.startScreencast
  const cdp = await page.context().newCDPSession(page)

  const frames: Buffer[] = []
  cdp.on('Page.screencastFrame', async (params: any) => {
    frames.push(Buffer.from(params.data, 'base64'))
    await cdp.send('Page.screencastFrameAck', { sessionId: params.sessionId })
  })

  await cdp.send('Page.startScreencast', {
    format: 'png',
    quality: 80,
    maxWidth: 640,
    maxHeight: 400,
    everyNthFrame: 1,
  })

  // click to switch
  await page.click(`a[href="${hrefB}"]`)
  await page.waitForTimeout(1500)

  await cdp.send('Page.stopScreencast')

  // save frames
  for (let i = 0; i < frames.length; i++) {
    const path = `/tmp/fade-frames/frame-${String(i).padStart(3, '0')}.png`
    await Bun.write(path, frames[i]!)
  }
  console.log(`saved ${frames.length} frames to /tmp/fade-frames/`)

  await browser.close()
}

main().catch((err) => {
  console.error('fatal:', err)
  process.exit(1)
})
