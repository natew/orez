#!/usr/bin/env bun
/**
 * lite integration test - validates orez backend with ~/chat app
 *
 * prereqs:
 *   cd ~/chat && bun lite:backend:clean && bun lite:backend
 *
 * usage:
 *   bun scripts/test-lite-integration.ts [--phase=1|2|3] [--headed]
 */

import { chromium, type Page, type ConsoleMessage } from 'playwright'

const args = process.argv.slice(2)
const headed = args.includes('--headed')
const phaseArg = args.find(a => a.startsWith('--phase='))
const targetPhase = phaseArg ? parseInt(phaseArg.split('=')[1]) : 3

const BASE_URL = 'http://localhost:8081'
const CHAT_DIR = process.env.HOME + '/chat'

interface TestResult {
  phase: number
  name: string
  passed: boolean
  error?: string
  consoleLogs: string[]
}

const results: TestResult[] = []
const consoleLogs: string[] = []

async function main() {
  console.log('\n🧪 orez lite integration test')
  console.log(`   target: phase ${targetPhase}`)
  console.log(`   mode: ${headed ? 'headed' : 'headless'}\n`)

  const browser = await chromium.launch({ headless: !headed })
  const context = await browser.newContext()
  const page = await context.newPage()

  // capture console logs
  page.on('console', (msg: ConsoleMessage) => {
    const text = msg.text()
    if (
      text.includes('[zero]') ||
      text.includes('error') ||
      text.includes('Error') ||
      text.includes('sync') ||
      text.includes('replica')
    ) {
      consoleLogs.push(`[${msg.type()}] ${text}`)
    }
  })

  try {
    // phase 1: basic login & sync
    if (targetPhase >= 1) {
      await runPhase1(page)
    }

    // phase 2: create server
    if (targetPhase >= 2) {
      await runPhase2(page)
    }

    // phase 3: prod sync
    if (targetPhase >= 3) {
      await runPhase3(page)
    }

  } catch (err: any) {
    console.error(`\n❌ Test failed: ${err.message}`)
  } finally {
    await browser.close()
  }

  // print results
  console.log('\n📊 Results:')
  for (const r of results) {
    const icon = r.passed ? '✅' : '❌'
    console.log(`   ${icon} Phase ${r.phase}: ${r.name}`)
    if (r.error) {
      console.log(`      Error: ${r.error}`)
    }
  }

  // print relevant console logs
  if (consoleLogs.length > 0) {
    console.log('\n📋 Console logs (errors/sync):')
    for (const log of consoleLogs.slice(-20)) {
      console.log(`   ${log}`)
    }
  }

  // check .orez logs if failed
  const failed = results.some(r => !r.passed)
  if (failed) {
    console.log('\n📁 Check .orez logs:')
    console.log(`   cat ${CHAT_DIR}/.orez/logs/orez.log | tail -50`)
    await dumpOrezLogs()
  }

  process.exit(failed ? 1 : 0)
}

async function runPhase1(page: Page) {
  console.log('🔄 Phase 1: Basic login & sync')

  try {
    // navigate with admin bypass
    await page.goto(`${BASE_URL}/?admin`, { waitUntil: 'domcontentloaded' })

    // wait for username to appear (indicates sync worked)
    const username = page.locator('[data-username]')
    await username.waitFor({ state: 'visible', timeout: 30_000 })

    const usernameValue = await username.getAttribute('data-username')
    console.log(`   ✓ Username synced: ${usernameValue}`)

    results.push({
      phase: 1,
      name: 'Basic login & sync',
      passed: true,
      consoleLogs: [...consoleLogs],
    })
  } catch (err: any) {
    await page.screenshot({ path: '/tmp/orez-phase1-fail.png' })
    results.push({
      phase: 1,
      name: 'Basic login & sync',
      passed: false,
      error: err.message,
      consoleLogs: [...consoleLogs],
    })
    throw err
  }
}

async function runPhase2(page: Page) {
  console.log('🔄 Phase 2: Create Tamagui server')

  try {
    // open devtools
    const devtoolsBtn = page.locator('[data-testid="devtools-button"]')
    await devtoolsBtn.click()

    // click create tamagui server
    const createBtn = page.getByText('Create Tamagui Server')
    await createBtn.waitFor({ state: 'visible', timeout: 5_000 })
    await createBtn.click()

    // wait for tamagui to appear in main content
    await page.waitForSelector('#main-content:has-text("Tamagui")', { timeout: 30_000 })
    console.log('   ✓ Tamagui server created')

    // navigate to tamagui
    const tamaguiLink = page.locator('#main-content a:has-text("Tamagui")')
    await tamaguiLink.click()

    // verify we're on tamagui page
    await page.waitForURL(/\/tamagui/, { timeout: 10_000 })
    console.log('   ✓ Navigated to /tamagui')

    results.push({
      phase: 2,
      name: 'Create Tamagui server',
      passed: true,
      consoleLogs: [...consoleLogs],
    })
  } catch (err: any) {
    await page.screenshot({ path: '/tmp/orez-phase2-fail.png' })
    results.push({
      phase: 2,
      name: 'Create Tamagui server',
      passed: false,
      error: err.message,
      consoleLogs: [...consoleLogs],
    })
    throw err
  }
}

async function runPhase3(page: Page) {
  console.log('🔄 Phase 3: Prod sync')

  try {
    // go back to home
    await page.goto(`${BASE_URL}/?admin`, { waitUntil: 'networkidle' })

    // open devtools
    const devtoolsBtn = page.locator('[data-testid="devtools-button"]')
    await devtoolsBtn.click()

    // clear sync cache
    const clearBtn = page.getByText('Clear Sync Cache')
    await clearBtn.waitFor({ state: 'visible', timeout: 5_000 })
    await clearBtn.click()
    console.log('   ✓ Cleared sync cache')

    // wait a bit
    await page.waitForTimeout(1_000)

    // sync prod
    const syncBtn = page.getByText('Sync Prod (Continue)')
    await syncBtn.click()
    console.log('   ⏳ Syncing from prod (this takes ~2m)...')

    // wait for servers to appear (up to 2 minutes)
    await page.waitForSelector('#main-content a[href^="/"]', { timeout: 120_000 })

    // count servers
    const serverLinks = await page.locator('#main-content a[href^="/"]').count()
    console.log(`   ✓ Synced ${serverLinks} servers from prod`)

    if (serverLinks < 2) {
      throw new Error(`Expected >1 servers, got ${serverLinks}`)
    }

    results.push({
      phase: 3,
      name: 'Prod sync',
      passed: true,
      consoleLogs: [...consoleLogs],
    })
  } catch (err: any) {
    await page.screenshot({ path: '/tmp/orez-phase3-fail.png' })
    results.push({
      phase: 3,
      name: 'Prod sync',
      passed: false,
      error: err.message,
      consoleLogs: [...consoleLogs],
    })
    throw err
  }
}

async function dumpOrezLogs() {
  try {
    const { execSync } = await import('node:child_process')
    const logs = execSync(`tail -30 ${CHAT_DIR}/.orez/logs/orez.log 2>/dev/null || echo "no logs"`, {
      encoding: 'utf-8',
    })
    console.log('\n📜 .orez/logs/orez.log (last 30 lines):')
    console.log(logs)
  } catch {}
}

main()
