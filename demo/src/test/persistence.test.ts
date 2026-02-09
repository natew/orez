import { test, expect } from '@playwright/test'
import { navigateTo, waitForApp, addTodo, getTodoCount, clearTodos } from './helpers'

const API = process.env.BASE_URL || 'http://localhost:3457'

test.beforeEach(async () => {
  await clearTodos(API)
})

test.describe('persistence', () => {
  test('todo survives page reload', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)

    const text = `persist-${Date.now()}`
    await addTodo(page, text)
    await expect(page.locator(`text=${text}`)).toBeVisible()

    await page.reload({ waitUntil: 'networkidle' })
    await waitForApp(page)

    await expect(page.locator(`text=${text}`)).toBeVisible()
    expect(await getTodoCount(page)).toBe(1)
  })

  test('completed state survives reload', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await addTodo(page, 'check persist')

    const checkbox = page.locator('[data-testid^="todo-item-"]').first().locator('input[type="checkbox"]')
    await checkbox.click()
    await page.waitForTimeout(300)
    await expect(checkbox).toBeChecked()

    await page.reload({ waitUntil: 'networkidle' })
    await waitForApp(page)

    const checkboxAfter = page.locator('[data-testid^="todo-item-"]').first().locator('input[type="checkbox"]')
    await expect(checkboxAfter).toBeChecked()
  })

  test('deletion persists across reload', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await addTodo(page, 'ephemeral')
    await expect(page.locator('text=ephemeral')).toBeVisible()

    await page.click('.delete-btn')
    await page.waitForTimeout(300)

    await page.reload({ waitUntil: 'networkidle' })
    await waitForApp(page)

    await expect(page.locator('text=ephemeral')).not.toBeVisible()
    expect(await getTodoCount(page)).toBe(0)
  })

  test('multiple todos persist correctly', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await addTodo(page, 'alpha')
    await addTodo(page, 'beta')
    await addTodo(page, 'gamma')
    expect(await getTodoCount(page)).toBe(3)

    await page.reload({ waitUntil: 'networkidle' })
    await waitForApp(page)

    expect(await getTodoCount(page)).toBe(3)
    await expect(page.locator('text=alpha')).toBeVisible()
    await expect(page.locator('text=beta')).toBeVisible()
    await expect(page.locator('text=gamma')).toBeVisible()
  })

  test('data visible across separate browser contexts', async ({ browser }) => {
    const ctx1 = await browser.newContext()
    const ctx2 = await browser.newContext()
    const page1 = await ctx1.newPage()
    const page2 = await ctx2.newPage()

    await navigateTo(page1, '/')
    await waitForApp(page1)
    const text = `cross-ctx-${Date.now()}`
    await addTodo(page1, text)

    await navigateTo(page2, '/')
    await waitForApp(page2)
    await expect(page2.locator(`text=${text}`)).toBeVisible()

    await ctx1.close()
    await ctx2.close()
  })
})
