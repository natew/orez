import { test, expect } from '@playwright/test'
import { navigateTo, waitForApp, addTodo, getTodoCount, clearTodos } from './helpers'

const API = process.env.BASE_URL || 'http://localhost:3457'

test.beforeEach(async () => {
  await clearTodos(API)
})

test.describe('todo flow', () => {
  test('starts with empty state', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    const count = await getTodoCount(page)
    expect(count).toBe(0)
  })

  test('add todo and verify it appears', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await addTodo(page, 'buy groceries')
    await expect(page.locator('text=buy groceries')).toBeVisible()
    expect(await getTodoCount(page)).toBe(1)
  })

  test('add multiple todos in correct order', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await addTodo(page, 'first')
    await addTodo(page, 'second')
    await addTodo(page, 'third')
    expect(await getTodoCount(page)).toBe(3)
    const items = page.locator('[data-testid^="todo-item-"]')
    await expect(items.first().locator('span')).toContainText('third')
    await expect(items.last().locator('span')).toContainText('first')
  })

  test('toggle todo completion', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await addTodo(page, 'toggle me')
    const item = page.locator('[data-testid^="todo-item-"]').first()
    const checkbox = item.locator('input[type="checkbox"]')
    await expect(checkbox).not.toBeChecked()
    await checkbox.click()
    await expect(item.locator('span')).toHaveClass('completed', { timeout: 3_000 })
    await expect(checkbox).toBeChecked()
  })

  test('untoggle back to incomplete', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await addTodo(page, 'toggle twice')
    const checkbox = page.locator('[data-testid^="todo-item-"]').first().locator('input[type="checkbox"]')
    await checkbox.click()
    await expect(checkbox).toBeChecked({ timeout: 3_000 })
    await checkbox.click()
    await expect(checkbox).not.toBeChecked({ timeout: 3_000 })
  })

  test('delete todo', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await addTodo(page, 'delete me')
    await expect(page.locator('text=delete me')).toBeVisible()
    const item = page.locator('[data-testid^="todo-item-"]', {
      has: page.locator('text=delete me'),
    })
    await item.locator('.delete-btn').click()
    await expect(page.locator('text=delete me')).not.toBeVisible({ timeout: 5_000 })
    expect(await getTodoCount(page)).toBe(0)
  })

  test('delete only removes target todo', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await addTodo(page, 'keep this')
    await addTodo(page, 'remove this')
    expect(await getTodoCount(page)).toBe(2)
    const item = page.locator('[data-testid^="todo-item-"]', {
      has: page.locator('text=remove this'),
    })
    await item.locator('.delete-btn').click()
    await expect(page.locator('text=remove this')).not.toBeVisible({ timeout: 5_000 })
    await expect(page.locator('text=keep this')).toBeVisible()
    expect(await getTodoCount(page)).toBe(1)
  })

  test('input clears after adding todo', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await page.fill('[data-testid="todo-input"]', 'test clear')
    await page.click('[data-testid="todo-add"]')
    await expect(page.locator('[data-testid="todo-input"]')).toHaveValue('')
  })

  test('empty input does not create todo', async ({ page }) => {
    await navigateTo(page, '/')
    await waitForApp(page)
    await page.click('[data-testid="todo-add"]')
    await page.waitForTimeout(500)
    expect(await getTodoCount(page)).toBe(0)
  })
})
