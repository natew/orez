import type { Page } from '@playwright/test'

export async function navigateTo(page: Page, path: string) {
  await page.goto(path, { waitUntil: 'networkidle' })
}

export async function waitForApp(page: Page, timeout = 10_000) {
  await page.waitForSelector('[data-testid="app-container"]', { timeout })
}

export async function addTodo(page: Page, text: string) {
  await page.fill('[data-testid="todo-input"]', text)
  await page.click('[data-testid="todo-add"]')
  // wait for input to clear (confirms round-trip)
  await page.waitForFunction(
    () => {
      const input = document.querySelector(
        '[data-testid="todo-input"]'
      ) as HTMLInputElement
      return input?.value === ''
    },
    { timeout: 5_000 }
  )
}

export async function getTodoCount(page: Page): Promise<number> {
  const countText = await page.textContent('[data-testid="todo-count"]')
  return parseInt(countText || '0', 10)
}

export async function clearTodos(baseUrl: string) {
  await fetch(`${baseUrl}/api/todos`, { method: 'DELETE' })
}
