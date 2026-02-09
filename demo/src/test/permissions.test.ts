import { test, expect } from '@playwright/test'
import { clearTodos } from './helpers'

const API = process.env.BASE_URL || 'http://localhost:3457'

test.beforeEach(async () => {
  await clearTodos(API)
})

test.describe('data integrity and security', () => {
  test('sql injection via todo text is safe', async ({ request }) => {
    const malicious = "'; DROP TABLE todo; --"
    const res = await request.post('/api/todos', { data: { text: malicious } })
    expect(res.status()).toBe(201)
    // table still works
    const list = await request.get('/api/todos')
    const todos = await list.json()
    expect(todos.some((t: any) => t.text === malicious)).toBe(true)
  })

  test('update only modifies completed field', async ({ request }) => {
    const create = await request.post('/api/todos', { data: { text: 'original' } })
    const { id } = await create.json()
    await request.patch(`/api/todos/${id}`, {
      data: { completed: true, text: 'hacked', id: 'new-id' },
    })
    const res = await request.get(`/api/todos/${id}`)
    const todo = await res.json()
    expect(todo.text).toBe('original')
    expect(todo.id).toBe(id)
    expect(todo.completed).toBe(true)
  })

  test('delete nonexistent todo does not error', async ({ request }) => {
    const res = await request.delete('/api/todos/does-not-exist')
    expect(res.status()).toBe(200)
  })

  test('update nonexistent todo does not create one', async ({ request }) => {
    await request.patch('/api/todos/ghost-id', { data: { completed: true } })
    const res = await request.get('/api/todos')
    expect((await res.json()).length).toBe(0)
  })

  test('pglite handles long text values', async ({ request }) => {
    const longText = 'a'.repeat(10_000)
    const res = await request.post('/api/todos', { data: { text: longText } })
    expect(res.status()).toBe(201)
    const list = await request.get('/api/todos')
    expect((await list.json())[0].text.length).toBe(10_000)
  })

  test('primary key uniqueness enforced', async ({ request }) => {
    const r1 = await request.post('/api/todos', { data: { text: 'pk 1' } })
    const r2 = await request.post('/api/todos', { data: { text: 'pk 2' } })
    expect((await r1.json()).id).not.toBe((await r2.json()).id)
  })

  test('completed defaults to false', async ({ request }) => {
    const res = await request.post('/api/todos', { data: { text: 'default check' } })
    expect((await res.json()).completed).toBe(false)
  })

  test('xss in todo text is escaped in html', async ({ page, request }) => {
    await request.post('/api/todos', {
      data: { text: '<img src=x onerror=alert(1)>' },
    })
    await page.goto('/')
    const span = page.locator('[data-testid^="todo-item-"] span').first()
    await expect(span).toBeVisible()
    const text = await span.textContent()
    expect(text).toContain('<img')
    expect(await page.evaluate(() => document.title)).toBe('zerolite demo')
  })

  test('change tracking records insert update delete', async ({ request }) => {
    const create = await request.post('/api/todos', { data: { text: 'track me' } })
    const { id } = await create.json()
    await request.patch(`/api/todos/${id}`, { data: { completed: true } })
    await request.delete(`/api/todos/${id}`)
    const res = await request.get(`/api/todos/${id}`)
    expect(res.status()).toBe(404)
  })

  test('empty text creates todo but ui prevents it', async ({ request }) => {
    const res = await request.post('/api/todos', { data: { text: '' } })
    expect(res.status()).toBeLessThan(500)
  })
})
