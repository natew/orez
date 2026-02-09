import { test, expect } from '@playwright/test'
import { clearTodos } from './helpers'

const API = process.env.BASE_URL || 'http://localhost:3457'

test.beforeEach(async () => {
  await clearTodos(API)
})

test.describe('mutations via api', () => {
  test('insert returns created todo with all fields', async ({ request }) => {
    const res = await request.post('/api/todos', { data: { text: 'api insert' } })
    expect(res.status()).toBe(201)
    const todo = await res.json()
    expect(todo.id).toBeTruthy()
    expect(todo.text).toBe('api insert')
    expect(todo.completed).toBe(false)
    expect(Number(todo.created_at)).toBeGreaterThan(0)
  })

  test('update sets completed to true', async ({ request }) => {
    const create = await request.post('/api/todos', { data: { text: 'update me' } })
    const { id } = await create.json()
    const res = await request.patch(`/api/todos/${id}`, { data: { completed: true } })
    const updated = await res.json()
    expect(updated.completed).toBe(true)
  })

  test('update sets completed back to false', async ({ request }) => {
    const create = await request.post('/api/todos', { data: { text: 'toggle api' } })
    const { id } = await create.json()
    await request.patch(`/api/todos/${id}`, { data: { completed: true } })
    const res = await request.patch(`/api/todos/${id}`, { data: { completed: false } })
    const updated = await res.json()
    expect(updated.completed).toBe(false)
  })

  test('delete removes todo', async ({ request }) => {
    const create = await request.post('/api/todos', { data: { text: 'delete api' } })
    const { id } = await create.json()
    await request.delete(`/api/todos/${id}`)
    const get = await request.get(`/api/todos/${id}`)
    expect(get.status()).toBe(404)
  })

  test('bulk delete clears all todos', async ({ request }) => {
    await request.post('/api/todos', { data: { text: 'bulk 1' } })
    await request.post('/api/todos', { data: { text: 'bulk 2' } })
    await request.post('/api/todos', { data: { text: 'bulk 3' } })
    await request.delete('/api/todos')
    const res = await request.get('/api/todos')
    expect((await res.json()).length).toBe(0)
  })

  test('rapid sequential inserts all persist', async ({ request }) => {
    const prefix = `rapid-${Date.now()}`
    for (let i = 0; i < 10; i++) {
      await request.post('/api/todos', { data: { text: `${prefix}-${i}` } })
    }
    const res = await request.get('/api/todos')
    const todos = await res.json()
    expect(todos.filter((t: any) => t.text.startsWith(prefix)).length).toBe(10)
  })

  test('special characters in todo text', async ({ request }) => {
    const texts = [
      'quotes "and" \'singles\'',
      '<script>alert("xss")</script>',
      'emoji 🎉🔥',
      'unicode: 日本語 中文 한국어',
    ]
    for (const text of texts) {
      await request.post('/api/todos', { data: { text } })
    }
    const res = await request.get('/api/todos')
    const todos = await res.json()
    for (const text of texts) {
      expect(todos.some((t: any) => t.text === text)).toBe(true)
    }
  })

  test('concurrent inserts do not lose data', async ({ request }) => {
    const prefix = `concurrent-${Date.now()}`
    await Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        request.post('/api/todos', { data: { text: `${prefix}-${i}` } })
      )
    )
    const res = await request.get('/api/todos')
    const todos = await res.json()
    expect(todos.filter((t: any) => t.text.startsWith(prefix)).length).toBe(5)
  })

  test('each todo gets unique id', async ({ request }) => {
    const ids = new Set<string>()
    for (let i = 0; i < 5; i++) {
      const res = await request.post('/api/todos', { data: { text: `unique-${i}` } })
      const { id } = await res.json()
      ids.add(id)
    }
    expect(ids.size).toBe(5)
  })

  test('get individual todo by id', async ({ request }) => {
    const create = await request.post('/api/todos', { data: { text: 'get by id' } })
    const { id } = await create.json()
    const res = await request.get(`/api/todos/${id}`)
    const todo = await res.json()
    expect(todo.id).toBe(id)
    expect(todo.text).toBe('get by id')
  })

  test('get nonexistent todo returns 404', async ({ request }) => {
    const res = await request.get('/api/todos/nonexistent-id')
    expect(res.status()).toBe(404)
  })

  test('created_at is monotonically increasing', async ({ request }) => {
    const timestamps: number[] = []
    for (let i = 0; i < 3; i++) {
      const res = await request.post('/api/todos', { data: { text: `ts-${i}` } })
      const todo = await res.json()
      timestamps.push(Number(todo.created_at))
      await new Promise((r) => setTimeout(r, 10))
    }
    for (let i = 1; i < timestamps.length; i++) {
      expect(timestamps[i]).toBeGreaterThanOrEqual(timestamps[i - 1])
    }
  })
})
