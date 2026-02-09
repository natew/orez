import { readFileSync } from 'node:fs'
import { join } from 'node:path'

import postgres from 'postgres'

const PORT = Number(process.env.PORT || 3456)
const PG_PORT = Number(process.env.PG_PORT || 6435)

const sql = postgres(`postgresql://user:password@127.0.0.1:${PG_PORT}/postgres`)

const indexHtml = readFileSync(join(import.meta.dir, '../public/index.html'), 'utf-8')

const headers = { 'Content-Type': 'application/json' }

export default {
  port: PORT,
  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url)

    if (url.pathname === '/') {
      return new Response(indexHtml, { headers: { 'Content-Type': 'text/html' } })
    }

    if (url.pathname === '/api/health') {
      return new Response(JSON.stringify({ status: 'ok', timestamp: Date.now() }), {
        headers,
      })
    }

    if (url.pathname === '/api/todos' && req.method === 'GET') {
      const todos = await sql`SELECT * FROM todo ORDER BY created_at DESC`
      return new Response(JSON.stringify(todos), { headers })
    }

    if (url.pathname === '/api/todos' && req.method === 'POST') {
      const body = await req.json()
      const id = crypto.randomUUID()
      await sql`INSERT INTO todo (id, text) VALUES (${id}, ${body.text})`
      const [todo] = await sql`SELECT * FROM todo WHERE id = ${id}`
      return new Response(JSON.stringify(todo), { headers, status: 201 })
    }

    if (url.pathname === '/api/todos' && req.method === 'DELETE') {
      await sql`DELETE FROM todo`
      return new Response(JSON.stringify({ ok: true }), { headers })
    }

    const todoMatch = url.pathname.match(/^\/api\/todos\/(.+)$/)
    if (todoMatch) {
      const id = todoMatch[1]

      if (req.method === 'GET') {
        const [todo] = await sql`SELECT * FROM todo WHERE id = ${id}`
        if (!todo) return new Response('not found', { status: 404 })
        return new Response(JSON.stringify(todo), { headers })
      }

      if (req.method === 'PATCH') {
        const body = await req.json()
        await sql`UPDATE todo SET completed = ${body.completed} WHERE id = ${id}`
        const [todo] = await sql`SELECT * FROM todo WHERE id = ${id}`
        return new Response(JSON.stringify(todo), { headers })
      }

      if (req.method === 'DELETE') {
        await sql`DELETE FROM todo WHERE id = ${id}`
        return new Response(JSON.stringify({ ok: true }), { headers })
      }
    }

    return new Response('not found', { status: 404 })
  },
}
