// the fixture app server: the /query (named-query transform) and /mutate
// (custom mutator execution) endpoints zero-cache forwards to via
// ZERO_QUERY_URL / ZERO_MUTATE_URL. this is the same role soot's app worker
// plays in prod. kept endpoint-shaped (name/args in, AST/results out) so the
// orez targets can reuse it unchanged.
import { createServer, type Server } from 'node:http'
import type { ReadonlyJSONValue } from '@rocicorp/zero'
import {
  handleMutateRequest,
  handleQueryRequest,
  zeroPostgresJS,
} from '@rocicorp/zero/pg'
import { mutators, schema, zql } from './fixture.js'

// server-side transform: query name + args -> zql AST. explicit switch, no
// registry lookup, so permission filters per query have an obvious home.
function transformQuery(name: string, args: ReadonlyJSONValue | undefined) {
  switch (name) {
    case 'allProjects':
      return zql.project.related('members')
    case 'projectById':
      return zql.project.where('id', (args as { id: string }).id).one()
    default:
      throw new Error(`unknown query: ${name}`)
  }
}

// walk the mutator registry defs by dotted name -> MutatorDefinition
function mustGetMutatorDef(name: string) {
  let node: unknown = mutators
  for (const part of name.split('.')) {
    node = (node as Record<string, unknown>)[part]
    if (!node) throw new Error(`unknown mutator: ${name}`)
  }
  return node as { fn: (opts: { tx: unknown; args: unknown; ctx: unknown }) => Promise<void> }
}

// fixture auth: zero-cache forwards the client's raw token as a bearer
// header; the app server authenticates it and echoes the userID back —
// zero-cache pins the connection to that server-validated userID
function userIDFromAuth(header: string | undefined): string | null {
  const token = header?.match(/^Bearer token-(.+)$/)?.[1]
  return token ?? null
}

export async function startAppServer(opts: { dbUrl: string; port: number }) {
  const db = zeroPostgresJS(schema, opts.dbUrl)

  const server: Server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? '/', 'http://localhost')
      const chunks: Buffer[] = []
      for await (const chunk of req) chunks.push(chunk as Buffer)
      const body = JSON.parse(Buffer.concat(chunks).toString() || 'null') as ReadonlyJSONValue
      const query = Object.fromEntries(url.searchParams)
      const userID = userIDFromAuth(req.headers.authorization)

      if (url.pathname === '/query') {
        const response = await handleQueryRequest({
          handler: transformQuery,
          schema,
          userID,
          query,
          body,
        })
        res.setHeader('content-type', 'application/json')
        res.end(JSON.stringify(response))
        return
      }

      if (url.pathname === '/mutate') {
        const response = await handleMutateRequest({
          dbProvider: db,
          userID,
          handler: (transact, _mutation) =>
            transact(async (tx, name, args) => {
              await mustGetMutatorDef(name).fn({ tx, args, ctx: undefined })
            }),
          query,
          body,
        })
        res.setHeader('content-type', 'application/json')
        res.end(JSON.stringify(response))
        return
      }

      res.statusCode = 404
      res.end()
    } catch (error) {
      console.error('[app-server]', error)
      res.statusCode = 500
      res.end(JSON.stringify({ error: String(error) }))
    }
  })

  await new Promise<void>((resolve) => server.listen(opts.port, '127.0.0.1', resolve))

  return {
    url: `http://127.0.0.1:${opts.port}`,
    async close() {
      await new Promise<void>((resolve, reject) =>
        server.close((err) => (err ? reject(err) : resolve()))
      )
    },
  }
}
