/**
 * minimal local s3-compatible server.
 * handles GET/PUT/DELETE/HEAD for object storage.
 */

import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  writeFileSync,
  unlinkSync,
  statSync,
} from 'node:fs'
import {
  createServer,
  type Server,
  type IncomingMessage,
  type ServerResponse,
} from 'node:http'
import { join, dirname, extname } from 'node:path'

import { log } from './log.js'

export interface S3LocalConfig {
  port: number
  dataDir: string
}

const MIME_TYPES: Record<string, string> = {
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.png': 'image/png',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.svg': 'image/svg+xml',
  '.json': 'application/json',
  '.txt': 'text/plain',
  '.pdf': 'application/pdf',
  '.mp4': 'video/mp4',
  '.mp3': 'audio/mpeg',
}

function corsHeaders(): Record<string, string> {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, PUT, DELETE, HEAD, OPTIONS',
    'Access-Control-Allow-Headers': '*',
    'Access-Control-Expose-Headers': 'ETag, Content-Length',
  }
}

export function startS3Local(config: S3LocalConfig): Promise<Server> {
  const storageDir = join(config.dataDir, 's3')
  mkdirSync(storageDir, { recursive: true })

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const headers = corsHeaders()

    if (req.method === 'OPTIONS') {
      res.writeHead(200, headers)
      res.end()
      return
    }

    const url = new URL(req.url || '/', `http://localhost:${config.port}`)

    // sanitize path to prevent traversal
    const normalized = url.pathname
      .split('/')
      .filter((s) => s && s !== '..' && s !== '.')
      .join('/')
    const filePath = join(storageDir, normalized)
    if (!filePath.startsWith(storageDir)) {
      res.writeHead(403, headers)
      res.end()
      return
    }

    try {
      switch (req.method) {
        case 'GET': {
          // S3 ListObjectsV2: GET /?list-type=2&prefix=...&max-keys=...
          if (url.searchParams.get('list-type') === '2') {
            const prefix = url.searchParams.get('prefix') || ''
            const maxKeys = parseInt(url.searchParams.get('max-keys') || '1000')
            const baseDir = join(storageDir, prefix)
            const keys: string[] = []

            function walkList(dir: string) {
              if (keys.length >= maxKeys) return
              let entries
              try {
                entries = readdirSync(dir, { withFileTypes: true })
              } catch {
                return
              }
              for (const entry of entries) {
                if (keys.length >= maxKeys) break
                const full = join(dir, entry.name)
                if (entry.isDirectory()) {
                  walkList(full)
                } else {
                  const rel = full.slice(storageDir.length + 1)
                  keys.push(rel)
                }
              }
            }
            walkList(baseDir)

            const keysXml = keys
              .map((k) => `<Contents><Key>${k}</Key></Contents>`)
              .join('')
            const xml = `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><KeyCount>${keys.length}</KeyCount>${keysXml}</ListBucketResult>`
            res.writeHead(200, { ...headers, 'Content-Type': 'application/xml' })
            res.end(xml)
            return
          }

          if (!existsSync(filePath) || statSync(filePath).isDirectory()) {
            res.writeHead(404, {
              ...headers,
              'Content-Type': 'application/xml',
            })
            res.end('<Error><Code>NoSuchKey</Code></Error>')
            return
          }
          const data = readFileSync(filePath)
          const ext = extname(filePath)
          const contentType = MIME_TYPES[ext] || 'application/octet-stream'
          res.writeHead(200, {
            ...headers,
            'Content-Type': contentType,
            'Content-Length': data.length.toString(),
            ETag: `"${Buffer.from(data).length}"`,
          })
          res.end(data)
          break
        }

        case 'PUT': {
          const chunks: Buffer[] = []
          req.on('data', (chunk: Buffer) => chunks.push(chunk))
          req.on('end', () => {
            mkdirSync(dirname(filePath), { recursive: true })
            const body = Buffer.concat(chunks)
            writeFileSync(filePath, body)
            res.writeHead(200, {
              ...headers,
              ETag: `"${body.length}"`,
            })
            res.end()
          })
          break
        }

        case 'DELETE': {
          if (existsSync(filePath)) {
            unlinkSync(filePath)
          }
          res.writeHead(204, headers)
          res.end()
          break
        }

        case 'HEAD': {
          if (!existsSync(filePath) || statSync(filePath).isDirectory()) {
            res.writeHead(404, headers)
            res.end()
            return
          }
          const stat = statSync(filePath)
          const ext2 = extname(filePath)
          res.writeHead(200, {
            ...headers,
            'Content-Type': MIME_TYPES[ext2] || 'application/octet-stream',
            'Content-Length': stat.size.toString(),
          })
          res.end()
          break
        }

        default:
          res.writeHead(405, headers)
          res.end()
      }
    } catch {
      res.writeHead(500, {
        ...headers,
        'Content-Type': 'application/xml',
      })
      res.end('<Error><Code>InternalError</Code></Error>')
    }
  })

  return new Promise((resolve, reject) => {
    server.listen(config.port, '127.0.0.1', () => {
      log.s3(`listening on port ${config.port}`)
      resolve(server)
    })
    server.on('error', reject)
  })
}
