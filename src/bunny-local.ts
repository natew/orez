/**
 * minimal local bunny cdn storage-compatible server.
 * handles GET/PUT/DELETE/HEAD and directory listing.
 */

import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  unlinkSync,
  statSync,
  readdirSync,
} from 'node:fs'
import {
  createServer,
  type Server,
  type IncomingMessage,
  type ServerResponse,
} from 'node:http'
import { join, dirname, extname, relative } from 'node:path'

import { log } from './log.js'

export interface BunnyLocalConfig {
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
  '.css': 'text/css',
  '.js': 'application/javascript',
  '.html': 'text/html',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.otf': 'font/otf',
}

function corsHeaders(): Record<string, string> {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, PUT, DELETE, HEAD, OPTIONS',
    'Access-Control-Allow-Headers': '*',
    'Access-Control-Expose-Headers': 'Content-Length',
  }
}

// recursively collect files for directory listing
function listFiles(
  dir: string,
  baseDir: string
): Array<{
  Guid: string
  StorageZoneName: string
  Path: string
  ObjectName: string
  Length: number
  LastChanged: string
  IsDirectory: boolean
  DateCreated: string
}> {
  if (!existsSync(dir) || !statSync(dir).isDirectory()) {
    return []
  }

  const entries = readdirSync(dir, { withFileTypes: true })
  return entries.map((entry) => {
    const fullPath = join(dir, entry.name)
    const stat = statSync(fullPath)
    const relativePath = '/' + relative(baseDir, dir) + '/'

    return {
      Guid: '00000000-0000-0000-0000-000000000000',
      StorageZoneName: '',
      Path: relativePath,
      ObjectName: entry.name,
      Length: entry.isDirectory() ? 0 : stat.size,
      LastChanged: stat.mtime.toISOString(),
      IsDirectory: entry.isDirectory(),
      DateCreated: stat.birthtime.toISOString(),
    }
  })
}

export function startBunnyLocal(config: BunnyLocalConfig): Promise<Server> {
  const storageDir = join(config.dataDir, 'bunny')
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
          // directory listing if path ends with / or is a directory
          if (
            url.pathname.endsWith('/') ||
            (existsSync(filePath) && statSync(filePath).isDirectory())
          ) {
            const items = listFiles(filePath, storageDir)
            const body = JSON.stringify(items)
            res.writeHead(200, {
              ...headers,
              'Content-Type': 'application/json',
            })
            res.end(body)
            return
          }

          if (!existsSync(filePath)) {
            res.writeHead(404, { ...headers, 'Content-Type': 'application/json' })
            res.end(JSON.stringify({ HttpCode: 404, Message: 'Object Not Found' }))
            return
          }
          const data = readFileSync(filePath)
          const ext = extname(filePath)
          const contentType = MIME_TYPES[ext] || 'application/octet-stream'
          res.writeHead(200, {
            ...headers,
            'Content-Type': contentType,
            'Content-Length': data.length.toString(),
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
            res.writeHead(201, {
              ...headers,
              'Content-Type': 'application/json',
            })
            res.end(JSON.stringify({ HttpCode: 201, Message: 'File uploaded.' }))
          })
          break
        }

        case 'DELETE': {
          if (existsSync(filePath)) {
            unlinkSync(filePath)
          }
          res.writeHead(200, {
            ...headers,
            'Content-Type': 'application/json',
          })
          res.end(JSON.stringify({ HttpCode: 200, Message: 'File deleted successfuly.' }))
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
        'Content-Type': 'application/json',
      })
      res.end(JSON.stringify({ HttpCode: 500, Message: 'Internal Server Error' }))
    }
  })

  return new Promise((resolve, reject) => {
    server.listen(config.port, '127.0.0.1', () => {
      log.bunny(`listening on port ${config.port}`)
      resolve(server)
    })
    server.on('error', reject)
  })
}
