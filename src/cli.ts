#!/usr/bin/env node
import { spawn } from 'node:child_process'

import { defineCommand, runMain } from 'citty'

import { startZeroLite } from './index.js'
import { log } from './log.js'

const s3Command = defineCommand({
  meta: {
    name: 's3',
    description: 'start a local s3-compatible server',
  },
  args: {
    port: {
      type: 'string',
      description: 'port to listen on',
      default: '9200',
    },
    'data-dir': {
      type: 'string',
      description: 'data directory for stored files',
      default: '.orez',
    },
  },
  async run({ args }) {
    const { startS3Local } = await import('./s3-local.js')
    const server = await startS3Local({
      port: Number(args.port),
      dataDir: args['data-dir'],
    })

    process.on('SIGINT', () => {
      server.close()
      process.exit(0)
    })
    process.on('SIGTERM', () => {
      server.close()
      process.exit(0)
    })
  },
})

const main = defineCommand({
  meta: {
    name: 'orez',
    description: 'pglite-powered zero-sync development backend',
  },
  args: {
    'pg-port': {
      type: 'string',
      description: 'postgresql proxy port',
      default: '6434',
    },
    'zero-port': {
      type: 'string',
      description: 'zero-cache port',
      default: '5849',
    },
    'data-dir': {
      type: 'string',
      description: 'data directory',
      default: '.orez',
    },
    migrations: {
      type: 'string',
      description: 'migrations directory',
      default: '',
    },
    seed: {
      type: 'string',
      description: 'seed file path',
      default: '',
    },
    'pg-user': {
      type: 'string',
      description: 'postgresql user',
      default: 'user',
    },
    'pg-password': {
      type: 'string',
      description: 'postgresql password',
      default: 'password',
    },
    'skip-zero-cache': {
      type: 'boolean',
      description: 'run pglite + proxy only, skip zero-cache',
      default: false,
    },
    'log-level': {
      type: 'string',
      description: 'log level: error, warn, info, debug',
      default: 'info',
    },
    s3: {
      type: 'boolean',
      description: 'also start a local s3-compatible server',
      default: false,
    },
    's3-port': {
      type: 'string',
      description: 's3 server port',
      default: '9200',
    },
    'disable-wasm-sqlite': {
      type: 'boolean',
      description: 'use native @rocicorp/zero-sqlite3 instead of wasm bedrock-sqlite',
      default: true,
    },
    'on-db-ready': {
      type: 'string',
      description: 'command to run after db+proxy are ready, before zero-cache starts',
      default: '',
    },
    'on-healthy': {
      type: 'string',
      description: 'command to run once all services are healthy',
      default: '',
    },
  },
  subCommands: {
    s3: s3Command,
  },
  async run({ args }) {
    const { config, stop } = await startZeroLite({
      pgPort: Number(args['pg-port']),
      zeroPort: Number(args['zero-port']),
      dataDir: args['data-dir'],
      migrationsDir: args.migrations,
      seedFile: args.seed,
      pgUser: args['pg-user'],
      pgPassword: args['pg-password'],
      skipZeroCache: args['skip-zero-cache'],
      disableWasmSqlite: args['disable-wasm-sqlite'],
      logLevel: args['log-level'] as 'error' | 'warn' | 'info' | 'debug',
      onDbReady: args['on-db-ready'],
    })

    let s3Server: import('node:http').Server | null = null
    if (args.s3) {
      const { startS3Local } = await import('./s3-local.js')
      s3Server = await startS3Local({
        port: Number(args['s3-port']),
        dataDir: args['data-dir'],
      })
    }

    log.orez('ready')
    log.orez(
      `pg: postgresql://${config.pgUser}:${config.pgPassword}@127.0.0.1:${config.pgPort}/postgres`
    )
    if (!config.skipZeroCache) {
      log.zero(`http://localhost:${config.zeroPort}`)
    }

    if (args['on-healthy']) {
      log.orez(`running on-healthy: ${args['on-healthy']}`)
      const child = spawn(args['on-healthy'], {
        shell: true,
        stdio: 'inherit',
        env: {
          ...process.env,
          OREZ_PG_PORT: String(config.pgPort),
          OREZ_ZERO_PORT: String(config.zeroPort),
        },
      })
      child.on('exit', (code) => {
        if (code !== 0 && code !== null) {
          log.orez(`on-healthy command exited with code ${code}`)
        }
      })
    }

    process.on('SIGINT', async () => {
      s3Server?.close()
      await stop()
      process.exit(0)
    })
    process.on('SIGTERM', async () => {
      s3Server?.close()
      await stop()
      process.exit(0)
    })
  },
})

runMain(main)
