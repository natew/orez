#!/usr/bin/env node
import { defineCommand, runMain } from 'citty'

import { startZeroLite } from './index'
import { startS3Local } from './s3-local'

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
    'web-port': {
      type: 'string',
      description: 'web server port (for zero-cache mutate/query urls)',
      default: '8081',
    },
    'data-dir': {
      type: 'string',
      description: 'data directory',
      default: '.zero-lite',
    },
    migrations: {
      type: 'string',
      description: 'migrations directory',
      default: 'src/database/migrations',
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
  },
  subCommands: {
    s3: s3Command,
  },
  async run({ args }) {
    const { config, stop } = await startZeroLite({
      pgPort: Number(args['pg-port']),
      zeroPort: Number(args['zero-port']),
      webPort: Number(args['web-port']),
      dataDir: args['data-dir'],
      migrationsDir: args.migrations,
      seedFile: args.seed,
      pgUser: args['pg-user'],
      pgPassword: args['pg-password'],
      skipZeroCache: args['skip-zero-cache'],
    })

    console.info(`[orez] ready`)
    console.info(
      `[orez]   pg: postgresql://${config.pgUser}:${config.pgPassword}@127.0.0.1:${config.pgPort}/postgres`
    )
    if (!config.skipZeroCache) {
      console.info(`[orez]   zero-cache: http://localhost:${config.zeroPort}`)
    }

    process.on('SIGINT', async () => {
      await stop()
      process.exit(0)
    })
    process.on('SIGTERM', async () => {
      await stop()
      process.exit(0)
    })
  },
})

runMain(main)
