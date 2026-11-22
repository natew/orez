import { basename, dirname, resolve } from 'node:path'

import { defineCommand } from 'citty'

import { prepareDataApp } from './app-modules.js'
import { generate, watch } from './generate.js'

export const generateCommand = defineCommand({
  meta: {
    name: 'generate',
    description: 'Generate models, types, tables, and query validators',
  },
  args: {
    dir: {
      type: 'positional',
      description:
        'Data directory or explicit data.config.ts path (defaults to src/data)',
      required: false,
      default: 'src/data',
    },
    watch: {
      type: 'boolean',
      description: 'Watch for changes and regenerate',
      required: false,
      default: false,
    },
    tsconfig: {
      type: 'string',
      description: 'TypeScript project used only for generation',
      required: false,
    },
    after: {
      type: 'string',
      description: 'Command to run after generation completes',
      required: false,
    },
    force: {
      type: 'boolean',
      description: 'Ignore cached inputs and regenerate all outputs',
      required: false,
      default: false,
    },
  },

  async run({ args }) {
    const target = resolve(args.dir)
    const config = basename(target) === 'data.config.ts' ? target : undefined
    const opts = {
      dir: config ? dirname(config) : target,
      config,
      tsconfig: args.tsconfig,
      after: args.after,
      force: args.force,
    }

    if (args.watch) {
      await watch(opts)
      await new Promise(() => {})
    } else {
      await generate(opts)
    }
  },
})

export const prepareCommand = defineCommand({
  meta: {
    name: 'prepare',
    description: 'Prepare typed data modules for tools that cannot load Bun plugins',
  },
  args: {
    dir: {
      type: 'positional',
      description: 'Data directory (defaults to data)',
      required: false,
    },
    schema: {
      type: 'string',
      description: 'Module exporting schema (defaults to <dir>/schema.ts)',
    },
    tsconfig: {
      type: 'string',
      description: 'TypeScript project used only for generation',
    },
  },
  async run({ args }) {
    await prepareDataApp(process.cwd(), args)
  },
})
