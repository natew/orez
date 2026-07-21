#!/usr/bin/env node
import { basename, dirname, resolve } from 'node:path'

import { defineCommand, runMain } from 'citty'

import { generate, watch } from './generate'

const generateCommand = defineCommand({
  meta: {
    name: 'generate',
    description: 'Generate models, types, tables, and query validators',
  },
  args: {
    dir: {
      type: 'positional',
      description:
        'Data directory or explicit on-zero.config.ts path (defaults to src/data)',
      required: false,
      default: 'src/data',
    },
    watch: {
      type: 'boolean',
      description: 'Watch for changes and regenerate',
      required: false,
      default: false,
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
    const config = basename(target) === 'on-zero.config.ts' ? target : undefined
    const opts = {
      dir: config ? dirname(config) : target,
      config,
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

const main = defineCommand({
  meta: {
    name: 'on-zero',
    description: 'on-zero CLI tools',
  },
  subCommands: {
    generate: generateCommand,
  },
})

runMain(main)
