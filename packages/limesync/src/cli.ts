#!/usr/bin/env node

function usage(): never {
  console.error(
    'usage: limesync dev [--config limesync.config.ts] -- <command> [args...]\n       limesync generate [dir] [--watch]\n       limesync prepare [dir]'
  )
  process.exit(1)
}

const [subcommand, ...args] = process.argv.slice(2)
if (subcommand === 'dev') {
  const { runDev } = await import('./dev-cli.js')
  await runDev(args, usage)
} else if (subcommand === 'generate' || subcommand === 'prepare') {
  const [{ runMain }, { generateCommand, prepareCommand }] = await Promise.all([
    import('citty'),
    import('./data/cli.js'),
  ])
  if (subcommand === 'generate') await runMain(generateCommand, { rawArgs: args })
  else await runMain(prepareCommand, { rawArgs: args })
} else {
  usage()
}
