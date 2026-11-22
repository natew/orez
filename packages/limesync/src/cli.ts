#!/usr/bin/env node

function usage(): never {
  console.error(
    'usage: limesync dev [--config limesync.config.ts] -- <command> [args...]\n       limesync generate [dir] [--watch]'
  )
  process.exit(1)
}

const [subcommand, ...args] = process.argv.slice(2)
if (subcommand === 'dev') {
  const { runDev } = await import('./dev-cli.js')
  await runDev(args, usage)
} else if (subcommand === 'generate') {
  const [{ runMain }, { generateCommand }] = await Promise.all([
    import('citty'),
    import('./data/cli.js'),
  ])
  await runMain(generateCommand, { rawArgs: args })
} else {
  usage()
}
