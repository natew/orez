#!/usr/bin/env bun
// Block until every GitHub workflow run for a commit reaches a terminal state,
// then exit 0 if they all succeeded and 1 otherwise.
//
// This exists so an agent waiting on CI spends no turns polling: run it as a
// background task and the harness wakes you once, with the answer. Polling
// happens inside this process, where it is free. The interval is deliberately
// coarse — GitHub allows 60 REST calls an hour and `gh run watch` alone drains
// that in about three minutes.
//
//   bun scripts/ops/watch-ci.ts [--sha <sha>] [--interval <seconds>]

const args = process.argv.slice(2)
const flag = (name: string): string | undefined => {
  const index = args.indexOf(`--${name}`)
  return index === -1 ? undefined : args[index + 1]
}

// `gh run list --commit` matches the full 40-character sha only: an
// abbreviated one silently returns no runs, which reads as "CI has not started"
// forever rather than as a bad argument.
const sha = new TextDecoder()
  .decode(
    await new Response(
      Bun.spawn(['git', 'rev-parse', flag('sha') ?? 'HEAD']).stdout
    ).arrayBuffer()
  )
  .trim()
const intervalMs = Math.max(60, Number(flag('interval') ?? 180)) * 1000
const deadline = Date.now() + Math.max(60, Number(flag('timeout') ?? 2400)) * 1000

type Run = {
  name: string
  status: string
  conclusion: string | null
  url: string
}

async function runsForSha(): Promise<Run[]> {
  const proc = Bun.spawn([
    'gh',
    'run',
    'list',
    '--commit',
    sha,
    '--limit',
    '30',
    '--json',
    'name,status,conclusion,url',
  ])
  const text = new TextDecoder().decode(await new Response(proc.stdout).arrayBuffer())
  if ((await proc.exited) !== 0) throw new Error(`gh run list failed for ${sha}`)
  return JSON.parse(text || '[]') as Run[]
}

console.log(`[watch-ci] waiting on ${sha.slice(0, 12)} every ${intervalMs / 1000}s`)
for (;;) {
  let runs: Run[]
  try {
    runs = await runsForSha()
  } catch (error) {
    // a rate limit or a transient gh failure is not a CI verdict; wait it out
    // rather than reporting a result this process never observed.
    console.log(`[watch-ci] ${String(error)}`)
    runs = []
  }
  const pending = runs.filter((run) => run.status !== 'completed')
  if (runs.length > 0 && pending.length === 0) {
    const failed = runs.filter((run) => run.conclusion !== 'success')
    for (const run of runs) console.log(`[watch-ci] ${run.conclusion} ${run.name}`)
    if (failed.length === 0) {
      console.log(`[watch-ci] all ${runs.length} runs succeeded`)
      process.exit(0)
    }
    for (const run of failed) console.log(`[watch-ci] failed: ${run.name} ${run.url}`)
    process.exit(1)
  }
  if (Date.now() >= deadline) {
    console.log(`[watch-ci] timed out with ${pending.length} runs still going`)
    process.exit(2)
  }
  console.log(
    `[watch-ci] ${pending.length}/${runs.length || '?'} still running; checking again in ${intervalMs / 1000}s`
  )
  await Bun.sleep(intervalMs)
}
