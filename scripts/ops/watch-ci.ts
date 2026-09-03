#!/usr/bin/env bun
// Block until every GitHub workflow run for a commit reaches a terminal state,
// then exit 0 if they all succeeded and 1 otherwise.
//
// This exists so an agent waiting on CI spends no turns polling: run it as a
// background task and the harness wakes you once, with the answer. Polling
// happens inside this process, where it is free. The interval is deliberately
// coarse so one monitor does not waste GitHub API calls.
//
//   bun scripts/ops/watch-ci.ts [--sha <sha>] [--workflow <file>] [--interval <seconds>]
//   bun scripts/ops/watch-ci.ts --run <id>          # one run, by id

const KNOWN_FLAGS = ['sha', 'run', 'workflow', 'interval', 'timeout']
const args = process.argv.slice(2)
// an unknown flag used to fall through to the HEAD default, so a watch on a
// mistyped argument reported a confident verdict about a different commit.
for (const arg of args) {
  if (arg.startsWith('--') && !KNOWN_FLAGS.includes(arg.slice(2))) {
    throw new Error(`unknown flag ${arg}; expected one of ${KNOWN_FLAGS.join(', ')}`)
  }
}
const flag = (name: string): string | undefined => {
  const index = args.indexOf(`--${name}`)
  return index === -1 ? undefined : args[index + 1]
}
const workflow = flag('workflow')
const runID = flag('run')
if (runID && (flag('sha') || workflow)) {
  throw new Error('--run watches one run; it takes no --sha or --workflow')
}

// `gh run list --commit` matches the full 40-character sha only: an
// abbreviated one silently returns no runs, which reads as "CI has not started"
// forever rather than as a bad argument.
const sha = runID
  ? ''
  : new TextDecoder()
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

async function watchedRuns(): Promise<Run[]> {
  const proc = Bun.spawn(
    runID
      ? ['gh', 'run', 'view', runID, '--json', 'name,status,conclusion,url']
      : [
          'gh',
          'run',
          'list',
          ...(workflow ? ['--workflow', workflow] : []),
          '--commit',
          sha,
          // a busy repo fans one push out past 30 runs, and a truncated list
          // reads as "everything finished" while runs are still going.
          '--limit',
          '100',
          '--json',
          'name,status,conclusion,url',
        ]
  )
  const text = new TextDecoder().decode(await new Response(proc.stdout).arrayBuffer())
  if ((await proc.exited) !== 0) {
    throw new Error(`gh run ${runID ? 'view' : 'list'} failed for ${runID || sha}`)
  }
  const parsed = JSON.parse(text || (runID ? '{}' : '[]'))
  return (Array.isArray(parsed) ? parsed : [parsed]) as Run[]
}

const target = runID
  ? `run ${runID}`
  : `${workflow ?? 'all workflows'} for ${sha.slice(0, 12)}`
console.log(`[watch-ci] waiting on ${target} every ${intervalMs / 1000}s`)
for (;;) {
  let runs: Run[]
  try {
    runs = await watchedRuns()
  } catch (error) {
    // a rate limit or a transient gh failure is not a CI verdict; wait it out
    // rather than reporting a result this process never observed.
    console.log(`[watch-ci] ${String(error)}`)
    runs = []
  }
  const pending = runs.filter((run) => run.status !== 'completed')
  if (runs.length > 0 && pending.length === 0) {
    // a repo that coalesces pushes cancels and skips runs as a matter of
    // course, and neither is a verdict on the commit: a skip means a path
    // filter did not match, a cancel means a newer push owns the answer.
    // calling them failures made every push into a busy repo read as a
    // catastrophe and buried the runs that actually broke.
    const failed = runs.filter(
      (run) =>
        run.conclusion !== 'success' &&
        run.conclusion !== 'skipped' &&
        run.conclusion !== 'cancelled'
    )
    const inconclusive = runs.filter(
      (run) => run.conclusion === 'skipped' || run.conclusion === 'cancelled'
    )
    for (const run of runs) console.log(`[watch-ci] ${run.conclusion} ${run.name}`)
    if (inconclusive.length > 0) {
      console.log(
        `[watch-ci] ${inconclusive.length} run(s) cancelled or skipped, so they carry no verdict`
      )
    }
    if (failed.length === 0) {
      console.log(
        `[watch-ci] ${runs.length - inconclusive.length}/${runs.length} runs succeeded, none failed`
      )
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
