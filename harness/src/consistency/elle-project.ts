// projects a recorded consistency history (history.jsonl) into the Jepsen/Elle
// list-append JSON that elle-cli consumes.
//
// projectElleListAppend is a lossless generic projection. A live lane like
// atomic-visibility reads real projects that already hold seed rows, so its
// observed lists contain values that no transaction in the history appended.
// Elle's list-append model requires every observed element to be explained by an
// append, so this CLI restricts each observed list to the values appended within
// the history: it extracts the tracked list-append sub-history embedded in the
// larger store. Because it drops values outside the append universe, this
// projection checks dependency safety among the tracked appends and does NOT
// detect a read of a value that no transaction appended. Append order is
// preserved for the observed appends (the atomic-visibility workload appends one
// unique rank per key). A non-vacuity guard rejects a history with no appends or
// no reads.
//
//   bun src/consistency/elle-project.ts --history <path> --out <path>
import { readFileSync, writeFileSync } from 'node:fs'
import { parseArgs } from 'node:util'

import { projectElleListAppend, type HistoryEvent } from './history.js'

const { values } = parseArgs({
  options: {
    history: { type: 'string' },
    out: { type: 'string' },
  },
})

if (values.history === undefined) {
  throw new Error('usage: elle-project.ts --history <history.jsonl> [--out <file>]')
}

const events: HistoryEvent[] = readFileSync(values.history, 'utf8')
  .split('\n')
  .filter((line) => line.trim() !== '')
  .map((line) => JSON.parse(line) as HistoryEvent)

const projected = projectElleListAppend(events)

// gather every value appended per key across the whole history: this is the
// list-append universe elle can reason about.
const appendedByKey = new Map<string, Set<number>>()
for (const txn of projected) {
  for (const op of txn.value) {
    if (op[0] === 'append') {
      const values = appendedByKey.get(op[1]) ?? new Set<number>()
      values.add(op[2])
      appendedByKey.set(op[1], values)
    }
  }
}

let appendOps = 0
let readOps = 0
const restricted = projected.map((txn) => ({
  ...txn,
  value: txn.value.map((op) => {
    if (op[0] === 'append') {
      appendOps++
      return op
    }
    readOps++
    if (op[2] === null) return op
    const universe = appendedByKey.get(op[1]) ?? new Set<number>()
    return ['r', op[1], op[2].filter((value) => universe.has(value))] as [
      'r',
      string,
      number[],
    ]
  }),
}))

if (restricted.length === 0) {
  throw new Error('projected elle history is empty; no list-append transactions to check')
}
if (appendOps === 0 || readOps === 0) {
  throw new Error(
    `projected elle history is vacuous: ${appendOps} append and ${readOps} read micro-ops`
  )
}

const serialized = `${JSON.stringify(restricted, null, 2)}\n`
if (values.out === undefined) {
  process.stdout.write(serialized)
} else {
  writeFileSync(values.out, serialized)
}

process.stderr.write(
  `[elle-project] ${restricted.length} txns, ${appendOps} appends, ${readOps} reads\n`
)
