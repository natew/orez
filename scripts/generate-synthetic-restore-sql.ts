#!/usr/bin/env bun
/**
 * Generate a synthetic pg_dump-style SQL file for restore stress testing.
 *
 * Usage:
 *   bun scripts/generate-synthetic-restore-sql.ts
 *   bun scripts/generate-synthetic-restore-sql.ts --output /tmp/restore.sql --tables 12 --rows 6000 --cols 10 --payload 128
 */

import { createWriteStream } from 'node:fs'
import { mkdir } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'

function arg(name: string): string | undefined {
  const idx = process.argv.indexOf(name)
  if (idx === -1 || idx + 1 >= process.argv.length) return undefined
  return process.argv[idx + 1]
}

function intArg(name: string, fallback: number): number {
  const raw = arg(name)
  if (!raw) return fallback
  const n = Number(raw)
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback
}

function escapeCopy(val: string): string {
  return val
    .replace(/\\/g, '\\\\')
    .replace(/\t/g, '\\t')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
}

async function main() {
  const tables = intArg('--tables', 10)
  const rowsPerTable = intArg('--rows', 5000)
  const cols = intArg('--cols', 10)
  const payloadBytes = intArg('--payload', 120)
  const out =
    arg('--output') ??
    `/tmp/orez-synthetic-restore-${Date.now()}-${tables}x${rowsPerTable}.sql`
  const output = resolve(out)

  await mkdir(dirname(output), { recursive: true })
  const ws = createWriteStream(output, { encoding: 'utf-8' })

  const write = (line: string) =>
    new Promise<void>((resolveWrite, rejectWrite) => {
      ws.write(line + '\n', (err) => {
        if (err) rejectWrite(err)
        else resolveWrite()
      })
    })

  let bytes = 0
  const writeTracked = async (line: string) => {
    bytes += Buffer.byteLength(line) + 1
    await write(line)
  }

  await writeTracked('SET statement_timeout = 0;')
  await writeTracked("SET client_encoding = 'UTF8';")
  await writeTracked('SET standard_conforming_strings = on;')
  await writeTracked('')

  for (let t = 0; t < tables; t++) {
    const table = `synthetic_restore_${t}`
    const colDefs = Array.from({ length: cols }, (_, i) => `c_${i} TEXT`)
    const colList = Array.from({ length: cols }, (_, i) => `c_${i}`).join(', ')

    await writeTracked(
      `CREATE TABLE IF NOT EXISTS ${table} (id BIGINT PRIMARY KEY, ${colDefs.join(', ')});`
    )
    await writeTracked(`COPY ${table} (id, ${colList}) FROM stdin;`)

    for (let r = 0; r < rowsPerTable; r++) {
      const id = t * 1_000_000 + r + 1
      const values = Array.from({ length: cols }, (_, c) => {
        if (r % 101 === 0 && c === 0) return '\\N'
        const prefix = `t${t}_r${r}_c${c}_`
        return escapeCopy(prefix + 'x'.repeat(Math.max(1, payloadBytes - prefix.length)))
      })
      await writeTracked(`${id}\t${values.join('\t')}`)
    }

    await writeTracked('\\.')
    await writeTracked('')
  }

  await new Promise<void>((resolveEnd, rejectEnd) => {
    ws.end((err) => {
      if (err) rejectEnd(err)
      else resolveEnd()
    })
  })

  const mb = (bytes / (1024 * 1024)).toFixed(2)
  console.log(`wrote ${output}`)
  console.log(`size ~${mb} MB, tables=${tables}, rows=${rowsPerTable}, cols=${cols}`)
}

void main().catch((err) => {
  console.error(err)
  process.exit(1)
})
