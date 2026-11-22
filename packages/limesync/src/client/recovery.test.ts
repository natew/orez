import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

import { createSchema, dropAllDatabases, string, table, Zero } from 'limesync/zero'
import { dropStore, SQLiteStore, type SQLiteDatabase } from 'limesync/zero/sqlite'
import { expect, test } from 'vitest'

import { installHttpPullTransport } from './transport.js'

test('recovers a pending SQLite write kept through reset and a schema upgrade', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'limesync-recovery-'))
  const note = table('note').columns({ id: string(), title: string() }).primaryKey('id')
  const tag = table('tag').columns({ id: string() }).primaryKey('id')
  const oldSchema = createSchema({ tables: [note], enableLegacyMutators: true })
  const schema = createSchema({ tables: [note, tag], enableLegacyMutators: true })
  const persisted = Promise.withResolvers<void>()
  const acknowledgedWrite = Promise.withResolvers<void>()
  const create = (filename: string): SQLiteDatabase => {
    const db = new DatabaseSync(filename)
    let wrotePendingMutation = false
    let wroteAcknowledgement = false
    return {
      close: () => db.close(),
      destroy: () => rmSync(filename, { force: true }),
      async exec(sql) {
        db.exec(sql)
        if (sql === 'COMMIT') {
          if (wrotePendingMutation) persisted.resolve()
          if (wroteAcknowledgement) acknowledgedWrite.resolve()
        }
      },
      prepare(sql) {
        const statement = db.prepare(sql)
        return {
          async exec(params) {
            statement.run(...params)
          },
          async all(params) {
            return statement.all(...params).map((row) => Object.values(row))
          },
          async *iterate(params) {
            for (const row of statement.iterate(...params)) yield Object.values(row)
          },
          async batch(rows) {
            for (const [key, value] of rows) {
              statement.run(key, value)
              if (value.includes('queued offline')) wrotePendingMutation = true
              if (/"lastServerAckdMutationIDs":\{"[^"]+":1\}/.test(value)) {
                wroteAcknowledgement = true
              }
            }
          },
        }
      },
    }
  }
  const kvStore = {
    singleProcess: true,
    create: (name: string) => new SQLiteStore(name, create, { directory }),
    drop: (name: string) => dropStore(name, create, { directory }),
  }
  const userID = 'recovery-user'
  const old = new Zero({ userID, schema: oldSchema, kvStore, server: null })
  const origin = 'https://limesync-recovery.test'
  const pushes: Array<{
    group: string
    mutation: { clientID: string; id: number; args: unknown }
  }> = []
  const recoveryPull = Promise.withResolvers<{ includeRows?: boolean }>()
  const transport = installHttpPullTransport({
    origin,
    async fetch(input, init) {
      const body = JSON.parse(String(init?.body))
      if (new URL(String(input)).pathname === '/push') {
        for (const mutation of body.mutations)
          pushes.push({ group: body.clientGroupID, mutation })
        return Response.json({
          pushResponse: {
            mutations: body.mutations.map(
              (mutation: { clientID: string; id: number }) => ({
                id: { clientID: mutation.clientID, id: mutation.id },
                result: {},
              })
            ),
          },
        })
      }
      const acknowledged = pushes.filter((push) => push.group === body.clientGroupID)
      if (acknowledged.length > 0) recoveryPull.resolve(body)
      return Response.json({
        cookie: 1,
        lastMutationIDChanges: Object.fromEntries(
          acknowledged.map(({ mutation }) => [mutation.clientID, mutation.id])
        ),
        rowsPatch: [],
      })
    },
  })
  let upgraded: Zero<typeof schema> | undefined
  try {
    await old.mutate.note.insert({ id: 'n1', title: 'queued offline' })
    const oldGroup = await old.clientGroupID

    const reset = await old.delete()
    await persisted.promise
    expect(reset).toEqual({ deleted: [], errors: [], kept: [expect.any(String)] })
    expect(await dropAllDatabases({ kvStore })).toEqual({
      dropped: [],
      errors: [],
      kept: reset.kept,
    })

    upgraded = new Zero({ userID, schema, kvStore, server: origin })
    expect(await upgraded.clientGroupID).not.toEqual(oldGroup)
    expect(await recoveryPull.promise).toMatchObject({ includeRows: false })
    expect(pushes).toEqual([
      {
        group: oldGroup,
        mutation: expect.objectContaining({
          clientID: old.clientID,
          id: 1,
        }),
      },
    ])
    expect(JSON.stringify(pushes[0]?.mutation.args)).toContain('queued offline')
    await acknowledgedWrite.promise
    await upgraded.close()
    const afterRecovery = await dropAllDatabases({ kvStore })
    expect(afterRecovery.kept).toEqual([])
    expect(afterRecovery.errors).toEqual([])
    expect(afterRecovery.dropped).toEqual(expect.arrayContaining(reset.kept))
  } finally {
    if (upgraded && !upgraded.closed) await upgraded.close()
    if (!old.closed) await old.close()
    transport.uninstall()
    rmSync(directory, { recursive: true, force: true })
  }
})
