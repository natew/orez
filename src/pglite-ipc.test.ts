import { describe, test, expect, beforeAll, afterAll } from 'vitest'

import { PGliteWorkerProxy } from './pglite-ipc.js'

describe('PGliteWorkerProxy', () => {
  let proxy: PGliteWorkerProxy

  beforeAll(async () => {
    proxy = new PGliteWorkerProxy({
      dataDir: 'memory://',
      name: 'test',
      withExtensions: false,
      debug: 0,
      pgliteOptions: {},
    })
    await proxy.waitReady
  }, 30_000)

  afterAll(async () => {
    await proxy.close()
  })

  test('exec creates table', async () => {
    await proxy.exec(`
      CREATE TABLE test_items (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )
    `)
  })

  test('query returns rows', async () => {
    await proxy.exec(`INSERT INTO test_items (name) VALUES ('hello')`)
    await proxy.exec(`INSERT INTO test_items (name) VALUES ('world')`)

    const result = await proxy.query<{ id: number; name: string }>(
      'SELECT * FROM test_items ORDER BY id'
    )
    expect(result.rows).toHaveLength(2)
    expect(result.rows[0].name).toBe('hello')
    expect(result.rows[1].name).toBe('world')
  })

  test('query with params', async () => {
    const result = await proxy.query<{ name: string }>(
      'SELECT name FROM test_items WHERE name = $1',
      ['world']
    )
    expect(result.rows).toHaveLength(1)
    expect(result.rows[0].name).toBe('world')
  })

  test('exec returns affectedRows', async () => {
    const result = await proxy.exec(`DELETE FROM test_items WHERE name = 'hello'`)
    expect(result[0].affectedRows).toBe(1)
  })

  test('execProtocolRaw handles wire protocol', async () => {
    // simple query message: SELECT 1 as num
    const query = 'SELECT 1 as num\0'
    const encoder = new TextEncoder()
    const queryBytes = encoder.encode(query)
    const buf = new Uint8Array(5 + queryBytes.length)
    buf[0] = 0x51 // 'Q' simple query
    new DataView(buf.buffer).setInt32(1, 4 + queryBytes.length)
    buf.set(queryBytes, 5)

    const result = await proxy.execProtocolRaw(buf)
    expect(result).toBeInstanceOf(Uint8Array)
    expect(result.length).toBeGreaterThan(0)
    // should contain a ReadyForQuery message (0x5a)
    let hasRfq = false
    for (let i = 0; i < result.length; i++) {
      if (result[i] === 0x5a) {
        hasRfq = true
        break
      }
    }
    expect(hasRfq).toBe(true)
  })

  test('listen receives notifications', async () => {
    const received: string[] = []
    const unsub = await proxy.listen('test_channel', (payload) => {
      received.push(payload)
    })

    await proxy.exec(`NOTIFY test_channel, 'hello'`)
    // give notification time to propagate
    await new Promise((r) => setTimeout(r, 100))

    expect(received).toContain('hello')
    await unsub()
  })

  test('error propagation with SQL code', async () => {
    await expect(proxy.exec('SELECT * FROM nonexistent_table')).rejects.toThrow()
  })
})
