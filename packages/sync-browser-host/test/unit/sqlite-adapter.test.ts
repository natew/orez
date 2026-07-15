import { expect, test } from 'bun:test'

import { assertConsumerSql } from '../../src/sqlite-adapter.js'

test('consumer SQL cannot own transactions or numbered parameters', () => {
  expect(() => assertConsumerSql('BEGIN')).toThrow('transaction SQL is host-owned')
  expect(() => assertConsumerSql('  SAVEPOINT nested')).toThrow(
    'transaction SQL is host-owned'
  )
  expect(() => assertConsumerSql('SELECT ?1')).toThrow(
    'numbered parameters are forbidden'
  )
  expect(() =>
    assertConsumerSql('CREATE TRIGGER changed AFTER INSERT ON item BEGIN SELECT 1; END')
  ).not.toThrow()
})
