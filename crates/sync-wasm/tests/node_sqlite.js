const { DatabaseSync } = require('node:sqlite')

const fromWire = (value) => {
  switch (value.kind) {
    case 'null':
      return null
    case 'integer':
      return BigInt(value.value)
    case 'real':
    case 'text':
      return value.value
    case 'blob':
      return new Uint8Array(value.value)
    default:
      throw new Error(`unknown wire value kind: ${value.kind}`)
  }
}

const toWire = (value) => {
  if (value === null) return { kind: 'null' }
  if (typeof value === 'bigint') return { kind: 'integer', value: value.toString() }
  if (typeof value === 'number') {
    return Number.isInteger(value)
      ? { kind: 'integer', value: value.toString() }
      : { kind: 'real', value }
  }
  if (typeof value === 'string') return { kind: 'text', value }
  if (value instanceof Uint8Array) return { kind: 'blob', value: Array.from(value) }
  throw new Error(`unsupported SQLite value: ${String(value)}`)
}

module.exports.createDb = () => {
  const sqlite = new DatabaseSync(':memory:')
  return {
    exec(sql, params) {
      const bindings = params.map(fromWire)
      if (bindings.length === 0) sqlite.exec(sql)
      else sqlite.prepare(sql).run(...bindings)
    },
    query(sql, params) {
      const statement = sqlite.prepare(sql)
      statement.setReadBigInts(true)
      return statement.all(...params.map(fromWire)).map((row) => {
        const columns = Object.keys(row)
        return { columns, values: columns.map((column) => toWire(row[column])) }
      })
    },
  }
}

module.exports.execSql = (db, sql) => db.exec(sql, [])
module.exports.querySql = (db, sql) => db.query(sql, [])
