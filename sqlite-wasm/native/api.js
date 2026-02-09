// better-sqlite3 compatible API for WASM SQLite (bedrock branch)
// provides Database and Statement classes matching @rocicorp/zero-sqlite3's API

'use strict'

const SQLITE_OK = 0
const SQLITE_ROW = 100
const SQLITE_DONE = 101
const SQLITE_INTEGER = 1
const SQLITE_FLOAT = 2
const SQLITE_TEXT = 3
const SQLITE_BLOB = 4
const SQLITE_NULL = 5
const SQLITE_UTF8 = 1
const SQLITE_TRANSIENT = -1
const SQLITE_DETERMINISTIC = 2048
const SQLITE_DIRECTONLY = 524288

const INT32_MIN = -2147483648
const INT32_MAX = 2147483647
const NULL = 0

let temp
const sqlite3 = {}

Module.onRuntimeInitialized = () => {
  temp = stackAlloc(8)

  const v = null
  const n = 'number'
  const s = 'string'
  const n1 = [n]
  const n2 = [n, ...n1]
  const n3 = [n, ...n2]
  const n4 = [n, ...n3]
  const n5 = [n, ...n4]

  const signatures = {
    open_v2: [n, [s, n, n, s]],
    exec: [n, n5],
    errmsg: [s, n1],
    prepare_v2: [n, n5],
    close_v2: [n, n1],
    finalize: [n, n1],
    reset: [n, n1],
    clear_bindings: [n, n1],
    bind_int: [n, n3],
    bind_int64: [n, n3],
    bind_double: [n, n3],
    bind_text: [n, n5],
    bind_blob: [n, n5],
    bind_null: [n, n2],
    bind_parameter_index: [n, [n, s]],
    bind_parameter_count: [n, n1],
    bind_parameter_name: [s, n2],
    step: [n, n1],
    column_int64: [n, n2],
    column_double: [n, n2],
    column_text: [s, n2],
    column_blob: [n, n2],
    column_type: [n, n2],
    column_name: [s, n2],
    column_count: [n, n1],
    column_bytes: [n, n2],
    column_table_name: [s, n2],
    column_origin_name: [s, n2],
    column_database_name: [s, n2],
    data_count: [n, n1],
    sql: [s, n1],
    stmt_readonly: [n, n1],
    last_insert_rowid: [n, n1],
    changes: [n, n1],
    total_changes: [n, n1],
    create_function_v2: [n, [n, s, n, n, n, n, n, n, n]],
    value_type: [n, n1],
    value_text: [s, n1],
    value_blob: [n, n1],
    value_int64: [n, n1],
    value_double: [n, n1],
    value_bytes: [n, n1],
    result_double: [v, n2],
    result_null: [v, n1],
    result_text: [v, n4],
    result_blob: [v, n4],
    result_int: [v, n2],
    result_int64: [v, n2],
    result_error: [v, n3],
    get_autocommit: [n, n1],
  }

  for (const [name, sig] of Object.entries(signatures)) {
    sqlite3[name] = cwrap(`sqlite3_${name}`, sig[0], sig[1])
  }

  // helpers

  function arrayToHeap(array) {
    const ptr = _malloc(array.byteLength)
    HEAPU8.set(array, ptr)
    return ptr
  }

  function stringToHeap(str) {
    const size = lengthBytesUTF8(str) + 1
    const ptr = _malloc(size)
    stringToUTF8(str, ptr, size)
    return ptr
  }

  function toNumberOrNot(val) {
    if (typeof val === 'bigint') {
      if (val >= Number.MIN_SAFE_INTEGER && val <= Number.MAX_SAFE_INTEGER) {
        return Number(val)
      }
      return val
    }
    return val
  }

  // sqlite error code name lookup
  const SQLITE_ERROR_NAMES = {
    1: 'SQLITE_ERROR',
    2: 'SQLITE_INTERNAL',
    3: 'SQLITE_PERM',
    4: 'SQLITE_ABORT',
    5: 'SQLITE_BUSY',
    6: 'SQLITE_LOCKED',
    7: 'SQLITE_NOMEM',
    8: 'SQLITE_READONLY',
    9: 'SQLITE_INTERRUPT',
    10: 'SQLITE_IOERR',
    11: 'SQLITE_CORRUPT',
    12: 'SQLITE_NOTFOUND',
    13: 'SQLITE_FULL',
    14: 'SQLITE_CANTOPEN',
    15: 'SQLITE_PROTOCOL',
    16: 'SQLITE_EMPTY',
    17: 'SQLITE_SCHEMA',
    18: 'SQLITE_TOOBIG',
    19: 'SQLITE_CONSTRAINT',
    20: 'SQLITE_MISMATCH',
    21: 'SQLITE_MISUSE',
    23: 'SQLITE_AUTH',
    24: 'SQLITE_FORMAT',
    25: 'SQLITE_RANGE',
    26: 'SQLITE_NOTADB',
    100: 'SQLITE_ROW',
    101: 'SQLITE_DONE',
  }

  // error class matching better-sqlite3
  class SqliteError extends Error {
    constructor(message, code) {
      super(message)
      this.name = 'SqliteError'
      if (typeof code === 'number') {
        // map numeric rc to string code, fall back to primary error code
        this.code =
          SQLITE_ERROR_NAMES[code & 0xff] || SQLITE_ERROR_NAMES[code] || 'SQLITE_ERROR'
      } else {
        this.code = code || 'SQLITE_ERROR'
      }
    }
  }

  // resolve binding parameters from variadic args (better-sqlite3 style)
  function resolveBindParams(args) {
    if (args.length === 0) return null
    if (args.length === 1) {
      const arg = args[0]
      if (arg === undefined || arg === null) return null
      if (Array.isArray(arg)) return arg
      if (
        typeof arg === 'object' &&
        !(arg instanceof Uint8Array) &&
        !(arg instanceof Buffer)
      )
        return arg
      return [arg]
    }
    return Array.from(args)
  }

  // custom function argument parsing
  function parseFunctionArgs(argc, argv) {
    const args = []
    for (let i = 0; i < argc; i++) {
      const ptr = getValue(argv + 4 * i, 'i32')
      const type = sqlite3.value_type(ptr)
      switch (type) {
        case SQLITE_INTEGER:
          args.push(toNumberOrNot(sqlite3.value_int64(ptr)))
          break
        case SQLITE_FLOAT:
          args.push(sqlite3.value_double(ptr))
          break
        case SQLITE_TEXT:
          args.push(sqlite3.value_text(ptr))
          break
        case SQLITE_BLOB: {
          const p = sqlite3.value_blob(ptr)
          args.push(
            p !== NULL
              ? Buffer.from(HEAPU8.slice(p, p + sqlite3.value_bytes(ptr)))
              : Buffer.alloc(0)
          )
          break
        }
        case SQLITE_NULL:
          args.push(null)
          break
      }
    }
    return args
  }

  function setFunctionResult(cx, result) {
    switch (typeof result) {
      case 'boolean':
        sqlite3.result_int(cx, result ? 1 : 0)
        break
      case 'number':
        if (Number.isSafeInteger(result)) {
          if (result >= INT32_MIN && result <= INT32_MAX) {
            sqlite3.result_int(cx, result)
          } else {
            sqlite3.result_int64(cx, BigInt(result))
          }
        } else {
          sqlite3.result_double(cx, result)
        }
        break
      case 'bigint':
        sqlite3.result_int64(cx, result)
        break
      case 'string': {
        const tp = stringToHeap(result)
        sqlite3.result_text(cx, tp, -1, SQLITE_TRANSIENT)
        _free(tp)
        break
      }
      case 'object':
        if (result === null) {
          sqlite3.result_null(cx)
        } else if (result instanceof Uint8Array || Buffer.isBuffer(result)) {
          const tp = arrayToHeap(result)
          sqlite3.result_blob(cx, tp, result.byteLength, SQLITE_TRANSIENT)
          _free(tp)
        } else {
          sqlite3.result_error(cx, stringToHeap('unsupported return type'), -1)
        }
        break
      default:
        sqlite3.result_null(cx)
        break
    }
  }

  // Database class (better-sqlite3 compatible)
  class Database {
    constructor(filename, options = {}) {
      if (typeof filename !== 'string' && !Buffer.isBuffer(filename)) {
        throw new TypeError('Expected first argument to be a string')
      }
      filename = String(filename)

      const readonly = !!options.readonly
      const fileMustExist = !!options.fileMustExist

      let flags
      if (readonly) {
        flags = 1 // SQLITE_OPEN_READONLY
      } else {
        flags = 2 // SQLITE_OPEN_READWRITE
        if (!fileMustExist) flags |= 4 // SQLITE_OPEN_CREATE
      }

      const rc = sqlite3.open_v2(filename, temp, flags, NULL)
      this._ptr = getValue(temp, 'i32')
      if (rc !== SQLITE_OK) {
        const msg =
          this._ptr !== NULL ? sqlite3.errmsg(this._ptr) : 'unable to open database'
        if (this._ptr !== NULL) sqlite3.close_v2(this._ptr)
        this._ptr = null
        throw new SqliteError(msg)
      }

      this._open = true
      this._readonly = readonly
      this._unsafe = false
      this._name = filename
      this._statements = new Set()
      this._functions = new Map()

      // set busy timeout to 5s by default
      this.exec('PRAGMA busy_timeout = 5000')
    }

    unsafeMode(enabled) {
      if (enabled === undefined) enabled = true
      this._unsafe = !!enabled
      return this
    }

    get open() {
      return this._open
    }
    get readonly() {
      return this._readonly
    }
    get name() {
      return this._name
    }
    get inTransaction() {
      return this._open && sqlite3.get_autocommit(this._ptr) === 0
    }

    prepare(source) {
      this._assertOpen()
      if (typeof source !== 'string') {
        throw new TypeError('Expected first argument to be a string')
      }
      const stmt = new Statement(this, source)
      this._statements.add(stmt)
      return stmt
    }

    exec(sql) {
      this._assertOpen()
      // reset active statements to prevent "SQL statements in progress" errors
      // native better-sqlite3 does this in C++; in wasm we do it unconditionally
      // since zero-cache may not always enable unsafeMode before exec
      for (const stmt of this._statements) {
        if (!stmt._finalized) sqlite3.reset(stmt._ptr)
      }
      const tp = stringToHeap(sql)
      try {
        const rc = sqlite3.exec(this._ptr, tp, NULL, NULL, NULL)
        if (rc !== SQLITE_OK) {
          throw new SqliteError(sqlite3.errmsg(this._ptr), rc)
        }
      } finally {
        _free(tp)
      }
      return this
    }

    transaction(fn) {
      if (typeof fn !== 'function') {
        throw new TypeError('Expected first argument to be a function')
      }
      const db = this

      const wrapTxn = (begin) => {
        const beginStmt = db.prepare(begin)
        const commitStmt = db.prepare('COMMIT')
        const rollbackStmt = db.prepare('ROLLBACK')

        const runner = function (...args) {
          let result
          // handle nested transactions with savepoints
          if (db.inTransaction) {
            return fn.apply(this, args)
          }
          beginStmt.run()
          try {
            result = fn.apply(this, args)
            commitStmt.run()
          } catch (err) {
            if (db.inTransaction) {
              rollbackStmt.run()
            }
            throw err
          }
          return result
        }
        return runner
      }

      const result = wrapTxn('BEGIN')
      result.deferred = wrapTxn('BEGIN DEFERRED')
      result.immediate = wrapTxn('BEGIN IMMEDIATE')
      result.exclusive = wrapTxn('BEGIN EXCLUSIVE')
      result.database = db
      return result
    }

    pragma(source, options) {
      if (typeof source !== 'string') {
        throw new TypeError('Expected first argument to be a string')
      }
      const simple = options && options.simple
      const sql = `PRAGMA ${source}`
      const stmt = this.prepare(sql)
      try {
        if (stmt.reader) {
          return simple ? stmt.pluck().get() : stmt.all()
        } else {
          return stmt.run()
        }
      } finally {
        // finalize the pragma statement so it doesn't block commits
        sqlite3.finalize(stmt._ptr)
        stmt._finalized = true
        this._statements.delete(stmt)
      }
    }

    function(name, fn, options = {}) {
      this._assertOpen()
      if (typeof name !== 'string')
        throw new TypeError('Expected first argument to be a string')
      if (typeof fn !== 'function')
        throw new TypeError('Expected second argument to be a function')

      const deterministic = !!options.deterministic
      const directOnly = !!options.directOnly
      const varargs = options.varargs || false

      if (this._functions.has(name)) {
        removeFunction(this._functions.get(name))
        this._functions.delete(name)
      }

      const wrappedFunc = (cx, argc, argv) => {
        const args = parseFunctionArgs(argc, argv)
        try {
          const result = fn(...args)
          setFunctionResult(cx, result)
        } catch (err) {
          const tp = stringToHeap(err.toString())
          sqlite3.result_error(cx, tp, -1)
          _free(tp)
        }
      }

      const funcPtr = addFunction(wrappedFunc, 'viii')
      this._functions.set(name, funcPtr)

      let eTextRep = SQLITE_UTF8
      if (deterministic) eTextRep |= SQLITE_DETERMINISTIC
      if (directOnly) eTextRep |= SQLITE_DIRECTONLY

      const rc = sqlite3.create_function_v2(
        this._ptr,
        name,
        varargs ? -1 : fn.length,
        eTextRep,
        NULL,
        funcPtr,
        NULL,
        NULL,
        NULL
      )
      if (rc !== SQLITE_OK) throw new SqliteError(sqlite3.errmsg(this._ptr))

      return this
    }

    aggregate(name, options) {
      this._assertOpen()
      if (typeof name !== 'string')
        throw new TypeError('Expected first argument to be a string')
      if (!options || typeof options !== 'object')
        throw new TypeError('Expected second argument to be an options object')

      const step = options.step
      const result = options.result || ((acc) => acc)
      const start = options.start
      const inverse = options.inverse
      const deterministic = !!options.deterministic
      const directOnly = !!options.directOnly
      const varargs = !!options.varargs

      if (typeof step !== 'function')
        throw new TypeError('Expected options.step to be a function')

      // use a simple approach: register as a scalar function that accumulates
      // better-sqlite3 aggregates are complex - for now use exec-based approach
      let accumulator
      const stepWrapper = (cx, argc, argv) => {
        const args = parseFunctionArgs(argc, argv)
        try {
          accumulator = step(accumulator, ...args)
        } catch (err) {
          const tp = stringToHeap(err.toString())
          sqlite3.result_error(cx, tp, -1)
          _free(tp)
        }
      }
      const finalWrapper = (cx) => {
        try {
          const val = result(accumulator)
          setFunctionResult(cx, val)
        } catch (err) {
          const tp = stringToHeap(err.toString())
          sqlite3.result_error(cx, tp, -1)
          _free(tp)
        }
        accumulator = typeof start === 'function' ? start() : start
      }

      accumulator = typeof start === 'function' ? start() : start

      if (this._functions.has(name + '_step')) {
        removeFunction(this._functions.get(name + '_step'))
        this._functions.delete(name + '_step')
      }
      if (this._functions.has(name + '_final')) {
        removeFunction(this._functions.get(name + '_final'))
        this._functions.delete(name + '_final')
      }

      const stepPtr = addFunction(stepWrapper, 'viii')
      const finalPtr = addFunction(finalWrapper, 'vi')
      this._functions.set(name + '_step', stepPtr)
      this._functions.set(name + '_final', finalPtr)

      let eTextRep = SQLITE_UTF8
      if (deterministic) eTextRep |= SQLITE_DETERMINISTIC
      if (directOnly) eTextRep |= SQLITE_DIRECTONLY

      const argCount = varargs ? -1 : step.length - 1
      const rc = sqlite3.create_function_v2(
        this._ptr,
        name,
        argCount,
        eTextRep,
        NULL,
        NULL,
        stepPtr,
        finalPtr,
        NULL
      )
      if (rc !== SQLITE_OK) throw new SqliteError(sqlite3.errmsg(this._ptr))

      return this
    }

    close() {
      if (!this._open) return

      // finalize all statements
      for (const stmt of this._statements) {
        if (!stmt._finalized) {
          sqlite3.finalize(stmt._ptr)
          stmt._finalized = true
        }
      }
      this._statements.clear()

      // remove custom functions
      for (const func of this._functions.values()) {
        removeFunction(func)
      }
      this._functions.clear()

      const rc = sqlite3.close_v2(this._ptr)
      if (rc !== SQLITE_OK) {
        throw new SqliteError(sqlite3.errmsg(this._ptr))
      }
      this._ptr = null
      this._open = false
    }

    _assertOpen() {
      if (!this._open) throw new SqliteError('The database connection is not open')
    }

    _handleError(rc) {
      if (rc !== SQLITE_OK) {
        throw new SqliteError(sqlite3.errmsg(this._ptr), rc)
      }
    }
  }

  // Statement class (better-sqlite3 compatible)
  class Statement {
    constructor(db, source) {
      this._db = db
      this._source = source
      this._finalized = false
      this._pluck = false
      this._expand = false
      this._raw = false
      this._bound = false

      const tp = stringToHeap(source)
      try {
        const rc = sqlite3.prepare_v2(db._ptr, tp, -1, temp, NULL)
        if (rc !== SQLITE_OK) {
          throw new SqliteError(sqlite3.errmsg(db._ptr), rc)
        }
      } finally {
        _free(tp)
      }
      this._ptr = getValue(temp, 'i32')
      if (this._ptr === NULL) {
        throw new SqliteError('Nothing to prepare')
      }

      this._columnCount = sqlite3.column_count(this._ptr)
      this._reader = this._columnCount > 0
      this._readonly = !!sqlite3.stmt_readonly(this._ptr)
    }

    get source() {
      return this._source
    }
    get reader() {
      return this._reader
    }
    get readonly() {
      return this._readonly
    }

    run(...args) {
      this._assertReady()
      const params = resolveBindParams(args)
      this._reset()
      if (params) this._bind(params)
      this._step()
      const result = {
        changes: sqlite3.changes(this._db._ptr),
        lastInsertRowid: toNumberOrNot(sqlite3.last_insert_rowid(this._db._ptr)),
      }
      // reset after step so SQLite doesn't consider this statement "in progress"
      // native better-sqlite3 does this in C++; without it, COMMIT fails with
      // "cannot commit transaction - SQL statements in progress"
      sqlite3.reset(this._ptr)
      return result
    }

    get(...args) {
      this._assertReady()
      const params = resolveBindParams(args)
      this._reset()
      if (params) this._bind(params)
      if (!this._step()) {
        sqlite3.reset(this._ptr)
        return undefined
      }
      const row = this._getRow()
      sqlite3.reset(this._ptr)
      return row
    }

    all(...args) {
      this._assertReady()
      const params = resolveBindParams(args)
      this._reset()
      if (params) this._bind(params)
      const rows = []
      while (this._step()) {
        rows.push(this._getRow())
        if (rows.length === 100000) {
          console.warn(
            `[bedrock-sqlite] all() returned 100k rows, query: ${this._source.slice(0, 200)}`
          )
        }
        if (rows.length >= 10000000) {
          sqlite3.reset(this._ptr)
          throw new SqliteError(
            `all() exceeded 10M row safety limit, likely infinite loop. query: ${this._source.slice(0, 200)}`
          )
        }
      }
      sqlite3.reset(this._ptr)
      return rows
    }

    *iterate(...args) {
      this._assertReady()
      const params = resolveBindParams(args)
      this._reset()
      if (params) this._bind(params)
      while (this._step()) {
        yield this._getRow()
      }
    }

    pluck(toggle = true) {
      this._pluck = toggle
      this._raw = false
      this._expand = false
      return this
    }

    expand(toggle = true) {
      this._expand = toggle
      this._pluck = false
      this._raw = false
      return this
    }

    raw(toggle = true) {
      this._raw = toggle
      this._pluck = false
      this._expand = false
      return this
    }

    columns() {
      this._assertReady()
      const cols = []
      for (let i = 0; i < this._columnCount; i++) {
        cols.push({
          name: sqlite3.column_name(this._ptr, i),
          column: sqlite3.column_origin_name(this._ptr, i),
          table: sqlite3.column_table_name(this._ptr, i),
          database: sqlite3.column_database_name(this._ptr, i),
          type: null,
        })
      }
      return cols
    }

    // stub: zero-cache calls these for scan statistics
    scanStatusV2() {
      return []
    }

    scanStatusReset() {}

    // stub: safe integers (not needed in wasm but api compat)
    safeIntegers() {
      return this
    }

    bind(...args) {
      this._assertReady()
      const params = resolveBindParams(args)
      this._reset()
      if (params) this._bind(params)
      this._bound = true
      return this
    }

    _reset() {
      sqlite3.clear_bindings(this._ptr)
      sqlite3.reset(this._ptr)
    }

    _step() {
      const rc = sqlite3.step(this._ptr)
      if (rc === SQLITE_ROW) return true
      if (rc === SQLITE_DONE) return false
      sqlite3.reset(this._ptr)
      throw new SqliteError(sqlite3.errmsg(this._db._ptr), rc)
    }

    _getRow() {
      if (this._pluck) {
        return this._getColumnValue(0)
      }
      if (this._raw) {
        const row = []
        for (let i = 0; i < this._columnCount; i++) {
          row.push(this._getColumnValue(i))
        }
        return row
      }
      if (this._expand) {
        const row = {}
        for (let i = 0; i < this._columnCount; i++) {
          let table = sqlite3.column_table_name(this._ptr, i)
          table = table === '' || table === null ? '$' : table
          const name = sqlite3.column_name(this._ptr, i)
          if (!row[table]) row[table] = {}
          row[table][name] = this._getColumnValue(i)
        }
        return row
      }
      // default: plain object keyed by column name
      const row = {}
      for (let i = 0; i < this._columnCount; i++) {
        row[sqlite3.column_name(this._ptr, i)] = this._getColumnValue(i)
      }
      return row
    }

    _getColumnValue(i) {
      const type = sqlite3.column_type(this._ptr, i)
      switch (type) {
        case SQLITE_INTEGER:
          return toNumberOrNot(sqlite3.column_int64(this._ptr, i))
        case SQLITE_FLOAT:
          return sqlite3.column_double(this._ptr, i)
        case SQLITE_TEXT:
          return sqlite3.column_text(this._ptr, i)
        case SQLITE_BLOB: {
          const p = sqlite3.column_blob(this._ptr, i)
          if (p !== NULL) {
            const nbytes = sqlite3.column_bytes(this._ptr, i)
            if (nbytes > 104857600) {
              throw new SqliteError(
                `blob column ${i} has unreasonable size: ${nbytes} bytes`
              )
            }
            return Buffer.from(HEAPU8.slice(p, p + nbytes))
          }
          return Buffer.alloc(0)
        }
        case SQLITE_NULL:
          return null
        default:
          return null
      }
    }

    _bind(params) {
      if (Array.isArray(params)) {
        for (let i = 0; i < params.length; i++) {
          this._bindValue(params[i], i + 1)
        }
      } else if (typeof params === 'object') {
        // named binding - better-sqlite3 style (keys without prefix)
        const count = sqlite3.bind_parameter_count(this._ptr)
        for (let i = 1; i <= count; i++) {
          const pname = sqlite3.bind_parameter_name(this._ptr, i)
          if (!pname) continue
          // try key as-is (with prefix), then without prefix
          const stripped = pname.slice(1) // remove @, $, or :
          const value =
            pname in params
              ? params[pname]
              : stripped in params
                ? params[stripped]
                : undefined
          if (value !== undefined) {
            this._bindValue(value, i)
          }
        }
      }
    }

    _bindValue(value, position) {
      let rc
      switch (typeof value) {
        case 'string': {
          const tp = stringToHeap(value)
          rc = sqlite3.bind_text(this._ptr, position, tp, -1, SQLITE_TRANSIENT)
          _free(tp)
          break
        }
        case 'number':
          if (Number.isSafeInteger(value)) {
            if (value >= INT32_MIN && value <= INT32_MAX) {
              rc = sqlite3.bind_int(this._ptr, position, value)
            } else {
              rc = sqlite3.bind_int64(this._ptr, position, BigInt(value))
            }
          } else {
            rc = sqlite3.bind_double(this._ptr, position, value)
          }
          break
        case 'bigint':
          rc = sqlite3.bind_int64(this._ptr, position, value)
          break
        case 'boolean':
          rc = sqlite3.bind_int(this._ptr, position, value ? 1 : 0)
          break
        case 'object':
          if (value === null) {
            rc = sqlite3.bind_null(this._ptr, position)
          } else if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
            const tp = arrayToHeap(value)
            rc = sqlite3.bind_blob(
              this._ptr,
              position,
              tp,
              value.byteLength,
              SQLITE_TRANSIENT
            )
            _free(tp)
          } else {
            throw new SqliteError(`Unsupported binding type at position ${position}`)
          }
          break
        default:
          if (value === undefined) {
            rc = sqlite3.bind_null(this._ptr, position)
          } else {
            throw new SqliteError(`Unsupported binding type: ${typeof value}`)
          }
      }
      if (rc !== SQLITE_OK) {
        throw new SqliteError(sqlite3.errmsg(this._db._ptr))
      }
    }

    _assertReady() {
      if (this._finalized) throw new SqliteError('This statement has been finalized')
      if (!this._db._open) throw new SqliteError('The database connection is not open')
    }
  }

  Module.Database = Database
  Module.SqliteError = SqliteError
}
