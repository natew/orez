import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import { applyZqliteHandleRegistry } from './zero-sqlite-handle-patch.js'

let tmpDirs: string[] = []

afterEach(() => {
  for (const dir of tmpDirs) {
    rmSync(dir, { recursive: true, force: true })
  }
  tmpDirs = []
})

// zero's bundler names the default import of @rocicorp/zero-sqlite3 either
// `SQLite3Database` (<= 1.8.0-canary.1) or `Sqlite3Database` (>= 1.8.0-canary.2)
// from identical source. the patch must survive both.
const bundledDb = (ctor: string) => `var Database = class {
	#db;
	constructor(lc, path, options, slowQueryThreshold = 100) {
		try {
			this.#db = new ${ctor}(path, options);
		} catch (cause) {
			throw new DatabaseInitError(String(cause), { cause });
		}
	}
	close() {
		this.#db.close();
	}
};`

describe('applyZqliteHandleRegistry', () => {
  it.each(['SQLite3Database', 'Sqlite3Database'])(
    'registers add/delete hooks regardless of the bundler binding name (%s)',
    (ctor) => {
      const file = writeDb(bundledDb(ctor))

      applyZqliteHandleRegistry(file)

      const patched = readFileSync(file, 'utf-8')
      expect(patched).toContain(
        `this.#db = new ${ctor}(path, options);\n\t\t\tglobalThis.__orez_open_sqlite_dbs?.add(this);`
      )
      expect(patched).toContain('globalThis.__orez_open_sqlite_dbs?.delete(this);')
    }
  )

  it('is idempotent', () => {
    const file = writeDb(bundledDb('Sqlite3Database'))

    applyZqliteHandleRegistry(file)
    applyZqliteHandleRegistry(file)

    const patched = readFileSync(file, 'utf-8')
    expect(patched.match(/__orez_open_sqlite_dbs\?\.add/g)).toHaveLength(1)
    expect(patched.match(/__orez_open_sqlite_dbs\?\.delete/g)).toHaveLength(1)
  })

  it('fails loudly when zqlite changes the Database wrapper shape', () => {
    const file = writeDb('var Database = class { constructor() {} };')

    expect(() => applyZqliteHandleRegistry(file)).toThrow(
      'could not patch zqlite Database handle registry'
    )
  })
})

function writeDb(content: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'orez-sqlite-handle-patch-'))
  tmpDirs.push(dir)
  const file = resolve(dir, 'db.js')
  writeFileSync(file, content)
  return file
}
