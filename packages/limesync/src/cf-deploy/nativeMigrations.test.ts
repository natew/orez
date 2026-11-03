import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import { readNativeSqlMigrationStatements } from './nativeMigrations.js'

const dirs: string[] = []

afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true })
})

function migrationsDirWith(sql: string) {
  const dir = mkdtempSync(join(tmpdir(), 'orez-native-migrations-'))
  dirs.push(dir)
  mkdirSync(join(dir, '0001_rebuild'))
  writeFileSync(join(dir, '0001_rebuild', 'migration.sql'), sql)
  return dir
}

const parseStatement = (sql: string) => ({ sql })

describe('readNativeSqlMigrationStatements', () => {
  it('carries block membership from the create through the rename', () => {
    const statements = readNativeSqlMigrationStatements(
      migrationsDirWith(
        [
          'PRAGMA foreign_keys=OFF;',
          'CREATE TABLE `__new_widget` (`id` text PRIMARY KEY, `label` text);',
          'INSERT INTO `__new_widget`(`id`) SELECT `id` FROM `widget`;',
          'DROP TABLE `widget`;',
          'ALTER TABLE `__new_widget` RENAME TO `widget`;',
          'PRAGMA foreign_keys=ON;',
          'CREATE INDEX `widget_label_idx` ON `widget` (`label`);',
        ].join('--> statement-breakpoint\n')
      ),
      parseStatement
    )

    expect(statements.map((statement) => statement.rebuildTarget ?? null)).toEqual([
      null,
      'widget',
      'widget',
      'widget',
      'widget',
      null,
      null,
    ])
    expect(statements[1]?.rebuildColumns).toEqual(['id', 'label'])
  })
})
