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
  // the reconcile decides whether a rebuild block landed by comparing this
  // column list against the live table, so a column the parser invents makes a
  // perfectly current table read as stale forever — and every migration run
  // then drops and rebuilds it.
  it('does not invent a column from a comma inside a DEFAULT literal', () => {
    const create = [
      'CREATE TABLE `repoBuildConfig` (',
      '  `id` text PRIMARY KEY,',
      `  \`platforms\` text DEFAULT '{"ios":true,"android":false}',`,
      '  `label` text',
      ');',
    ].join('\n')
    expect(create).toContain(`'{"ios":true,"android":false}'`)

    const expected = ['id', 'platforms', 'label']

    const statements = readNativeSqlMigrationStatements(
      migrationsDirWith(
        [
          create.replace(
            'CREATE TABLE `repoBuildConfig`',
            'CREATE TABLE `__new_repoBuildConfig`'
          ),
          'INSERT INTO `__new_repoBuildConfig` SELECT * FROM `repoBuildConfig`;',
          'DROP TABLE `repoBuildConfig`;',
          'ALTER TABLE `__new_repoBuildConfig` RENAME TO `repoBuildConfig`;',
        ].join('--> statement-breakpoint\n')
      ),
      parseStatement
    )

    const block = statements.find((statement) => statement.rebuildColumns)
    expect(block?.rebuildTarget).toBe('repoBuildConfig')
    expect(block?.rebuildColumns).toEqual(expected)
  })

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
