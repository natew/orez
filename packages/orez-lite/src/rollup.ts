import { createBuilder } from '@rocicorp/zero'

import type { Schema } from '@rocicorp/zero'

export type RollupMode = 'existing' | 'materialized'

export type CountRollup = {
  readonly kind: 'count'
}

export type SumRollup<SourceColumn extends string = string> = {
  readonly kind: 'sum'
  readonly source: SourceColumn
}

type TableName<S extends Schema> = Extract<keyof S['tables'], string>
type ColumnName<S extends Schema, TTable extends TableName<S>> = Extract<
  keyof S['tables'][TTable]['columns'],
  string
>
type NumericColumnName<S extends Schema, TTable extends TableName<S>> = Extract<
  {
    [TColumn in ColumnName<S, TTable>]: S['tables'][TTable]['columns'][TColumn] extends {
      readonly type: 'number'
    }
      ? TColumn
      : never
  }[ColumnName<S, TTable>],
  string
>

export type RollupDefinition<S extends Schema> = {
  [TSource in TableName<S>]: {
    [TTarget in TableName<S>]: {
      readonly source: TSource
      readonly target: TTarget
      readonly mode: RollupMode
      readonly groupBy: Readonly<
        Partial<Record<ColumnName<S, TSource>, ColumnName<S, TTarget>>>
      >
      readonly aggregates: Readonly<
        Partial<
          Record<
            NumericColumnName<S, TTarget>,
            CountRollup | SumRollup<NumericColumnName<S, TSource>>
          >
        >
      >
    }
  }[TableName<S>]
}[TableName<S>]

type RollupColumn = {
  readonly logical: string
  readonly physical: string
}

type CompiledGroup = {
  readonly source: RollupColumn
  readonly target: RollupColumn
}

type CompiledAggregate =
  | {
      readonly kind: 'count'
      readonly target: RollupColumn
    }
  | {
      readonly kind: 'sum'
      readonly source: RollupColumn
      readonly target: RollupColumn
    }

type CompiledRollup = {
  readonly name: string
  readonly source: {
    readonly logical: string
    readonly physical: string
    readonly primaryKey: readonly string[]
  }
  readonly target: {
    readonly logical: string
    readonly physical: string
    readonly primaryKey: readonly string[]
  }
  readonly mode: RollupMode
  readonly groups: readonly CompiledGroup[]
  readonly aggregates: readonly CompiledAggregate[]
}

type RollupRuntime<S extends Schema> = {
  readonly schema: S
  readonly definitions: readonly CompiledRollup[]
  readonly queryBuilder: object
  readonly bySource: ReadonlyMap<string, readonly CompiledRollup[]>
}

const ROLLUP_RUNTIME: unique symbol = Symbol.for('orez-lite:rollup-runtime')

export type RollupSet<S extends Schema = Schema> = {
  readonly [ROLLUP_RUNTIME]: RollupRuntime<S>
}

export function count(): CountRollup {
  return { kind: 'count' }
}

export function sum<const SourceColumn extends string>(
  source: SourceColumn
): SumRollup<SourceColumn> {
  return { kind: 'sum', source }
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`
}

function tablePhysicalName(table: {
  readonly name: string
  readonly serverName?: string | undefined
}): string {
  return table.serverName ?? table.name.replace(/^public\./, '')
}

function columnPhysicalName(columnName: string, column: unknown): string {
  if (
    column &&
    typeof column === 'object' &&
    'serverName' in column &&
    typeof column.serverName === 'string'
  ) {
    return column.serverName
  }
  return columnName
}

function ensureOwnColumn(
  rollupName: string,
  tableName: string,
  columns: Readonly<Record<string, unknown>>,
  columnName: string
): unknown {
  if (!Object.hasOwn(columns, columnName)) {
    throw new TypeError(
      `rollup ${rollupName} references unknown column ${tableName}.${columnName}`
    )
  }
  return columns[columnName]
}

export function defineRollups<
  S extends Schema,
  const Definitions extends Readonly<Record<string, RollupDefinition<S>>>,
>(schema: S, definitions: Definitions): RollupSet<S> {
  const compiled: CompiledRollup[] = []
  const triggerNames = new Set<string>()

  for (const [name, definition] of Object.entries(definitions)) {
    if (!/^[A-Za-z][A-Za-z0-9_]{0,62}$/.test(name)) {
      throw new TypeError(
        `rollup name must be an identifier of at most 63 characters: ${JSON.stringify(name)}`
      )
    }
    if (triggerNames.has(name)) {
      throw new TypeError(`duplicate rollup name: ${name}`)
    }
    triggerNames.add(name)

    const source = schema.tables[definition.source]
    const target = schema.tables[definition.target]
    if (!source) {
      throw new TypeError(`rollup ${name} references unknown table ${definition.source}`)
    }
    if (!target) {
      throw new TypeError(`rollup ${name} references unknown table ${definition.target}`)
    }
    if (definition.source === definition.target) {
      throw new TypeError(`rollup ${name} must use different source and target tables`)
    }

    const groups = Object.entries(definition.groupBy).map(
      ([sourceColumnName, targetColumnName]) => {
        if (typeof targetColumnName !== 'string') {
          throw new TypeError(`rollup ${name} has an invalid group mapping`)
        }
        const sourceColumn = ensureOwnColumn(
          name,
          definition.source,
          source.columns,
          sourceColumnName
        )
        const targetColumn = ensureOwnColumn(
          name,
          definition.target,
          target.columns,
          targetColumnName
        )
        return {
          source: {
            logical: sourceColumnName,
            physical: columnPhysicalName(sourceColumnName, sourceColumn),
          },
          target: {
            logical: targetColumnName,
            physical: columnPhysicalName(targetColumnName, targetColumn),
          },
        }
      }
    )
    if (groups.length === 0) {
      throw new TypeError(`rollup ${name} must group by at least one column`)
    }
    if (new Set(groups.map((group) => group.target.logical)).size !== groups.length) {
      throw new TypeError(`rollup ${name} maps more than one source column to one key`)
    }

    const targetPrimaryKey = [...target.primaryKey]
    const groupedTargetColumns = [...groups].map((group) => group.target.logical).sort()
    if (
      targetPrimaryKey.length !== groupedTargetColumns.length ||
      [...targetPrimaryKey]
        .sort()
        .some((columnName, index) => columnName !== groupedTargetColumns[index])
    ) {
      throw new TypeError(
        `rollup ${name} groupBy must map every primary key column of ${definition.target}`
      )
    }

    const aggregates: CompiledAggregate[] = Object.entries(definition.aggregates).map(
      ([targetColumnName, aggregate]) => {
        if (!aggregate || typeof aggregate !== 'object') {
          throw new TypeError(`rollup ${name} has an invalid aggregate`)
        }
        const targetColumn = ensureOwnColumn(
          name,
          definition.target,
          target.columns,
          targetColumnName
        )
        if (
          !targetColumn ||
          typeof targetColumn !== 'object' ||
          !('type' in targetColumn) ||
          targetColumn.type !== 'number'
        ) {
          throw new TypeError(
            `rollup ${name} target column ${definition.target}.${targetColumnName} must be numeric`
          )
        }
        const compiledTarget = {
          logical: targetColumnName,
          physical: columnPhysicalName(targetColumnName, targetColumn),
        }
        const kind = Reflect.get(aggregate, 'kind')
        if (kind === 'count') {
          return { kind: 'count', target: compiledTarget }
        }
        const aggregateSource = Reflect.get(aggregate, 'source')
        if (kind !== 'sum' || typeof aggregateSource !== 'string') {
          throw new TypeError(`rollup ${name} has an invalid aggregate`)
        }
        const sourceColumn = ensureOwnColumn(
          name,
          definition.source,
          source.columns,
          aggregateSource
        )
        if (
          !sourceColumn ||
          typeof sourceColumn !== 'object' ||
          !('type' in sourceColumn) ||
          sourceColumn.type !== 'number'
        ) {
          throw new TypeError(
            `rollup ${name} source column ${definition.source}.${aggregateSource} must be numeric`
          )
        }
        return {
          kind: 'sum',
          source: {
            logical: aggregateSource,
            physical: columnPhysicalName(aggregateSource, sourceColumn),
          },
          target: compiledTarget,
        }
      }
    )
    if (aggregates.length === 0) {
      throw new TypeError(`rollup ${name} must declare at least one aggregate`)
    }
    if (aggregates.filter((aggregate) => aggregate.kind === 'count').length > 1) {
      throw new TypeError(`rollup ${name} may declare only one count aggregate`)
    }
    if (
      groups.some((group) =>
        aggregates.some((aggregate) => aggregate.target.logical === group.target.logical)
      )
    ) {
      throw new TypeError(`rollup ${name} cannot aggregate into a primary key column`)
    }

    if (definition.mode === 'materialized') {
      if (!aggregates.some((aggregate) => aggregate.kind === 'count')) {
        throw new TypeError(
          `materialized rollup ${name} needs a count aggregate to remove empty groups`
        )
      }
      const ownedTargetColumns = new Set([
        ...groups.map((group) => group.target.logical),
        ...aggregates.map((aggregate) => aggregate.target.logical),
      ])
      const extraTargetColumn = Object.keys(target.columns).find(
        (columnName) => !ownedTargetColumns.has(columnName)
      )
      if (extraTargetColumn) {
        throw new TypeError(
          `materialized rollup ${name} target has non-rollup column ${definition.target}.${extraTargetColumn}`
        )
      }
    } else if (definition.mode !== 'existing') {
      throw new TypeError(`rollup ${name} has an invalid target mode`)
    }

    compiled.push({
      name,
      source: {
        logical: definition.source,
        physical: tablePhysicalName(source),
        primaryKey: [...source.primaryKey],
      },
      target: {
        logical: definition.target,
        physical: tablePhysicalName(target),
        primaryKey: targetPrimaryKey,
      },
      mode: definition.mode,
      groups,
      aggregates,
    })
  }

  const sourceTables = new Set(compiled.map((rollup) => rollup.source.logical))
  for (const rollup of compiled) {
    if (sourceTables.has(rollup.target.logical)) {
      throw new TypeError(
        `rollup ${rollup.name} target ${rollup.target.logical} cannot feed another rollup`
      )
    }
    for (const candidate of compiled) {
      if (candidate === rollup || candidate.target.logical !== rollup.target.logical) {
        continue
      }
      if (rollup.mode === 'materialized' || candidate.mode === 'materialized') {
        throw new TypeError(
          `materialized rollup target ${rollup.target.logical} may have only one owner`
        )
      }
      const duplicate = rollup.aggregates.find((aggregate) =>
        candidate.aggregates.some(
          (other) => other.target.logical === aggregate.target.logical
        )
      )
      if (duplicate) {
        throw new TypeError(
          `rollup target column ${rollup.target.logical}.${duplicate.target.logical} has more than one owner`
        )
      }
    }
  }

  const bySource = new Map<string, CompiledRollup[]>()
  for (const rollup of compiled) {
    const sourceRollups = bySource.get(rollup.source.logical)
    if (sourceRollups) sourceRollups.push(rollup)
    else bySource.set(rollup.source.logical, [rollup])
  }

  return {
    [ROLLUP_RUNTIME]: {
      schema,
      definitions: compiled,
      queryBuilder: createBuilder(schema),
      bySource,
    },
  }
}

function sourceReference(column: RollupColumn, row: 'NEW' | 'OLD'): string {
  return `${row}.${quoteIdentifier(column.physical)}`
}

function targetWhere(rollup: CompiledRollup, row: 'NEW' | 'OLD'): string {
  return rollup.groups
    .map(
      (group) =>
        `${quoteIdentifier(group.target.physical)} IS ${sourceReference(group.source, row)}`
    )
    .join(' AND ')
}

function sameGroup(rollup: CompiledRollup): string {
  return rollup.groups
    .map(
      (group) =>
        `${sourceReference(group.source, 'OLD')} IS ${sourceReference(group.source, 'NEW')}`
    )
    .join(' AND ')
}

function aggregateChanged(rollup: CompiledRollup): string {
  const sums = rollup.aggregates.filter(
    (aggregate): aggregate is Extract<CompiledAggregate, { kind: 'sum' }> =>
      aggregate.kind === 'sum'
  )
  return sums.length
    ? sums
        .map(
          (aggregate) =>
            `${sourceReference(aggregate.source, 'OLD')} IS NOT ${sourceReference(aggregate.source, 'NEW')}`
        )
        .join(' OR ')
    : '0'
}

function contribution(aggregate: CompiledAggregate, row: 'NEW' | 'OLD'): string {
  return aggregate.kind === 'count'
    ? '1'
    : `COALESCE(${sourceReference(aggregate.source, row)}, 0)`
}

function assignments(
  rollup: CompiledRollup,
  operation: 'add' | 'remove' | 'delta'
): string {
  return rollup.aggregates
    .flatMap((aggregate) => {
      const target = quoteIdentifier(aggregate.target.physical)
      if (operation === 'delta') {
        return aggregate.kind === 'sum'
          ? [
              `${target} = ${target} + ${contribution(aggregate, 'NEW')} - ${contribution(aggregate, 'OLD')}`,
            ]
          : []
      }
      const operator = operation === 'add' ? '+' : '-'
      const row = operation === 'add' ? 'NEW' : 'OLD'
      return [`${target} = ${target} ${operator} ${contribution(aggregate, row)}`]
    })
    .join(', ')
}

function insertColumns(rollup: CompiledRollup): string {
  return [
    ...rollup.groups.map((group) => group.target),
    ...rollup.aggregates.map((a) => a.target),
  ]
    .map((column) => quoteIdentifier(column.physical))
    .join(', ')
}

function insertValues(rollup: CompiledRollup, row: 'NEW' | 'OLD'): string {
  return [
    ...rollup.groups.map((group) => sourceReference(group.source, row)),
    ...rollup.aggregates.map((aggregate) => contribution(aggregate, row)),
  ].join(', ')
}

function upsertAssignments(rollup: CompiledRollup): string {
  return rollup.aggregates
    .map((aggregate) => {
      const target = quoteIdentifier(aggregate.target.physical)
      return `${target} = ${target} + excluded.${target}`
    })
    .join(', ')
}

function countTarget(rollup: CompiledRollup): RollupColumn {
  const aggregate = rollup.aggregates.find(
    (candidate): candidate is Extract<CompiledAggregate, { kind: 'count' }> =>
      candidate.kind === 'count'
  )
  if (!aggregate) {
    throw new TypeError(`materialized rollup ${rollup.name} has no count aggregate`)
  }
  return aggregate.target
}

function watchedSourceColumns(rollup: CompiledRollup): string {
  return [
    ...new Set([
      ...rollup.groups.map((group) => group.source.physical),
      ...rollup.aggregates.flatMap((aggregate) =>
        aggregate.kind === 'sum' ? [aggregate.source.physical] : []
      ),
    ]),
  ]
    .map(quoteIdentifier)
    .join(', ')
}

function backfillStatements(rollup: CompiledRollup): string[] {
  const sourceTable = quoteIdentifier(rollup.source.physical)
  const targetTable = quoteIdentifier(rollup.target.physical)
  if (rollup.mode === 'materialized') {
    const select = [
      ...rollup.groups.map((group) => quoteIdentifier(group.source.physical)),
      ...rollup.aggregates.map((aggregate) =>
        aggregate.kind === 'count'
          ? 'COUNT(*)'
          : `COALESCE(SUM(${quoteIdentifier(aggregate.source.physical)}), 0)`
      ),
    ].join(', ')
    const groupBy = rollup.groups
      .map((group) => quoteIdentifier(group.source.physical))
      .join(', ')
    return [
      `DELETE FROM ${targetTable}`,
      `INSERT INTO ${targetTable} (${insertColumns(rollup)}) SELECT ${select} FROM ${sourceTable} GROUP BY ${groupBy}`,
    ]
  }

  const correlation = rollup.groups
    .map(
      (group) =>
        `${sourceTable}.${quoteIdentifier(group.source.physical)} IS ${targetTable}.${quoteIdentifier(group.target.physical)}`
    )
    .join(' AND ')
  const values = rollup.aggregates
    .map((aggregate) => {
      const target = quoteIdentifier(aggregate.target.physical)
      const value =
        aggregate.kind === 'count'
          ? `COUNT(*)`
          : `COALESCE(SUM(${sourceTable}.${quoteIdentifier(aggregate.source.physical)}), 0)`
      return `${target} = (SELECT ${value} FROM ${sourceTable} WHERE ${correlation})`
    })
    .join(', ')
  return [`UPDATE ${targetTable} SET ${values}`]
}

function triggerStatements(rollup: CompiledRollup): string[] {
  const sourceTable = quoteIdentifier(rollup.source.physical)
  const targetTable = quoteIdentifier(rollup.target.physical)
  const insertTrigger = quoteIdentifier(`_orez_rollup_${rollup.name}_insert`)
  const updateTrigger = quoteIdentifier(`_orez_rollup_${rollup.name}_update`)
  const deleteTrigger = quoteIdentifier(`_orez_rollup_${rollup.name}_delete`)
  const keyConflict = rollup.groups
    .map((group) => quoteIdentifier(group.target.physical))
    .join(', ')
  const changedGroup = `NOT (${sameGroup(rollup)})`
  const sumsChanged = aggregateChanged(rollup)

  if (rollup.mode === 'existing') {
    const updateColumns = watchedSourceColumns(rollup)
    const updateBody = [
      `UPDATE ${targetTable} SET ${assignments(rollup, 'remove')} WHERE ${targetWhere(rollup, 'OLD')} AND ${changedGroup};`,
      `UPDATE ${targetTable} SET ${assignments(rollup, 'add')} WHERE ${targetWhere(rollup, 'NEW')} AND ${changedGroup};`,
      ...(assignments(rollup, 'delta')
        ? [
            `UPDATE ${targetTable} SET ${assignments(rollup, 'delta')} WHERE ${targetWhere(rollup, 'NEW')} AND (${sameGroup(rollup)}) AND (${sumsChanged});`,
          ]
        : []),
    ].join(' ')
    return [
      `CREATE TRIGGER ${insertTrigger} AFTER INSERT ON ${sourceTable} BEGIN UPDATE ${targetTable} SET ${assignments(rollup, 'add')} WHERE ${targetWhere(rollup, 'NEW')}; END`,
      `CREATE TRIGGER ${updateTrigger} AFTER UPDATE OF ${updateColumns} ON ${sourceTable} BEGIN ${updateBody} END`,
      `CREATE TRIGGER ${deleteTrigger} AFTER DELETE ON ${sourceTable} BEGIN UPDATE ${targetTable} SET ${assignments(rollup, 'remove')} WHERE ${targetWhere(rollup, 'OLD')}; END`,
    ]
  }

  const countColumn = quoteIdentifier(countTarget(rollup).physical)
  const insert = `INSERT INTO ${targetTable} (${insertColumns(rollup)}) VALUES (${insertValues(rollup, 'NEW')}) ON CONFLICT (${keyConflict}) DO UPDATE SET ${upsertAssignments(rollup)};`
  const updateColumns = watchedSourceColumns(rollup)
  const updateBody = [
    `UPDATE ${targetTable} SET ${assignments(rollup, 'remove')} WHERE ${targetWhere(rollup, 'OLD')} AND ${changedGroup};`,
    `DELETE FROM ${targetTable} WHERE ${targetWhere(rollup, 'OLD')} AND ${countColumn} <= 0 AND ${changedGroup};`,
    `INSERT INTO ${targetTable} (${insertColumns(rollup)}) SELECT ${insertValues(rollup, 'NEW')} WHERE ${changedGroup} ON CONFLICT (${keyConflict}) DO UPDATE SET ${upsertAssignments(rollup)};`,
    ...(assignments(rollup, 'delta')
      ? [
          `UPDATE ${targetTable} SET ${assignments(rollup, 'delta')} WHERE ${targetWhere(rollup, 'NEW')} AND (${sameGroup(rollup)}) AND (${sumsChanged});`,
        ]
      : []),
  ].join(' ')
  return [
    `CREATE TRIGGER ${insertTrigger} AFTER INSERT ON ${sourceTable} BEGIN ${insert} END`,
    `CREATE TRIGGER ${updateTrigger} AFTER UPDATE OF ${updateColumns} ON ${sourceTable} BEGIN ${updateBody} END`,
    `CREATE TRIGGER ${deleteTrigger} AFTER DELETE ON ${sourceTable} BEGIN UPDATE ${targetTable} SET ${assignments(rollup, 'remove')} WHERE ${targetWhere(rollup, 'OLD')}; DELETE FROM ${targetTable} WHERE ${targetWhere(rollup, 'OLD')} AND ${countColumn} <= 0; END`,
  ]
}

export function rollupMigrationStatements(rollups: RollupSet): readonly string[] {
  return rollups[ROLLUP_RUNTIME].definitions.flatMap((rollup) => {
    const triggerNames = ['insert', 'update', 'delete'].map((operation) =>
      quoteIdentifier(`_orez_rollup_${rollup.name}_${operation}`)
    )
    return [
      ...triggerNames.map((name) => `DROP TRIGGER IF EXISTS ${name}`),
      ...backfillStatements(rollup),
      ...triggerStatements(rollup),
    ]
  })
}

export function rollupMigrationSQL(rollups: RollupSet): string {
  return rollupMigrationStatements(rollups).join('\n--> statement-breakpoint\n')
}

type RollupRow = Readonly<Record<string, unknown>>

function rollupRow(value: unknown, context: string): RollupRow {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${context} must be a row object`)
  }
  return Object.fromEntries(Object.entries(value))
}

function numericValue(value: unknown, context: string, nullable: boolean): number {
  if (nullable && value === null) return 0
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new TypeError(`${context} must be a finite number`)
  }
  return value
}

async function readRollupRow(
  tx: object,
  builder: object,
  tableName: string,
  primaryKey: readonly string[],
  key: RollupRow
): Promise<RollupRow | null> {
  let query: unknown = Reflect.get(builder, tableName)
  if (!query || typeof query !== 'object') {
    throw new TypeError(`rollup query table is unavailable: ${tableName}`)
  }
  for (const columnName of primaryKey) {
    const value = key[columnName]
    if (value === undefined || value === null) {
      throw new TypeError(`rollup key ${tableName}.${columnName} must be non-null`)
    }
    const where = Reflect.get(query, 'where')
    if (typeof where !== 'function') {
      throw new TypeError(`rollup query does not support where(): ${tableName}`)
    }
    query = Reflect.apply(where, query, [columnName, '=', value])
    if (!query || typeof query !== 'object') {
      throw new TypeError(`rollup query returned an invalid value: ${tableName}`)
    }
  }
  const one = Reflect.get(query, 'one')
  const run = Reflect.get(tx, 'run')
  if (typeof one !== 'function' || typeof run !== 'function') {
    throw new TypeError(`rollup transaction cannot read ${tableName}`)
  }
  const singular = Reflect.apply(one, query, [])
  const result = await Reflect.apply(run, tx, [singular])
  return result === undefined || result === null
    ? null
    : rollupRow(result, `rollup query result for ${tableName}`)
}

function groupKey(rollup: CompiledRollup, row: RollupRow): RollupRow {
  const key: Record<string, unknown> = {}
  for (const group of rollup.groups) {
    const value = row[group.source.logical]
    if (value === undefined || value === null) {
      throw new TypeError(
        `rollup ${rollup.name} group ${rollup.source.logical}.${group.source.logical} must be non-null`
      )
    }
    key[group.target.logical] = value
  }
  return key
}

function sameKey(rollup: CompiledRollup, left: RollupRow, right: RollupRow): boolean {
  return rollup.groups.every((group) =>
    Object.is(left[group.target.logical], right[group.target.logical])
  )
}

function aggregateContribution(
  rollup: CompiledRollup,
  row: RollupRow
): Readonly<Record<string, number>> {
  const contribution: Record<string, number> = {}
  for (const aggregate of rollup.aggregates) {
    contribution[aggregate.target.logical] =
      aggregate.kind === 'count'
        ? 1
        : numericValue(
            row[aggregate.source.logical],
            `rollup ${rollup.name} source ${rollup.source.logical}.${aggregate.source.logical}`,
            true
          )
  }
  return contribution
}

async function mutateRollupTarget(
  tx: object,
  rollup: CompiledRollup,
  operation: 'delete' | 'insert' | 'update',
  row: RollupRow
): Promise<void> {
  const mutate = Reflect.get(tx, 'mutate')
  if (!mutate || typeof mutate !== 'object') {
    throw new TypeError(`rollup transaction has no mutate object`)
  }
  const tableMutate = Reflect.get(mutate, rollup.target.logical)
  if (!tableMutate || typeof tableMutate !== 'object') {
    throw new TypeError(`rollup target mutation is unavailable: ${rollup.target.logical}`)
  }
  const method = Reflect.get(tableMutate, operation)
  if (typeof method !== 'function') {
    throw new TypeError(
      `rollup target mutation is unavailable: ${rollup.target.logical}.${operation}`
    )
  }
  await Reflect.apply(method, tableMutate, [row])
}

async function addRollupContribution(
  tx: object,
  builder: object,
  rollup: CompiledRollup,
  sourceRow: RollupRow
): Promise<void> {
  const key = groupKey(rollup, sourceRow)
  const targetRow = await readRollupRow(
    tx,
    builder,
    rollup.target.logical,
    rollup.target.primaryKey,
    key
  )
  const contribution = aggregateContribution(rollup, sourceRow)
  if (!targetRow) {
    if (rollup.mode === 'materialized') {
      await mutateRollupTarget(tx, rollup, 'insert', { ...key, ...contribution })
    }
    return
  }
  const update: Record<string, unknown> = { ...key }
  for (const aggregate of rollup.aggregates) {
    update[aggregate.target.logical] =
      numericValue(
        targetRow[aggregate.target.logical],
        `rollup ${rollup.name} target ${rollup.target.logical}.${aggregate.target.logical}`,
        false
      ) + contribution[aggregate.target.logical]!
  }
  await mutateRollupTarget(tx, rollup, 'update', update)
}

async function removeRollupContribution(
  tx: object,
  builder: object,
  rollup: CompiledRollup,
  sourceRow: RollupRow
): Promise<void> {
  const key = groupKey(rollup, sourceRow)
  const targetRow = await readRollupRow(
    tx,
    builder,
    rollup.target.logical,
    rollup.target.primaryKey,
    key
  )
  if (!targetRow) return
  const contribution = aggregateContribution(rollup, sourceRow)
  const update: Record<string, unknown> = { ...key }
  let remainingCount: number | null = null
  for (const aggregate of rollup.aggregates) {
    const next =
      numericValue(
        targetRow[aggregate.target.logical],
        `rollup ${rollup.name} target ${rollup.target.logical}.${aggregate.target.logical}`,
        false
      ) - contribution[aggregate.target.logical]!
    update[aggregate.target.logical] = next
    if (aggregate.kind === 'count') remainingCount = next
  }
  if (rollup.mode === 'materialized' && remainingCount !== null && remainingCount <= 0) {
    await mutateRollupTarget(tx, rollup, 'delete', key)
    return
  }
  await mutateRollupTarget(tx, rollup, 'update', update)
}

async function updateRollupContribution(
  tx: object,
  builder: object,
  rollup: CompiledRollup,
  previousSourceRow: RollupRow,
  nextSourceRow: RollupRow
): Promise<void> {
  const previousKey = groupKey(rollup, previousSourceRow)
  const nextKey = groupKey(rollup, nextSourceRow)
  if (!sameKey(rollup, previousKey, nextKey)) {
    await removeRollupContribution(tx, builder, rollup, previousSourceRow)
    await addRollupContribution(tx, builder, rollup, nextSourceRow)
    return
  }

  const previous = aggregateContribution(rollup, previousSourceRow)
  const next = aggregateContribution(rollup, nextSourceRow)
  if (
    rollup.aggregates.every((aggregate) =>
      Object.is(previous[aggregate.target.logical], next[aggregate.target.logical])
    )
  ) {
    return
  }
  const targetRow = await readRollupRow(
    tx,
    builder,
    rollup.target.logical,
    rollup.target.primaryKey,
    nextKey
  )
  if (!targetRow) return
  const update: Record<string, unknown> = { ...nextKey }
  for (const aggregate of rollup.aggregates) {
    update[aggregate.target.logical] =
      numericValue(
        targetRow[aggregate.target.logical],
        `rollup ${rollup.name} target ${rollup.target.logical}.${aggregate.target.logical}`,
        false
      ) +
      next[aggregate.target.logical]! -
      previous[aggregate.target.logical]!
  }
  await mutateRollupTarget(tx, rollup, 'update', update)
}

/**
 * Wrap a Zero client transaction so source-table CRUD also projects its
 * declared rollups. Server transactions are returned unchanged because SQLite
 * triggers own the authoritative write.
 */
export function withOptimisticRollups<T extends object>(tx: T, rollups: RollupSet): T {
  const runtime = rollups[ROLLUP_RUNTIME]
  if (Reflect.get(tx, 'location') !== 'client' || runtime.definitions.length === 0) {
    return tx
  }
  const baseMutate = Reflect.get(tx, 'mutate')
  if (!baseMutate || typeof baseMutate !== 'object') {
    throw new TypeError(`rollup transaction has no mutate object`)
  }
  const builder = runtime.queryBuilder
  const bySource = runtime.bySource
  const tableProxies = new Map<PropertyKey, object>()
  const wrappedMutate = new Proxy(baseMutate, {
    get(target, tableName, receiver) {
      if (typeof tableName !== 'string' || !bySource.has(tableName)) {
        return Reflect.get(target, tableName, receiver)
      }
      const cached = tableProxies.get(tableName)
      if (cached) return cached
      const tableMutate = Reflect.get(target, tableName, receiver)
      if (!tableMutate || typeof tableMutate !== 'object') return tableMutate
      const tableProxy = new Proxy(tableMutate, {
        get(tableTarget, operation, tableReceiver) {
          const method = Reflect.get(tableTarget, operation, tableReceiver)
          if (
            typeof operation !== 'string' ||
            !['insert', 'upsert', 'update', 'delete'].includes(operation) ||
            typeof method !== 'function'
          ) {
            return method
          }
          return async (...args: unknown[]) => {
            const input = rollupRow(
              args[0],
              `rollup source mutation ${tableName}.${operation}`
            )
            const definitions = bySource.get(tableName)!
            const source = definitions[0]!.source
            const previous =
              operation === 'insert'
                ? null
                : await readRollupRow(
                    tx,
                    builder,
                    source.logical,
                    source.primaryKey,
                    input
                  )
            await Reflect.apply(method, tableTarget, args)
            const next =
              operation === 'delete'
                ? null
                : await readRollupRow(
                    tx,
                    builder,
                    source.logical,
                    source.primaryKey,
                    input
                  )
            for (const rollup of definitions) {
              if (previous && next) {
                await updateRollupContribution(tx, builder, rollup, previous, next)
              } else if (previous) {
                await removeRollupContribution(tx, builder, rollup, previous)
              } else if (next) {
                await addRollupContribution(tx, builder, rollup, next)
              }
            }
          }
        },
      })
      tableProxies.set(tableName, tableProxy)
      return tableProxy
    },
  })
  return new Proxy(tx, {
    get(target, property, receiver) {
      if (property === 'mutate') return wrappedMutate
      const value = Reflect.get(target, property, receiver)
      return typeof value === 'function' ? value.bind(target) : value
    },
  })
}
