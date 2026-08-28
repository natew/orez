import { createBuilder } from '@rocicorp/zero'

import type { Schema } from '@rocicorp/zero'

export type AggregateMode = 'existing' | 'materialized'

export type CountColumn = {
  readonly kind: 'count'
}

export type SumColumn<SourceColumn extends string = string> = {
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

export type AggregateDefinition<S extends Schema> = {
  [TSource in TableName<S>]: {
    [TTarget in TableName<S>]: {
      readonly source: TSource
      readonly target: TTarget
      readonly mode: AggregateMode
      readonly groupBy: Readonly<
        Partial<Record<ColumnName<S, TSource>, ColumnName<S, TTarget>>>
      >
      readonly columns: Readonly<
        Partial<
          Record<
            NumericColumnName<S, TTarget>,
            CountColumn | SumColumn<NumericColumnName<S, TSource>>
          >
        >
      >
    }
  }[TableName<S>]
}[TableName<S>]

type ColumnRef = {
  readonly logical: string
  readonly physical: string
}

type CompiledGroup = {
  readonly source: ColumnRef
  readonly target: ColumnRef
}

type CompiledColumn =
  | {
      readonly kind: 'count'
      readonly target: ColumnRef
    }
  | {
      readonly kind: 'sum'
      readonly source: ColumnRef
      readonly target: ColumnRef
    }

type CompiledAggregate = {
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
  readonly mode: AggregateMode
  readonly groups: readonly CompiledGroup[]
  readonly columns: readonly CompiledColumn[]
}

type AggregateRuntime<S extends Schema> = {
  readonly schema: S
  readonly definitions: readonly CompiledAggregate[]
  readonly queryBuilder: object
  readonly bySource: ReadonlyMap<string, readonly CompiledAggregate[]>
}

const AGGREGATE_RUNTIME: unique symbol = Symbol.for('orez-lite:aggregate-runtime')

export type AggregateSet<S extends Schema = Schema> = {
  readonly [AGGREGATE_RUNTIME]: AggregateRuntime<S>
}

export function count(): CountColumn {
  return { kind: 'count' }
}

export function sum<const SourceColumn extends string>(
  source: SourceColumn
): SumColumn<SourceColumn> {
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
  aggregateName: string,
  tableName: string,
  columns: Readonly<Record<string, unknown>>,
  columnName: string
): unknown {
  if (!Object.hasOwn(columns, columnName)) {
    throw new TypeError(
      `aggregate ${aggregateName} references unknown column ${tableName}.${columnName}`
    )
  }
  return columns[columnName]
}

// what a namespace's aggregates.ts exports. annotate the declaration with
// `satisfies AggregateDefinitions<typeof schema>` so a bad table, column, or
// mode is reported in the file that declares it rather than in generated output.
export type AggregateDefinitions<S extends Schema> = Readonly<
  Record<string, AggregateDefinition<S>>
>

type UnionToIntersection<U> = (U extends unknown ? (arg: U) => void : never) extends (
  arg: infer I
) => void
  ? I
  : never

// combine per-namespace definition records into the single record
// defineAggregates compiles. spreading them would let a name declared in two
// namespaces silently win by source order, and the loser's triggers would never
// install — the same invisible zero-total failure a missed definition causes.
//
// the parameter stays unconstrained so each namespace's literal types survive
// into defineAggregates, which is where they get checked against the schema.
// constraining to AggregateDefinitions here has nothing to infer S from, so it
// widens every source and target back to string and the schema check is lost.
export function mergeAggregateDefinitions<
  const Records extends ReadonlyArray<Readonly<Record<string, object>>>,
>(...records: Records): UnionToIntersection<Records[number]> {
  const merged: Record<string, object> = {}
  for (const record of records) {
    for (const [name, definition] of Object.entries(record)) {
      if (name in merged) {
        throw new TypeError(`aggregate ${name} is declared in more than one namespace`)
      }
      merged[name] = definition
    }
  }
  return merged as UnionToIntersection<Records[number]>
}

export function defineAggregates<
  S extends Schema,
  const Definitions extends Readonly<Record<string, AggregateDefinition<S>>>,
>(schema: S, definitions: Definitions): AggregateSet<S> {
  const compiled: CompiledAggregate[] = []
  const triggerNames = new Set<string>()

  for (const [name, definition] of Object.entries(definitions)) {
    if (!/^[A-Za-z][A-Za-z0-9_]{0,62}$/.test(name)) {
      throw new TypeError(
        `aggregate name must be an identifier of at most 63 characters: ${JSON.stringify(name)}`
      )
    }
    if (triggerNames.has(name)) {
      throw new TypeError(`duplicate aggregate name: ${name}`)
    }
    triggerNames.add(name)

    const source = schema.tables[definition.source]
    const target = schema.tables[definition.target]
    if (!source) {
      throw new TypeError(
        `aggregate ${name} references unknown table ${definition.source}`
      )
    }
    if (!target) {
      throw new TypeError(
        `aggregate ${name} references unknown table ${definition.target}`
      )
    }
    if (definition.source === definition.target) {
      throw new TypeError(`aggregate ${name} must use different source and target tables`)
    }

    const groups = Object.entries(definition.groupBy).map(
      ([sourceColumnName, targetColumnName]) => {
        if (typeof targetColumnName !== 'string') {
          throw new TypeError(`aggregate ${name} has an invalid group mapping`)
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
      throw new TypeError(`aggregate ${name} must group by at least one column`)
    }
    if (new Set(groups.map((group) => group.target.logical)).size !== groups.length) {
      throw new TypeError(`aggregate ${name} maps more than one source column to one key`)
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
        `aggregate ${name} groupBy must map every primary key column of ${definition.target}`
      )
    }

    const columns: CompiledColumn[] = Object.entries(definition.columns).map(
      ([targetColumnName, column]) => {
        if (!column || typeof column !== 'object') {
          throw new TypeError(`aggregate ${name} has an invalid column`)
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
            `aggregate ${name} target column ${definition.target}.${targetColumnName} must be numeric`
          )
        }
        const compiledTarget = {
          logical: targetColumnName,
          physical: columnPhysicalName(targetColumnName, targetColumn),
        }
        const kind = Reflect.get(column, 'kind')
        if (kind === 'count') {
          return { kind: 'count', target: compiledTarget }
        }
        const columnSource = Reflect.get(column, 'source')
        if (kind !== 'sum' || typeof columnSource !== 'string') {
          throw new TypeError(`aggregate ${name} has an invalid column`)
        }
        const sourceColumn = ensureOwnColumn(
          name,
          definition.source,
          source.columns,
          columnSource
        )
        if (
          !sourceColumn ||
          typeof sourceColumn !== 'object' ||
          !('type' in sourceColumn) ||
          sourceColumn.type !== 'number'
        ) {
          throw new TypeError(
            `aggregate ${name} source column ${definition.source}.${columnSource} must be numeric`
          )
        }
        return {
          kind: 'sum',
          source: {
            logical: columnSource,
            physical: columnPhysicalName(columnSource, sourceColumn),
          },
          target: compiledTarget,
        }
      }
    )
    if (columns.length === 0) {
      throw new TypeError(`aggregate ${name} must declare at least one column`)
    }
    if (columns.filter((column) => column.kind === 'count').length > 1) {
      throw new TypeError(`aggregate ${name} may declare only one count column`)
    }
    if (
      groups.some((group) =>
        columns.some((column) => column.target.logical === group.target.logical)
      )
    ) {
      throw new TypeError(`aggregate ${name} cannot write a primary key column`)
    }

    if (definition.mode === 'materialized') {
      if (!columns.some((column) => column.kind === 'count')) {
        throw new TypeError(
          `materialized aggregate ${name} needs a count column to remove empty groups`
        )
      }
      const ownedTargetColumns = new Set([
        ...groups.map((group) => group.target.logical),
        ...columns.map((column) => column.target.logical),
      ])
      const extraTargetColumn = Object.keys(target.columns).find(
        (columnName) => !ownedTargetColumns.has(columnName)
      )
      if (extraTargetColumn) {
        throw new TypeError(
          `materialized aggregate ${name} target has non-aggregate column ${definition.target}.${extraTargetColumn}`
        )
      }
    } else if (definition.mode !== 'existing') {
      throw new TypeError(`aggregate ${name} has an invalid target mode`)
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
      columns,
    })
  }

  const sourceTables = new Set(compiled.map((aggregate) => aggregate.source.logical))
  for (const aggregate of compiled) {
    if (sourceTables.has(aggregate.target.logical)) {
      throw new TypeError(
        `aggregate ${aggregate.name} target ${aggregate.target.logical} cannot feed another aggregate`
      )
    }
    for (const candidate of compiled) {
      if (
        candidate === aggregate ||
        candidate.target.logical !== aggregate.target.logical
      ) {
        continue
      }
      if (aggregate.mode === 'materialized' || candidate.mode === 'materialized') {
        throw new TypeError(
          `materialized aggregate target ${aggregate.target.logical} may have only one owner`
        )
      }
      const duplicate = aggregate.columns.find((column) =>
        candidate.columns.some((other) => other.target.logical === column.target.logical)
      )
      if (duplicate) {
        throw new TypeError(
          `aggregate target column ${aggregate.target.logical}.${duplicate.target.logical} has more than one owner`
        )
      }
    }
  }

  const bySource = new Map<string, CompiledAggregate[]>()
  for (const aggregate of compiled) {
    const sourceAggregates = bySource.get(aggregate.source.logical)
    if (sourceAggregates) sourceAggregates.push(aggregate)
    else bySource.set(aggregate.source.logical, [aggregate])
  }

  return {
    [AGGREGATE_RUNTIME]: {
      schema,
      definitions: compiled,
      queryBuilder: createBuilder(schema),
      bySource,
    },
  }
}

function sourceReference(column: ColumnRef, row: 'NEW' | 'OLD'): string {
  return `${row}.${quoteIdentifier(column.physical)}`
}

function targetWhere(aggregate: CompiledAggregate, row: 'NEW' | 'OLD'): string {
  return aggregate.groups
    .map(
      (group) =>
        `${quoteIdentifier(group.target.physical)} IS ${sourceReference(group.source, row)}`
    )
    .join(' AND ')
}

function sameGroup(aggregate: CompiledAggregate): string {
  return aggregate.groups
    .map(
      (group) =>
        `${sourceReference(group.source, 'OLD')} IS ${sourceReference(group.source, 'NEW')}`
    )
    .join(' AND ')
}

function aggregateChanged(aggregate: CompiledAggregate): string {
  const sums = aggregate.columns.filter(
    (column): column is Extract<CompiledColumn, { kind: 'sum' }> => column.kind === 'sum'
  )
  return sums.length
    ? sums
        .map(
          (column) =>
            `${sourceReference(column.source, 'OLD')} IS NOT ${sourceReference(column.source, 'NEW')}`
        )
        .join(' OR ')
    : '0'
}

function contribution(column: CompiledColumn, row: 'NEW' | 'OLD'): string {
  return column.kind === 'count'
    ? '1'
    : `COALESCE(${sourceReference(column.source, row)}, 0)`
}

function assignments(
  aggregate: CompiledAggregate,
  operation: 'add' | 'remove' | 'delta'
): string {
  return aggregate.columns
    .flatMap((column) => {
      const target = quoteIdentifier(column.target.physical)
      if (operation === 'delta') {
        return column.kind === 'sum'
          ? [
              `${target} = ${target} + ${contribution(column, 'NEW')} - ${contribution(column, 'OLD')}`,
            ]
          : []
      }
      const operator = operation === 'add' ? '+' : '-'
      const row = operation === 'add' ? 'NEW' : 'OLD'
      return [`${target} = ${target} ${operator} ${contribution(column, row)}`]
    })
    .join(', ')
}

function insertColumns(aggregate: CompiledAggregate): string {
  return [
    ...aggregate.groups.map((group) => group.target),
    ...aggregate.columns.map((a) => a.target),
  ]
    .map((column) => quoteIdentifier(column.physical))
    .join(', ')
}

function insertValues(aggregate: CompiledAggregate, row: 'NEW' | 'OLD'): string {
  return [
    ...aggregate.groups.map((group) => sourceReference(group.source, row)),
    ...aggregate.columns.map((column) => contribution(column, row)),
  ].join(', ')
}

function upsertAssignments(aggregate: CompiledAggregate): string {
  return aggregate.columns
    .map((column) => {
      const target = quoteIdentifier(column.target.physical)
      return `${target} = ${target} + excluded.${target}`
    })
    .join(', ')
}

function countTarget(aggregate: CompiledAggregate): ColumnRef {
  const column = aggregate.columns.find(
    (candidate): candidate is Extract<CompiledColumn, { kind: 'count' }> =>
      candidate.kind === 'count'
  )
  if (!column) {
    throw new TypeError(`materialized aggregate ${aggregate.name} has no count column`)
  }
  return column.target
}

function watchedSourceColumns(aggregate: CompiledAggregate): string {
  return [
    ...new Set([
      ...aggregate.groups.map((group) => group.source.physical),
      ...aggregate.columns.flatMap((column) =>
        column.kind === 'sum' ? [column.source.physical] : []
      ),
    ]),
  ]
    .map(quoteIdentifier)
    .join(', ')
}

function backfillStatements(aggregate: CompiledAggregate): string[] {
  const sourceTable = quoteIdentifier(aggregate.source.physical)
  const targetTable = quoteIdentifier(aggregate.target.physical)
  if (aggregate.mode === 'materialized') {
    const select = [
      ...aggregate.groups.map((group) => quoteIdentifier(group.source.physical)),
      ...aggregate.columns.map((column) =>
        column.kind === 'count'
          ? 'COUNT(*)'
          : `COALESCE(SUM(${quoteIdentifier(column.source.physical)}), 0)`
      ),
    ].join(', ')
    const groupBy = aggregate.groups
      .map((group) => quoteIdentifier(group.source.physical))
      .join(', ')
    return [
      `DELETE FROM ${targetTable}`,
      `INSERT INTO ${targetTable} (${insertColumns(aggregate)}) SELECT ${select} FROM ${sourceTable} GROUP BY ${groupBy}`,
    ]
  }

  const correlation = aggregate.groups
    .map(
      (group) =>
        `${sourceTable}.${quoteIdentifier(group.source.physical)} IS ${targetTable}.${quoteIdentifier(group.target.physical)}`
    )
    .join(' AND ')
  const values = aggregate.columns
    .map((column) => {
      const target = quoteIdentifier(column.target.physical)
      const value =
        column.kind === 'count'
          ? `COUNT(*)`
          : `COALESCE(SUM(${sourceTable}.${quoteIdentifier(column.source.physical)}), 0)`
      return `${target} = (SELECT ${value} FROM ${sourceTable} WHERE ${correlation})`
    })
    .join(', ')
  return [`UPDATE ${targetTable} SET ${values}`]
}

function triggerStatements(aggregate: CompiledAggregate): string[] {
  const sourceTable = quoteIdentifier(aggregate.source.physical)
  const targetTable = quoteIdentifier(aggregate.target.physical)
  const insertTrigger = quoteIdentifier(`_orez_aggregate_${aggregate.name}_insert`)
  const updateTrigger = quoteIdentifier(`_orez_aggregate_${aggregate.name}_update`)
  const deleteTrigger = quoteIdentifier(`_orez_aggregate_${aggregate.name}_delete`)
  const keyConflict = aggregate.groups
    .map((group) => quoteIdentifier(group.target.physical))
    .join(', ')
  const changedGroup = `NOT (${sameGroup(aggregate)})`
  const sumsChanged = aggregateChanged(aggregate)

  if (aggregate.mode === 'existing') {
    const updateColumns = watchedSourceColumns(aggregate)
    const updateBody = [
      `UPDATE ${targetTable} SET ${assignments(aggregate, 'remove')} WHERE ${targetWhere(aggregate, 'OLD')} AND ${changedGroup};`,
      `UPDATE ${targetTable} SET ${assignments(aggregate, 'add')} WHERE ${targetWhere(aggregate, 'NEW')} AND ${changedGroup};`,
      ...(assignments(aggregate, 'delta')
        ? [
            `UPDATE ${targetTable} SET ${assignments(aggregate, 'delta')} WHERE ${targetWhere(aggregate, 'NEW')} AND (${sameGroup(aggregate)}) AND (${sumsChanged});`,
          ]
        : []),
    ].join(' ')
    return [
      `CREATE TRIGGER ${insertTrigger} AFTER INSERT ON ${sourceTable} BEGIN UPDATE ${targetTable} SET ${assignments(aggregate, 'add')} WHERE ${targetWhere(aggregate, 'NEW')}; END`,
      `CREATE TRIGGER ${updateTrigger} AFTER UPDATE OF ${updateColumns} ON ${sourceTable} BEGIN ${updateBody} END`,
      `CREATE TRIGGER ${deleteTrigger} AFTER DELETE ON ${sourceTable} BEGIN UPDATE ${targetTable} SET ${assignments(aggregate, 'remove')} WHERE ${targetWhere(aggregate, 'OLD')}; END`,
    ]
  }

  const countColumn = quoteIdentifier(countTarget(aggregate).physical)
  const insert = `INSERT INTO ${targetTable} (${insertColumns(aggregate)}) VALUES (${insertValues(aggregate, 'NEW')}) ON CONFLICT (${keyConflict}) DO UPDATE SET ${upsertAssignments(aggregate)};`
  const updateColumns = watchedSourceColumns(aggregate)
  const updateBody = [
    `UPDATE ${targetTable} SET ${assignments(aggregate, 'remove')} WHERE ${targetWhere(aggregate, 'OLD')} AND ${changedGroup};`,
    `DELETE FROM ${targetTable} WHERE ${targetWhere(aggregate, 'OLD')} AND ${countColumn} <= 0 AND ${changedGroup};`,
    `INSERT INTO ${targetTable} (${insertColumns(aggregate)}) SELECT ${insertValues(aggregate, 'NEW')} WHERE ${changedGroup} ON CONFLICT (${keyConflict}) DO UPDATE SET ${upsertAssignments(aggregate)};`,
    ...(assignments(aggregate, 'delta')
      ? [
          `UPDATE ${targetTable} SET ${assignments(aggregate, 'delta')} WHERE ${targetWhere(aggregate, 'NEW')} AND (${sameGroup(aggregate)}) AND (${sumsChanged});`,
        ]
      : []),
  ].join(' ')
  return [
    `CREATE TRIGGER ${insertTrigger} AFTER INSERT ON ${sourceTable} BEGIN ${insert} END`,
    `CREATE TRIGGER ${updateTrigger} AFTER UPDATE OF ${updateColumns} ON ${sourceTable} BEGIN ${updateBody} END`,
    `CREATE TRIGGER ${deleteTrigger} AFTER DELETE ON ${sourceTable} BEGIN UPDATE ${targetTable} SET ${assignments(aggregate, 'remove')} WHERE ${targetWhere(aggregate, 'OLD')}; DELETE FROM ${targetTable} WHERE ${targetWhere(aggregate, 'OLD')} AND ${countColumn} <= 0; END`,
  ]
}

export function aggregateMigrationStatements(
  aggregates: AggregateSet
): readonly string[] {
  return aggregates[AGGREGATE_RUNTIME].definitions.flatMap((aggregate) => {
    const triggerNames = ['insert', 'update', 'delete'].map((operation) =>
      quoteIdentifier(`_orez_aggregate_${aggregate.name}_${operation}`)
    )
    return [
      ...triggerNames.map((name) => `DROP TRIGGER IF EXISTS ${name}`),
      ...backfillStatements(aggregate),
      ...triggerStatements(aggregate),
    ]
  })
}

export function aggregateMigrationSQL(aggregates: AggregateSet): string {
  return aggregateMigrationStatements(aggregates).join('\n--> statement-breakpoint\n')
}

type AggregateRow = Readonly<Record<string, unknown>>
type AggregateTransactionOverlay = Map<string, AggregateRow | null>

function aggregateRow(value: unknown, context: string): AggregateRow {
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

async function readAggregateRow(
  tx: object,
  builder: object,
  tableName: string,
  primaryKey: readonly string[],
  key: AggregateRow,
  overlay?: AggregateTransactionOverlay
): Promise<AggregateRow | null> {
  const overlayKey = JSON.stringify([
    tableName,
    ...primaryKey.map((columnName) => key[columnName]),
  ])
  if (overlay?.has(overlayKey)) return overlay.get(overlayKey) ?? null

  let query: unknown = Reflect.get(builder, tableName)
  if (!query || typeof query !== 'object') {
    throw new TypeError(`aggregate query table is unavailable: ${tableName}`)
  }
  for (const columnName of primaryKey) {
    const value = key[columnName]
    if (value === undefined || value === null) {
      throw new TypeError(`aggregate key ${tableName}.${columnName} must be non-null`)
    }
    const where = Reflect.get(query, 'where')
    if (typeof where !== 'function') {
      throw new TypeError(`aggregate query does not support where(): ${tableName}`)
    }
    query = Reflect.apply(where, query, [columnName, '=', value])
    if (!query || typeof query !== 'object') {
      throw new TypeError(`aggregate query returned an invalid value: ${tableName}`)
    }
  }
  const one = Reflect.get(query, 'one')
  const run = Reflect.get(tx, 'run')
  if (typeof one !== 'function' || typeof run !== 'function') {
    throw new TypeError(`aggregate transaction cannot read ${tableName}`)
  }
  const singular = Reflect.apply(one, query, [])
  const result = await Reflect.apply(run, tx, [singular])
  const row =
    result === undefined || result === null
      ? null
      : aggregateRow(result, `aggregate query result for ${tableName}`)
  overlay?.set(overlayKey, row)
  return row
}

function groupKey(aggregate: CompiledAggregate, row: AggregateRow): AggregateRow {
  const key: Record<string, unknown> = {}
  for (const group of aggregate.groups) {
    const value = row[group.source.logical]
    if (value === undefined || value === null) {
      throw new TypeError(
        `aggregate ${aggregate.name} group ${aggregate.source.logical}.${group.source.logical} must be non-null`
      )
    }
    key[group.target.logical] = value
  }
  return key
}

function sameKey(
  aggregate: CompiledAggregate,
  left: AggregateRow,
  right: AggregateRow
): boolean {
  return aggregate.groups.every((group) =>
    Object.is(left[group.target.logical], right[group.target.logical])
  )
}

function aggregateContribution(
  aggregate: CompiledAggregate,
  row: AggregateRow
): Readonly<Record<string, number>> {
  const contribution: Record<string, number> = {}
  for (const column of aggregate.columns) {
    contribution[column.target.logical] =
      column.kind === 'count'
        ? 1
        : numericValue(
            row[column.source.logical],
            `aggregate ${aggregate.name} source ${aggregate.source.logical}.${column.source.logical}`,
            true
          )
  }
  return contribution
}

async function mutateAggregateTarget(
  tx: object,
  aggregate: CompiledAggregate,
  operation: 'delete' | 'insert' | 'update',
  row: AggregateRow,
  overlay: AggregateTransactionOverlay
): Promise<void> {
  const mutate = Reflect.get(tx, 'mutate')
  if (!mutate || typeof mutate !== 'object') {
    throw new TypeError(`aggregate transaction has no mutate object`)
  }
  const tableMutate = Reflect.get(mutate, aggregate.target.logical)
  if (!tableMutate || typeof tableMutate !== 'object') {
    throw new TypeError(
      `aggregate target mutation is unavailable: ${aggregate.target.logical}`
    )
  }
  const method = Reflect.get(tableMutate, operation)
  if (typeof method !== 'function') {
    throw new TypeError(
      `aggregate target mutation is unavailable: ${aggregate.target.logical}.${operation}`
    )
  }
  await Reflect.apply(method, tableMutate, [row])
  const overlayKey = JSON.stringify([
    aggregate.target.logical,
    ...aggregate.target.primaryKey.map((columnName) => row[columnName]),
  ])
  const previous = overlay.get(overlayKey)
  overlay.set(
    overlayKey,
    operation === 'delete'
      ? null
      : operation === 'update' && previous
        ? { ...previous, ...row }
        : row
  )
}

async function addAggregateContribution(
  tx: object,
  builder: object,
  aggregate: CompiledAggregate,
  sourceRow: AggregateRow,
  overlay: AggregateTransactionOverlay
): Promise<void> {
  const key = groupKey(aggregate, sourceRow)
  const targetRow = await readAggregateRow(
    tx,
    builder,
    aggregate.target.logical,
    aggregate.target.primaryKey,
    key,
    overlay
  )
  const contribution = aggregateContribution(aggregate, sourceRow)
  if (!targetRow) {
    if (aggregate.mode === 'materialized') {
      await mutateAggregateTarget(
        tx,
        aggregate,
        'insert',
        { ...key, ...contribution },
        overlay
      )
    }
    return
  }
  const update: Record<string, unknown> = { ...key }
  for (const column of aggregate.columns) {
    update[column.target.logical] =
      numericValue(
        targetRow[column.target.logical],
        `aggregate ${aggregate.name} target ${aggregate.target.logical}.${column.target.logical}`,
        false
      ) + contribution[column.target.logical]!
  }
  await mutateAggregateTarget(tx, aggregate, 'update', update, overlay)
}

async function removeAggregateContribution(
  tx: object,
  builder: object,
  aggregate: CompiledAggregate,
  sourceRow: AggregateRow,
  overlay: AggregateTransactionOverlay
): Promise<void> {
  const key = groupKey(aggregate, sourceRow)
  const targetRow = await readAggregateRow(
    tx,
    builder,
    aggregate.target.logical,
    aggregate.target.primaryKey,
    key,
    overlay
  )
  if (!targetRow) return
  const contribution = aggregateContribution(aggregate, sourceRow)
  const update: Record<string, unknown> = { ...key }
  let remainingCount: number | null = null
  for (const column of aggregate.columns) {
    const next =
      numericValue(
        targetRow[column.target.logical],
        `aggregate ${aggregate.name} target ${aggregate.target.logical}.${column.target.logical}`,
        false
      ) - contribution[column.target.logical]!
    update[column.target.logical] = next
    if (column.kind === 'count') remainingCount = next
  }
  if (
    aggregate.mode === 'materialized' &&
    remainingCount !== null &&
    remainingCount <= 0
  ) {
    await mutateAggregateTarget(tx, aggregate, 'delete', key, overlay)
    return
  }
  await mutateAggregateTarget(tx, aggregate, 'update', update, overlay)
}

async function updateAggregateContribution(
  tx: object,
  builder: object,
  aggregate: CompiledAggregate,
  previousSourceRow: AggregateRow,
  nextSourceRow: AggregateRow,
  overlay: AggregateTransactionOverlay
): Promise<void> {
  const previousKey = groupKey(aggregate, previousSourceRow)
  const nextKey = groupKey(aggregate, nextSourceRow)
  if (!sameKey(aggregate, previousKey, nextKey)) {
    await removeAggregateContribution(tx, builder, aggregate, previousSourceRow, overlay)
    await addAggregateContribution(tx, builder, aggregate, nextSourceRow, overlay)
    return
  }

  const previous = aggregateContribution(aggregate, previousSourceRow)
  const next = aggregateContribution(aggregate, nextSourceRow)
  if (
    aggregate.columns.every((column) =>
      Object.is(previous[column.target.logical], next[column.target.logical])
    )
  ) {
    return
  }
  const targetRow = await readAggregateRow(
    tx,
    builder,
    aggregate.target.logical,
    aggregate.target.primaryKey,
    nextKey,
    overlay
  )
  if (!targetRow) return
  const update: Record<string, unknown> = { ...nextKey }
  for (const column of aggregate.columns) {
    update[column.target.logical] =
      numericValue(
        targetRow[column.target.logical],
        `aggregate ${aggregate.name} target ${aggregate.target.logical}.${column.target.logical}`,
        false
      ) +
      next[column.target.logical]! -
      previous[column.target.logical]!
  }
  await mutateAggregateTarget(tx, aggregate, 'update', update, overlay)
}

/**
 * Wrap a Zero client transaction so source-table CRUD also projects its
 * declared aggregates. Server transactions are returned unchanged because SQLite
 * triggers own the authoritative write.
 */
export function withOptimisticAggregates<T extends object>(
  tx: T,
  aggregates: AggregateSet
): T {
  const runtime = aggregates[AGGREGATE_RUNTIME]
  if (Reflect.get(tx, 'location') !== 'client' || runtime.definitions.length === 0) {
    return tx
  }
  const baseMutate = Reflect.get(tx, 'mutate')
  if (!baseMutate || typeof baseMutate !== 'object') {
    throw new TypeError(`aggregate transaction has no mutate object`)
  }
  const builder = runtime.queryBuilder
  const bySource = runtime.bySource
  const overlay: AggregateTransactionOverlay = new Map()
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
            const input = aggregateRow(
              args[0],
              `aggregate source mutation ${tableName}.${operation}`
            )
            const definitions = bySource.get(tableName)!
            const source = definitions[0]!.source
            const previous = await readAggregateRow(
              tx,
              builder,
              source.logical,
              source.primaryKey,
              input,
              overlay
            )
            await Reflect.apply(method, tableTarget, args)
            const next =
              operation === 'delete'
                ? null
                : operation === 'update'
                  ? previous && { ...previous, ...input }
                  : operation === 'insert'
                    ? (previous ?? input)
                    : input
            const sourceOverlayKey = JSON.stringify([
              source.logical,
              ...source.primaryKey.map((columnName) => input[columnName]),
            ])
            overlay.set(sourceOverlayKey, next)
            for (const aggregate of definitions) {
              if (previous && next) {
                await updateAggregateContribution(
                  tx,
                  builder,
                  aggregate,
                  previous,
                  next,
                  overlay
                )
              } else if (previous) {
                await removeAggregateContribution(
                  tx,
                  builder,
                  aggregate,
                  previous,
                  overlay
                )
              } else if (next) {
                await addAggregateContribution(tx, builder, aggregate, next, overlay)
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
