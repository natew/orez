export interface DurableSqlResult {
  one(): Record<string, unknown> | undefined
  toArray(): Array<Record<string, unknown>>
}

export interface DurableSqlStorage {
  exec(sql: string, ...params: unknown[]): DurableSqlResult
}

const WATERMARK_STATE_TABLE = '_zero_change_state'

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

function finitePositiveNumber(value: unknown): number {
  const number = Number(value ?? 0)
  return Number.isFinite(number) && number > 0 ? number : 0
}

export class DurableWatermarkState {
  constructor(private readonly sql: DurableSqlStorage) {}

  ensureTables(): void {
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS _zero_changes (watermark INTEGER PRIMARY KEY AUTOINCREMENT, table_name TEXT NOT NULL, op TEXT NOT NULL CHECK (op IN ('INSERT', 'UPDATE', 'DELETE')), row_data TEXT, old_data TEXT, created_at INTEGER NOT NULL DEFAULT (unixepoch()))"
    )
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${quoteIdent(WATERMARK_STATE_TABLE)} (id INTEGER PRIMARY KEY CHECK (id = 1), last_value INTEGER NOT NULL DEFAULT 0)`
    )
    this.setWatermarkState(this.watermarkState())
  }

  next(): number {
    return this.current() + 1
  }

  mark(watermark: number): void {
    this.setWatermarkState(watermark)
    this.updateWatermarkSequences(watermark)
  }

  current(): number {
    this.ensureTables()
    const state = this.watermarkState()
    const row = this.sql
      .exec('SELECT COALESCE(MAX(watermark), 0) AS watermark FROM _zero_changes')
      .one() as { watermark?: unknown } | undefined
    const tableWatermark = finitePositiveNumber(row?.watermark)
    const sequenceWatermark = this.watermarkSequenceValue()
    const watermark = Math.max(state, tableWatermark, sequenceWatermark)
    if (watermark > state) this.setWatermarkState(watermark)
    if (watermark > sequenceWatermark) this.updateWatermarkSequences(watermark)
    return watermark
  }

  private watermarkState(): number {
    try {
      const table = quoteIdent(WATERMARK_STATE_TABLE)
      const row = this.sql.exec(`SELECT last_value FROM ${table} WHERE id = 1`).one() as
        | { last_value?: unknown }
        | undefined
      return finitePositiveNumber(row?.last_value)
    } catch {
      return 0
    }
  }

  private setWatermarkState(watermark: number): void {
    const table = quoteIdent(WATERMARK_STATE_TABLE)
    this.sql.exec(`INSERT OR IGNORE INTO ${table} (id, last_value) VALUES (1, 0)`)
    this.sql.exec(`UPDATE ${table} SET last_value = ? WHERE id = 1`, watermark)
  }

  private watermarkSequenceValue(): number {
    let watermark = 0
    for (const name of this.watermarkSequenceTables()) {
      try {
        const row = this.sql
          .exec(`SELECT last_value, is_called FROM ${quoteIdent(name)} WHERE dummy = 1`)
          .one() as { last_value?: unknown; is_called?: unknown } | undefined
        if (!row || !row.is_called) continue
        watermark = Math.max(watermark, finitePositiveNumber(row.last_value))
      } catch {
        /* not an orez sequence table */
      }
    }
    return watermark
  }

  private watermarkSequenceTables(): string[] {
    return this.sql
      .exec(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%zero_watermark%'"
      )
      .toArray()
      .map((row) => String(row.name || ''))
      .filter(Boolean)
  }

  private updateWatermarkSequences(watermark: number): void {
    for (const name of this.watermarkSequenceTables()) {
      const table = quoteIdent(name)
      try {
        this.sql.exec(
          `INSERT OR IGNORE INTO ${table} (dummy, last_value, is_called) VALUES (1, ?, 1)`,
          watermark
        )
        this.sql.exec(
          `UPDATE ${table} SET last_value = ?, is_called = 1 WHERE dummy = 1`,
          watermark
        )
      } catch {
        /* not an orez sequence table */
      }
    }
  }
}
