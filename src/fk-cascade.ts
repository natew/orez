/**
 * fk-cascade — pg→sqlite compat for ON DELETE actions.
 *
 * orez's backends drop every FOREIGN KEY constraint when translating PG DDL to
 * their store (the PGlite shim regex-strips them; the DO backend filters the
 * CONSTR_FOREIGN AST nodes). that leaves the store with no FK enforcement and,
 * crucially, no `ON DELETE CASCADE` / `ON DELETE SET NULL` — so a plain
 * `DELETE FROM parent` orphans every child row instead of cascading.
 *
 * native engine cascade is not an option here: change capture is trigger-based
 * (`_zero_change_trigger` / `_zero_track_change`), and SQLite does NOT fire
 * triggers for native foreign-key cascade actions unless `recursive_triggers`
 * is on. an engine-level cascade would delete child rows invisibly, so zero
 * clients would never learn the rows are gone — strictly worse than orphaning.
 *
 * instead we restore faithful PG semantics by EXPANSION: capture the FK edges
 * the backends were about to drop, and rewrite each `DELETE FROM parent` into
 * an ordered set of explicit, set-based child statements (leaves-first) plus
 * the original parent delete. every emitted statement is a real DELETE/UPDATE,
 * so it goes through the same change-tracking trigger and replicates to zero
 * clients exactly like any other write. one implementation, both backends.
 *
 * this module is pure: it operates on already-parsed libpg_query CREATE TABLE
 * AST nodes (for edge capture) and plain SQL strings (for expansion). the
 * caller owns naming — it passes a `resolveTable` that produces the canonical
 * SQL identifier for a table in its namespace (schema-qualified for PGlite,
 * flattened for the DO backend). that identifier doubles as the registry key,
 * so capture and expansion stay consistent within a backend.
 */

/** the ON DELETE actions we can faithfully expand. restrict/no-action/set-default are left alone (no enforcement, matching today). */
export type FkDeleteAction = 'cascade' | 'set-null'

export interface FkChild {
  /** canonical SQL identifier of the child (referencing) table; also its registry key. */
  table: string
  /** child FK column identifiers (already quoted), in FK order. */
  columns: string[]
  /** referenced parent column identifiers (already quoted), aligned with `columns`. */
  refColumns: string[]
  onDelete: FkDeleteAction
}

/**
 * parent-table → children that reference it. built once from DDL, consulted on
 * every DELETE. keyed by the canonical table identifier `resolveTable` produces.
 */
export class FkCascadeRegistry {
  private byParent = new Map<string, FkChild[]>()

  add(parentKey: string, child: FkChild): void {
    const list = this.byParent.get(parentKey)
    if (list) list.push(child)
    else this.byParent.set(parentKey, [child])
  }

  childrenOf(parentKey: string): readonly FkChild[] {
    return this.byParent.get(parentKey) ?? EMPTY
  }

  /** true once any cascade/set-null edge has been captured — lets backends skip the DELETE parse on FK-free schemas. */
  get hasEdges(): boolean {
    return this.byParent.size > 0
  }

  clear(): void {
    this.byParent.clear()
  }
}

const EMPTY: readonly FkChild[] = Object.freeze([])

export interface TableRef {
  schemaname?: string
  relname: string
}

export type ResolveTable = (ref: TableRef) => string

/** libpg_query fk_del_action codes → the action we expand (others: no enforcement, skip). */
const DELETE_ACTION: Record<string, FkDeleteAction | undefined> = {
  c: 'cascade', // CASCADE
  n: 'set-null', // SET NULL
  // a: NO ACTION, r: RESTRICT, d: SET DEFAULT — no faithful expansion, leave unenforced
}

export function quoteIdent(name: string): string {
  return '"' + name.replace(/"/g, '""') + '"'
}

function stringValues(nodes: unknown): string[] {
  if (!Array.isArray(nodes)) return []
  const out: string[] = []
  for (const node of nodes) {
    const sval = (node as { String?: { sval?: string; str?: string } })?.String
    const value = sval?.sval ?? sval?.str
    if (typeof value === 'string') out.push(value)
  }
  return out
}

/**
 * capture every cascade/set-null FK edge declared by a CREATE TABLE statement
 * into `registry`. handles both inline column FKs (`col … REFERENCES parent(c)`)
 * and table-level FKs (`FOREIGN KEY (col) REFERENCES parent(c)`). call this at
 * the exact site each backend drops the constraint, BEFORE it is discarded.
 */
export function recordCreateTableForeignKeys(
  createStmt: { relation?: TableRef; tableElts?: unknown[] } | undefined,
  registry: FkCascadeRegistry,
  resolveTable: ResolveTable
): void {
  const relation = createStmt?.relation
  if (!relation?.relname) return
  const childTable = resolveTable(relation)
  for (const elt of createStmt?.tableElts ?? []) {
    const node = elt as {
      Constraint?: ForeignKeyConstraint
      ColumnDef?: { colname?: string; constraints?: unknown[] }
    }
    // table-level: FOREIGN KEY (a, b) REFERENCES parent (x, y)
    if (node.Constraint?.contype === 'CONSTR_FOREIGN') {
      addForeignKey(
        registry,
        childTable,
        node.Constraint,
        stringValues(node.Constraint.fk_attrs),
        resolveTable
      )
      continue
    }
    // inline column: col … REFERENCES parent (x)
    const col = node.ColumnDef
    if (col?.colname && Array.isArray(col.constraints)) {
      for (const c of col.constraints) {
        const constraint = (c as { Constraint?: ForeignKeyConstraint }).Constraint
        if (constraint?.contype === 'CONSTR_FOREIGN') {
          addForeignKey(registry, childTable, constraint, [col.colname], resolveTable)
        }
      }
    }
  }
}

interface ForeignKeyConstraint {
  contype?: string
  pktable?: TableRef
  fk_attrs?: unknown[]
  pk_attrs?: unknown[]
  fk_del_action?: string
}

function addForeignKey(
  registry: FkCascadeRegistry,
  childTable: string,
  constraint: ForeignKeyConstraint,
  childColumns: string[],
  resolveTable: ResolveTable
): void {
  const onDelete = DELETE_ACTION[constraint.fk_del_action ?? 'a']
  if (!onDelete) return // restrict / no-action / set-default: nothing to expand
  const parent = constraint.pktable
  if (!parent?.relname) return
  const refColumns = stringValues(constraint.pk_attrs)
  // need an explicit referenced column to build the IN-subquery; drizzle always
  // emits one. without it we can't faithfully expand, so skip rather than guess.
  if (childColumns.length === 0 || refColumns.length !== childColumns.length) return
  registry.add(resolveTable(parent), {
    table: childTable,
    columns: childColumns.map(quoteIdent),
    refColumns: refColumns.map(quoteIdent),
    onDelete,
  })
}

function columnTuple(columns: string[]): string {
  return columns.length === 1 ? columns[0] : `(${columns.join(', ')})`
}

/**
 * build the set-based predicate selecting child rows whose FK points at the
 * parent rows matched by `parentPredicate`:
 *   childCols IN (SELECT refCols FROM parentTable WHERE parentPredicate)
 */
function childMatchPredicate(
  child: FkChild,
  parentTable: string,
  parentPredicate: string | null
): string {
  const where = parentPredicate ? ` WHERE ${parentPredicate}` : ''
  const select = `SELECT ${child.refColumns.join(', ')} FROM ${parentTable}${where}`
  return `${columnTuple(child.columns)} IN (${select})`
}

export interface ExpandOptions {
  /** guard against pathological / cyclic graphs. */
  maxDepth?: number
}

/**
 * expand a `DELETE FROM target [WHERE wherePredicate]` into the ordered list of
 * statements that reproduces PG's ON DELETE semantics. children are emitted
 * leaves-first (grandchildren before children) so every delete runs before the
 * rows it depends on disappear; the returned list does NOT include the original
 * parent delete — the caller still runs that, after these.
 *
 * each statement embeds the full original predicate via nested subqueries, so
 * for a parameterized DELETE the SAME bound params apply unchanged to every
 * emitted statement.
 *
 * cascade → `DELETE FROM child WHERE childCols IN (SELECT … )`
 * set-null → `UPDATE child SET childCols = NULL WHERE childCols IN (SELECT … )`
 *            (SET NULL does not delete the child, so its subtree is not recursed)
 */
export function expandDelete(
  target: string,
  wherePredicate: string | null,
  registry: FkCascadeRegistry,
  options: ExpandOptions = {}
): string[] {
  if (!registry.hasEdges) return []
  const maxDepth = options.maxDepth ?? 32
  const out: string[] = []

  const visit = (
    table: string,
    predicate: string | null,
    depth: number,
    path: ReadonlySet<string>
  ): void => {
    if (depth > maxDepth) return
    for (const child of registry.childrenOf(table)) {
      if (path.has(child.table)) continue // cycle guard — self-ref / mutual FK
      const match = childMatchPredicate(child, table, predicate)
      if (child.onDelete === 'cascade') {
        // delete grandchildren first, keyed off the rows this child is about to lose
        visit(child.table, match, depth + 1, new Set(path).add(child.table))
        out.push(`DELETE FROM ${child.table} WHERE ${match}`)
      } else {
        // set-null: null the link, leave the child row (and its subtree) intact
        const assignments = child.columns.map((c) => `${c} = NULL`).join(', ')
        out.push(`UPDATE ${child.table} SET ${assignments} WHERE ${match}`)
      }
    }
  }

  visit(target, wherePredicate, 0, new Set([target]))
  return out
}
