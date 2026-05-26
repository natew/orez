/**
 * AST traversal helpers tailored to the libpg_query output.
 *
 * The official @pgsql/traverse offers two APIs:
 *
 *   walk()  — uses a runtime schema; recurses into both tag-wrapped nodes
 *             ({SelectStmt:...}) AND unwrapped sub-fields (ColumnDef.typeName
 *             is *not* tag-wrapped — it's a plain object). Good for traversal,
 *             but mutation requires keyPath bookkeeping.
 *
 *   visit() — only recurses into tag-wrapped objects with exactly one key.
 *             Misses non-wrapped sub-fields like typeName.
 *
 * For our compiler passes we want: "visit every node of a given tag, AND
 * also visit unwrapped sub-objects that have a known tag-like field set."
 * The simplest reliable approach is a hand-rolled walk: recurse over every
 * object/array, fire callbacks keyed by either (a) the tag wrapper key, or
 * (b) the parent field-name for in-place node types we want to visit
 * (typeName etc.).
 *
 * Each callback gets `(node, parent, key)` so mutation is local: just
 * reassign `parent[key]` (for tag-wrapped) or mutate `node` in place (for
 * unwrapped sub-fields).
 */

export type NodeCallback = (node: any, parent: any, key: string | number) => void

export interface VisitorMap {
  /**
   * Tag-wrapped nodes: keyed by the wrapper tag name (FuncCall, SelectStmt,
   * SQLValueFunction, etc.). The callback receives the *inner* node data.
   *
   * Also accepts the synthetic tag names for unwrapped sub-fields listed in
   * UNWRAPPED_FIELDS — e.g. `TypeName` fires for the value at any
   * `someParent.typeName` slot.
   */
  [tag: string]: NodeCallback | undefined
}

/**
 * Unwrapped fields: child-key → semantic tag name. When a node has a child
 * field with one of these names whose value is a plain object (no tag wrapper),
 * we fire the corresponding visitor against that value.
 */
const UNWRAPPED_FIELDS: Record<string, string> = {
  // ColumnDef.typeName, TypeCast.typeName, etc. are TypeName nodes, not wrapped
  typeName: 'TypeName',
  // SelectStmt.fromClause[i] is sometimes a wrapped RangeVar but in some
  // sub-positions (FROM-list with single table) it appears unwrapped.
  relation: 'RangeVar',
}

function fireForChild(
  visitors: VisitorMap,
  childKey: string,
  childValue: any,
  parent: any
): void {
  const tag = UNWRAPPED_FIELDS[childKey]
  if (!tag) return
  if (!childValue || typeof childValue !== 'object' || Array.isArray(childValue)) return
  const cb = visitors[tag]
  if (cb) cb(childValue, parent, childKey)
}

/**
 * Walk a node tree firing callbacks. For tag-wrapped nodes ({Tag: data}),
 * the callback fires on `data` (inner) with parent=the wrapper's parent,
 * key=where the wrapper sits. For unwrapped sub-fields (typeName etc.),
 * fires on the object directly with parent=its container, key=field name.
 *
 * Mutation rules:
 *   - tag-wrapped: assign `parent[key] = newWrapper` to replace the node
 *   - unwrapped:   mutate `node` in place (it's already the live object)
 */
export function walkAst(root: any, visitors: VisitorMap): void {
  function recurse(node: any, parent: any, key: string | number): void {
    if (node == null || typeof node !== 'object') return

    if (Array.isArray(node)) {
      for (let i = 0; i < node.length; i++) recurse(node[i], node, i)
      return
    }

    const keys = Object.keys(node)

    // Tag-wrapper case: {Tag: data}
    if (keys.length === 1 && /^[A-Z]/.test(keys[0])) {
      const tag = keys[0]
      const data = node[tag]
      const cb = visitors[tag]
      if (cb) cb(data, parent, key)
      // recurse into the inner data
      if (data && typeof data === 'object') {
        for (const childKey of Object.keys(data)) {
          // fire unwrapped-field visitors BEFORE recursing into the field
          fireForChild(visitors, childKey, data[childKey], data)
          recurse(data[childKey], data, childKey)
        }
      }
      return
    }

    // Non-wrapper object case (already an unwrapped sub-tree). Recurse,
    // firing unwrapped-field visitors as we encounter named child fields.
    for (const childKey of keys) {
      fireForChild(visitors, childKey, node[childKey], node)
      recurse(node[childKey], node, childKey)
    }
  }

  recurse(root, null as any, '' as any)
}
