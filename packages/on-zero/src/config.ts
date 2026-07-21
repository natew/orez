/**
 * Extra membership for one Zero instance.
 *
 * Most support tables are derived from static `tx.query.<table>` and
 * `tx.mutate.<table>` references. Declare only tables reached through dynamic
 * server code that the generator cannot inspect. Support tables are available
 * to server mutations and change-log mapping, but are never synced to clients.
 */
export type DataInstanceConfig<Scope extends string = string> = {
  /**
   * Column used to partition every synced table in this instance. Omit it for
   * an unscoped instance.
   */
  scope?: Scope
  /**
   * Fileless tables used by server writes when static analysis cannot find the
   * access. These tables enter the generated Zero schema without becoming
   * client query namespaces or synced tables.
   */
  supportTables?: readonly string[]
}

/** Configuration for a multi-instance data directory. */
export type DataConfig<
  Instances extends Record<string, DataInstanceConfig> = Record<
    string,
    DataInstanceConfig
  >,
> = {
  /**
   * Every Zero instance in this data directory, keyed by its generated client
   * name. Each key maps to the sibling data folder with the same name.
   */
  instances: Instances
}

/**
 * Defines the multi-instance data layout consumed by `on-zero generate`.
 * Single-instance applications do not need a config file.
 */
export function defineConfig<const Instances extends Record<string, DataInstanceConfig>>(
  config: DataConfig<Instances>
): DataConfig<Instances> {
  return config
}
