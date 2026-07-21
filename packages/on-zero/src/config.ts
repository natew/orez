/**
 * Configuration for one independently synchronized Zero instance.
 *
 * An instance owns every namespace discovered directly under `dir`, including
 * tables reached through `related()`. A root instance may contain nested
 * instance directories because namespace discovery does not recurse through
 * another instance's directory.
 */
export type DataInstanceConfig<Scope extends string = string> = {
  /**
   * Directory containing this instance's namespace files and folders.
   * Relative paths are resolved from `on-zero.config.ts`. When omitted, the
   * instance key is used as `./<key>`.
   *
   * Use `dir: '.'` for an explicitly configured root instance. Namespace
   * files may use either `<name>.ts` or `<name>/queries.ts` plus
   * `<name>/mutations.ts`. The directory name itself has no runtime meaning;
   * the config key is the generated instance name.
   */
  dir?: string
  /**
   * Column that partitions every table synchronized by this instance.
   * Generation fails when a namespace table or a table reached through
   * `related()` does not declare this column. Server-only support tables are
   * intentionally exempt because they are never synchronized to clients.
   */
  scope?: Scope
  /**
   * Server-only tables this instance reads or writes that static mutation
   * analysis cannot discover, such as tables reached through dynamic helpers
   * or server actions. These tables are included in schema generation and
   * server push typing, but they do not become client query namespaces or enter
   * this instance's synchronized table set.
   */
  supportTables?: readonly string[]
}

/**
 * Root configuration for a multi-instance on-zero data layout.
 *
 * Keep this in `on-zero.config.ts` at the data root. The file is the single
 * configuration surface for current and future data-generation options.
 * Single-instance applications should omit it and keep namespaces directly in
 * the data root.
 */
export type DataConfig<
  Instances extends Record<string, DataInstanceConfig> = Record<
    string,
    DataInstanceConfig
  >,
> = {
  /**
   * Explicit independently synchronized instances, keyed by their generated
   * client name. Every namespace must live under one of the configured `dir`
   * paths. Each configured directory must exist. The data root may be one of
   * those directories when declared explicitly with `dir: '.'`.
   */
  instances: Instances
}

/**
 * Defines `on-zero.config.ts` with literal instance names, directory paths,
 * scope columns, and support tables preserved for generated types.
 *
 * This function returns its input unchanged. Its purpose is validation,
 * editor documentation, and one stable home for on-zero data configuration.
 */
export function defineConfig<const Instances extends Record<string, DataInstanceConfig>>(
  config: DataConfig<Instances>
): DataConfig<Instances> {
  return config
}
