type PackageManifest = {
  name: string
  dependencies?: Record<string, string>
  optionalDependencies?: Record<string, string>
}

/**
 * Put published workspace dependencies before their consumers while keeping
 * unrelated packages in their original order. This avoids exposing a package
 * whose exact-version dependency has not been published yet.
 */
export function orderReleasePackages<T extends { pkg: PackageManifest }>(
  packages: T[]
): T[] {
  const byName = new Map(packages.map((item) => [item.pkg.name, item]))
  const ordered: T[] = []
  const visiting = new Set<string>()
  const visited = new Set<string>()

  const visit = (item: T) => {
    const name = item.pkg.name
    if (visited.has(name)) return
    if (visiting.has(name)) throw new Error(`release package dependency cycle at ${name}`)
    visiting.add(name)
    const dependencies = {
      ...item.pkg.dependencies,
      ...item.pkg.optionalDependencies,
    }
    for (const dependency of Object.keys(dependencies)) {
      const workspacePackage = byName.get(dependency)
      if (workspacePackage) visit(workspacePackage)
    }
    visiting.delete(name)
    visited.add(name)
    ordered.push(item)
  }

  for (const item of packages) visit(item)
  return ordered
}
