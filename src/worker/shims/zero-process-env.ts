type PropertySnapshot = {
  hadValue: boolean
  key: PropertyKey
  previousValue: unknown
  target: Record<PropertyKey, unknown>
  value: unknown
}

function installIfMissing(
  snapshots: PropertySnapshot[],
  target: Record<PropertyKey, unknown>,
  key: PropertyKey,
  value: unknown
): void {
  if (target[key] !== undefined) return
  snapshots.push({
    hadValue: Object.prototype.hasOwnProperty.call(target, key),
    key,
    previousValue: target[key],
    target,
    value,
  })
  target[key] = value
}

function installValue(
  snapshots: PropertySnapshot[],
  target: Record<PropertyKey, unknown>,
  key: PropertyKey,
  value: unknown
): void {
  snapshots.push({
    hadValue: Object.prototype.hasOwnProperty.call(target, key),
    key,
    previousValue: target[key],
    target,
    value,
  })
  target[key] = value
}

function prepareProcess(snapshots: PropertySnapshot[]): void {
  const globalRecord = globalThis as Record<PropertyKey, unknown>
  installIfMissing(snapshots, globalRecord, 'process', {})

  const processRecord = globalRecord.process as Record<PropertyKey, unknown>
  installIfMissing(snapshots, processRecord, 'env', {})
  installIfMissing(snapshots, processRecord, 'pid', 1)
  installIfMissing(snapshots, processRecord, 'argv', [])
  installIfMissing(snapshots, processRecord, 'kill', () => true)
}

function restoreProcess(snapshots: PropertySnapshot[]): void {
  for (let index = snapshots.length - 1; index >= 0; index--) {
    const snapshot = snapshots[index]
    if (snapshot.target[snapshot.key] !== snapshot.value) continue
    if (snapshot.hadValue) snapshot.target[snapshot.key] = snapshot.previousValue
    else delete snapshot.target[snapshot.key]
  }
}

// zero-cache reads the global process object while its static module graph is
// evaluated. prepare it before the adjacent static run-worker import, then hand
// ownership of these bootstrap mutations to the first embed generation.
const bootstrapSnapshots: PropertySnapshot[] = []
prepareProcess(bootstrapSnapshots)
const bootstrapProcess = (globalThis as Record<PropertyKey, unknown>).process as Record<
  PropertyKey,
  unknown
>
const bootstrapEnv = bootstrapProcess.env as Record<PropertyKey, unknown>
installValue(bootstrapSnapshots, bootstrapEnv, 'SINGLE_PROCESS', '1')
installIfMissing(bootstrapSnapshots, bootstrapEnv, 'NODE_ENV', 'development')
let bootstrapAvailable = true
let activeSnapshots: PropertySnapshot[] | null = null
let leaseCount = 0

export function acquireZeroProcessEnv(): () => void {
  if (leaseCount === 0) {
    const snapshots = bootstrapAvailable ? bootstrapSnapshots : []
    bootstrapAvailable = false
    prepareProcess(snapshots)
    const processRecord = (globalThis as Record<PropertyKey, unknown>).process as Record<
      PropertyKey,
      unknown
    >
    const env = processRecord.env as Record<PropertyKey, unknown>
    installValue(snapshots, env, 'SINGLE_PROCESS', '1')
    installIfMissing(snapshots, env, 'NODE_ENV', 'development')
    activeSnapshots = snapshots
  }
  leaseCount++
  let released = false
  return () => {
    if (released) return
    released = true
    leaseCount--
    if (leaseCount !== 0 || !activeSnapshots) return
    const snapshots = activeSnapshots
    activeSnapshots = null
    restoreProcess(snapshots)
  }
}
