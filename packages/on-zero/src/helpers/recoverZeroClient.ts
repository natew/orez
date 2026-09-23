import { globalValue } from './globalValue'

import type { ZeroEventsEmitter, ZeroRecoveryReasonKey } from '../types'
import type { Context, LogLevel, LogSink } from '@rocicorp/logger'
import type { UpdateNeededReason } from '@rocicorp/zero'

// per-reason guard window: the SAME reason re-failing right after its reload is
// surfaced as fatal instead of reload-storming on something the reload can't fix.
const RECOVER_GUARD_MS = 60_000

// if the scheduled reload never actually takes the page down (a consumer's
// scheduleReload deferred it, a native reload no-oped, or reload() threw) the
// module latch would stay set and kill EVERY later recovery for the page's
// life. time it out so recovery can try again — a real reload tears down the
// context long before this fires, so it only matters when the reload didn't
// land.
const RELOAD_LATCH_TIMEOUT_MS = 15_000

// update-needed reasons that mean "new client code landed": every mounted Zero
// instance whose group is stale fires one, and they all ride ONE group
// transition per shared client identity instead of each recovering alone.
const GROUP_TRANSITION_REASONS: ReadonlySet<ZeroRecoveryReasonKey> = new Set([
  'NewClientGroup',
  'VersionNotSupported',
  'SchemaVersionNotSupported',
])

// the fatal local-store-loss signature Zero logs when the IndexedDB it expects
// is gone (evicted, or deleted by another tab).
const LOCAL_STORE_LOST = 'Expected IndexedDB not found'
const SQLITE_ERROR_NAME = 'SqliteError'
const SQLITE_STATEMENT_FINALIZED = 'This statement has been finalized'
const STORE_CLOSED = 'Store is closed'
const STORE_CLOSED_REPEAT_MIN_MS = 2_000
const STORE_CLOSED_REPEAT_MAX_MS = 60_000

export type ZeroRecoveryLogClassification = {
  reasonKey: Exclude<
    ZeroRecoveryReasonKey,
    | 'NewClientGroup'
    | 'VersionNotSupported'
    | 'SchemaVersionNotSupported'
    | 'client-state-not-found'
  >
  message: string
  dropLocalState: boolean
}

export type ZeroLogPattern = string | RegExp

// a minimal synchronous key/value store the cross-reload guard persists into.
// web defaults to sessionStorage; native (Hermes) has none, so a consumer can
// inject an MMKV/sqlite-backed store to get cross-reload loop protection — the
// in-memory guard already covers within-a-page-load.
export type RecoveryGuardStorage = {
  getItem: (key: string) => string | null
  setItem: (key: string, value: string) => void
  removeItem?: (key: string) => void
}

// what a scheduleReload consumer receives so it can gate/annotate the reload
// (soot: IDE-active gate + countdown toast + background-mutation fence; mobile:
// expo-updates reload) while still driving the SAME deletes-then-reload work.
export type ScheduleReloadContext = {
  reason: string
  reasonKey: ZeroRecoveryReasonKey
  dropLocalState: boolean
  // deletes every affected instance's local store, awaits beforeReload, then
  // reloads. idempotent — safe to call once the consumer decides to proceed.
  performReload: () => Promise<void>
}

export type ZeroRecoveryDeps = {
  // delete THIS client's local persistent state (its own scoped IDB). pass the
  // instance's `.delete`. recovery is per-instance, so a combined client drops
  // EVERY underlying store; the latch below keeps it to one reload.
  deleteLocalState: () => Promise<unknown>
  // recovery lifecycle for consumers (preview postMessage, toast, …).
  zeroEvents: ZeroEventsEmitter
  // awaited before the reload — e.g. wait for the dev origin to come back so the
  // reload doesn't land on a restarting server. optional.
  beforeReload?: () => Promise<void>
  // take over WHEN/HOW the recovery reload happens. default: immediate guarded
  // reload. the consumer decides (defer until safe, show a countdown, reload
  // natively) but still calls ctx.performReload to run the real work.
  scheduleReload?: (ctx: ScheduleReloadContext) => void
  // cross-reload guard backing store (defaults to sessionStorage on web). inject
  // a native KV so Hermes gets real cross-reload loop protection.
  guardStorage?: RecoveryGuardStorage
  // classified transport/app messages that are expected and must not recover.
  benignLogPatterns?: readonly ZeroLogPattern[]
  // internal lifecycle fence. recovery invalidates queued and in-flight work
  // before the consumer can defer the reload.
  onRecovery?: (reasonKey: ZeroRecoveryReasonKey) => void
  // non-DOM hosts cannot reload a page. reconstruct the mounted client in
  // place after the same delete/beforeReload work completes.
  recoverInPlace?: () => Promise<boolean>
  // injectable for tests; defaults to a real page reload.
  reload?: () => void | boolean
  // shared client identity for group-transition coalescing (the Zero userID):
  // sibling instances with the same identity ride one transition. defaults to
  // '' (one transition per document for every mounted instance).
  clientIdentity?: string
  // read this instance's Zero client group ID. instances share one ID if and
  // only if they share name, mutators, indexes, and schema version, so a
  // post-recovery failure proves by comparison whether the recovery actually
  // landed new code. without it the transition falls back to a one-retry
  // budget before going terminal.
  getGroupID?: () => Promise<string> | string
  // wait for the reminted replacement to connect. in-place path only: the
  // transition resolves only when every reminted participant reports
  // connected. defaults to resolving immediately.
  awaitReconnected?: () => Promise<boolean>
}

// one participant of an in-progress group transition: a mounted Zero instance
// (identified by its recovery closures, which are stable per instance) that
// signaled while the transition was open.
type TransitionParticipant = {
  deps: ZeroRecoveryDeps
  // whether this participant's stale store must be dropped before recovery.
  // SchemaVersionNotSupported sets it; NewClientGroup never does (its data
  // is still valid, and the store is shared with sibling tabs).
  dropLocalState: boolean
  deleteExecuted: boolean
  remintExecuted: boolean
  reminted: boolean
  skipped: boolean
  // the instance's group when it joined (undefined when unreadable).
  oldGroupID: string | undefined
  wrappedRemint: (() => Promise<boolean>) | null
}

type GroupTransition = {
  key: string
  reasons: Set<ZeroRecoveryReasonKey>
  participants: Map<object, TransitionParticipant>
  // every instance constructed after this sequence number belongs to the new
  // code: if one of those fires while the transition is still open, the
  // recovery did not fix it (or even newer code superseded it).
  startSeq: number
  startedAtMs: number
  // set when a reload run gathers this transition's work: joins after that
  // run their delete/remint immediately instead of waiting for a next run.
  remintsExecuted: boolean
  pendingWaits: number
  settled: boolean
  outcome: 'completed' | 'terminal' | 'reloading' | 'superseded' | null
  // releases connected-waits when the transition settles another way
  // (terminal, superseded): without this a settle during a wait would leave
  // the reload run awaiting forever and wedge the latch with it.
  settledPromise: Promise<void>
  notifySettled: () => void
}

type TerminalStamp = {
  t: number
  groups: string[]
}

// everything recovery coordinates through, shared across dual-loaded module
// copies (cjs/esm) via globalValue. before, each copy kept its own reload
// latch and guard: N copies scheduled N reloads and the second copy's guard
// check went fatal while the first copy's reload was still pending. now one
// transition, one latch, and one guard serve the whole realm.
type RecoverySharedState = {
  // one reload per page-load. the local-state deletes that must precede it are
  // collected so EVERY affected instance drops its own store before we reload.
  // they are thunks (not started promises) so a deferred reload can't leave the
  // app running on an already-deleted store: the deletes run only when the
  // reload actually proceeds. keyed by owner so the same instance never queues
  // its delete twice and a group transition can take a delete over.
  reloadScheduled: boolean
  reloadInProgress: boolean
  reloadLatchTimer: ReturnType<typeof setTimeout> | undefined
  pendingDeletes: Array<{ owner: object; run: () => Promise<unknown> }>
  pendingInPlaceRecoveries: Array<() => Promise<boolean>>
  // within-a-page-load per-reason guard for the non-transition reasons: real
  // loop protection everywhere, including Hermes (no storage needed). a reload
  // wipes it, which is why the injectable storage below survives across reloads.
  inMemoryGuard: Map<string, number>
  constructionSeq: number
  constructionSeqByDeps: WeakMap<object, number>
  groupByDeps: WeakMap<object, string>
  transitions: Map<string, GroupTransition>
  terminalMemory: Map<string, TerminalStamp>
  terminalEmitted: Map<string, number>
  unknownPostSignals: Map<string, { count: number; windowStartMs: number }>
  lastStoreClosedAtMs: number
}

function sharedRecoveryState(): RecoverySharedState {
  return globalValue<RecoverySharedState>('on-zero:recovery-shared', () => ({
    reloadScheduled: false,
    reloadInProgress: false,
    reloadLatchTimer: undefined,
    pendingDeletes: [],
    pendingInPlaceRecoveries: [],
    inMemoryGuard: new Map(),
    constructionSeq: 0,
    constructionSeqByDeps: new WeakMap(),
    groupByDeps: new WeakMap(),
    transitions: new Map(),
    terminalMemory: new Map(),
    terminalEmitted: new Map(),
    unknownPostSignals: new Map(),
    lastStoreClosedAtMs: 0,
  }))
}

function documentScope(): string {
  try {
    return globalThis.location?.href ?? ''
  } catch {
    return ''
  }
}

function guardStorageKey(reasonKey: ZeroRecoveryReasonKey): string {
  return `on-zero-recover-${documentScope()}-${reasonKey}`
}

function defaultGuardStorage(): RecoveryGuardStorage | undefined {
  try {
    if (typeof window === 'undefined' || !window.sessionStorage) return undefined
    return window.sessionStorage
  } catch {
    // sessionStorage access throws in some sandboxes / Hermes — no web store.
    return undefined
  }
}

// cross-page-load, per-reason guard for the NON-transition reasons: distinct
// keys mean a client-state-not-found recovery never suppresses a later
// desync one. the in-memory tier catches re-fires within a page-load (works
// on Hermes); the storage tier survives the reload, so a reason that reloaded
// then immediately re-fires is caught as a genuine fatal instead of
// reload-storming across loads. group-transition reasons use the richer
// markers below instead (they must tell "the reload didn't help" apart from
// "newer code landed since").
function recoveryGuardOpen(
  reasonKey: ZeroRecoveryReasonKey,
  guardStorage: RecoveryGuardStorage | undefined
): boolean {
  // sessionStorage is shared by same-origin frames in one top-level tab. scope
  // its guard to the current document so one preview frame recovering does not
  // falsely fatal a sibling frame. the URL survives a reload, preserving the
  // cross-reload loop guard for the document that actually recovered.
  const key = guardStorageKey(reasonKey)
  const now = Date.now()

  const memLast = sharedRecoveryState().inMemoryGuard.get(key) ?? 0
  if (memLast > 0 && now - memLast < RECOVER_GUARD_MS) return false

  const storage = guardStorage ?? defaultGuardStorage()
  if (storage) {
    try {
      const rawLast = storage.getItem(key)
      const last = rawLast ? Number(rawLast) : 0
      if (last > 0 && now - last < RECOVER_GUARD_MS) return false
      storage.setItem(key, String(now))
    } catch {
      // storage unavailable mid-flight — the in-memory tier is the floor.
    }
  }

  sharedRecoveryState().inMemoryGuard.set(key, now)
  return true
}

// what a group transition persists across a reload: when it fired, which
// client groups it recovered from, and whether it ended terminal. legacy
// writers stored a bare timestamp; those read back as { t } with unknown
// groups and keep the legacy conservative behavior.
type RecoveryMarker = {
  t: number
  groups?: string[]
  terminal?: boolean
  msg?: string
}

function readRecoveryMarker(
  reasonKey: ZeroRecoveryReasonKey,
  guardStorage: RecoveryGuardStorage | undefined
): RecoveryMarker | null {
  const storage = guardStorage ?? defaultGuardStorage()
  if (!storage) return null
  let raw: string | null = null
  try {
    raw = storage.getItem(guardStorageKey(reasonKey))
  } catch {
    return null
  }
  if (!raw) return null
  if (/^\d+$/.test(raw)) return { t: Number(raw) }
  try {
    const parsed = JSON.parse(raw) as RecoveryMarker
    if (!parsed || typeof parsed.t !== 'number') return null
    return parsed
  } catch {
    return null
  }
}

function writeRecoveryMarker(
  reasonKey: ZeroRecoveryReasonKey,
  marker: RecoveryMarker,
  guardStorage: RecoveryGuardStorage | undefined
): void {
  const storage = guardStorage ?? defaultGuardStorage()
  if (!storage) return
  try {
    storage.setItem(guardStorageKey(reasonKey), JSON.stringify(marker))
  } catch {
    // storage unavailable mid-flight — the in-memory tier is the floor.
  }
}

function clearRecoveryMarker(
  reasonKey: ZeroRecoveryReasonKey,
  guardStorage: RecoveryGuardStorage | undefined
): void {
  const storage = guardStorage ?? defaultGuardStorage()
  if (!storage) return
  try {
    if (typeof storage.removeItem === 'function') {
      storage.removeItem(guardStorageKey(reasonKey))
    } else {
      storage.setItem(guardStorageKey(reasonKey), JSON.stringify({ t: 0 }))
    }
  } catch {
    // storage unavailable mid-flight — the next read treats it as stale.
  }
}

function armReloadLatchTimeout() {
  const shared = sharedRecoveryState()
  if (typeof setTimeout !== 'function') return
  if (shared.reloadLatchTimer) clearTimeout(shared.reloadLatchTimer)
  shared.reloadLatchTimer = setTimeout(() => {
    shared.reloadLatchTimer = undefined
    // re-open SCHEDULING only. a reload that never landed (still deferred behind
    // a consumer's IDE gate / countdown, which can hold for minutes) must not
    // kill future recovery — but the deferred performReload still owns the
    // pending delete thunks and, if it is already mid-flight (a slow
    // beforeReload can exceed this timeout), reloadInProgress must stay set so a
    // second recovery can't double-drive the reload. so touch neither
    // pendingDeletes nor reloadInProgress here.
    shared.reloadScheduled = false
  }, RELOAD_LATCH_TIMEOUT_MS)
  // don't hold the event loop open (node/native) waiting on this defense timer.
  if (
    shared.reloadLatchTimer &&
    typeof shared.reloadLatchTimer === 'object' &&
    'unref' in shared.reloadLatchTimer
  ) {
    shared.reloadLatchTimer.unref()
  }
}

function disarmReloadLatchTimeout() {
  const shared = sharedRecoveryState()
  if (shared.reloadLatchTimer) {
    clearTimeout(shared.reloadLatchTimer)
    shared.reloadLatchTimer = undefined
  }
}

function pushGlobalDelete(deps: ZeroRecoveryDeps): void {
  const shared = sharedRecoveryState()
  if (shared.pendingDeletes.some((entry) => entry.owner === deps.deleteLocalState)) {
    return
  }
  shared.pendingDeletes.push({
    owner: deps.deleteLocalState,
    run: () => Promise.resolve().then(deps.deleteLocalState),
  })
}

function removeGlobalDelete(owner: object): void {
  const pending = sharedRecoveryState().pendingDeletes
  const index = pending.findIndex((entry) => entry.owner === owner)
  if (index !== -1) pending.splice(index, 1)
}

function removeGlobalRemint(fn: (() => Promise<boolean>) | undefined): void {
  if (!fn) return
  const pending = sharedRecoveryState().pendingInPlaceRecoveries
  const index = pending.indexOf(fn)
  if (index !== -1) pending.splice(index, 1)
}

// the construction sequence number tells pre-transition instances (built
// before the transition started: they join it) from post-transition ones
// (built by the transition's own remints: if one of those fires, the recovery
// did not fix it). stamped in makeZeroRecovery, which runs once per instance.
function seqOf(deps: ZeroRecoveryDeps): number {
  const shared = sharedRecoveryState()
  let seq = shared.constructionSeqByDeps.get(deps)
  if (seq === undefined) {
    seq = ++shared.constructionSeq
    shared.constructionSeqByDeps.set(deps, seq)
  }
  return seq
}

function refreshGroupCache(deps: ZeroRecoveryDeps): void {
  if (!deps.getGroupID) return
  let result: Promise<string> | string
  try {
    result = deps.getGroupID()
  } catch {
    return
  }
  if (typeof result === 'string') {
    if (result) sharedRecoveryState().groupByDeps.set(deps, result)
    return
  }
  void Promise.resolve(result).then(
    (id) => {
      if (typeof id === 'string' && id) sharedRecoveryState().groupByDeps.set(deps, id)
    },
    () => {}
  )
}

function cachedGroupID(deps: ZeroRecoveryDeps): string | undefined {
  return sharedRecoveryState().groupByDeps.get(deps)
}

// prime the group cache right after construction so the synchronous read at
// signal time hits: signals always fire post-open, long after this resolves.
export function primeGroupCache(deps: ZeroRecoveryDeps): void {
  refreshGroupCache(deps)
}

function participantKey(deps: ZeroRecoveryDeps): object {
  return deps.recoverInPlace ?? deps.deleteLocalState
}

function transitionKey(deps: ZeroRecoveryDeps): string {
  return `${documentScope()}\0${deps.clientIdentity ?? ''}`
}

function findOpenTransitionParticipant(
  deps: ZeroRecoveryDeps
): TransitionParticipant | undefined {
  const transition = sharedRecoveryState().transitions.get(transitionKey(deps))
  if (!transition || transition.settled) return undefined
  // a post-transition instance is never a participant of the open transition.
  if (seqOf(deps) > transition.startSeq) return undefined
  return transition.participants.get(participantKey(deps))
}

// true while any group transition is in flight. remint() consults this: a
// transition owns the retry policy (one remint per provider per transition,
// terminal-on-failure), so its remints bypass the cooldown/attempt budget and
// rapid legitimate transitions all land.
export function isGroupTransitionOpen(): boolean {
  return sharedRecoveryState().transitions.size > 0
}

function terminalKey(key: string, reasonKey: ZeroRecoveryReasonKey): string {
  return `${key}\0${reasonKey}`
}

function stampTerminal(
  key: string,
  reasonKey: ZeroRecoveryReasonKey,
  guardStorage: RecoveryGuardStorage | undefined,
  message: string,
  groups: string[]
): void {
  sharedRecoveryState().terminalMemory.set(terminalKey(key, reasonKey), {
    t: Date.now(),
    groups,
  })
  writeRecoveryMarker(
    reasonKey,
    { t: Date.now(), terminal: true, groups, msg: message },
    guardStorage
  )
}

// the one terminal error a failed transition emits: a single console error
// carrying the old/new group IDs, one fatal event, and one host event — never
// a cascade, never a loop. re-emits after the window expires so a failure an
// hour later still surfaces.
function emitTerminalOnce(
  deps: ZeroRecoveryDeps,
  key: string,
  reasonKey: ZeroRecoveryReasonKey,
  message: string
): void {
  const shared = sharedRecoveryState()
  const now = Date.now()
  const emitKey = terminalKey(key, reasonKey)
  const lastEmitted = shared.terminalEmitted.get(emitKey) ?? 0
  if (lastEmitted > 0 && now - lastEmitted < RECOVER_GUARD_MS) return
  shared.terminalEmitted.set(emitKey, now)
  try {
    console.error(`[on-zero] ${message}`)
    deps.zeroEvents.emit({ type: 'fatal', reasonKey, reason: message })
  } catch {}
  // hosts embedding this app (preview shells, test harnesses) observe the
  // terminal state without sharing the module instance: a plain event on the
  // realm's global scope.
  try {
    globalThis.dispatchEvent?.(
      new CustomEvent('on-zero-fatal', { detail: { reasonKey, reason: message } })
    )
  } catch {}
}

function settleTransition(
  transition: GroupTransition,
  outcome: GroupTransition['outcome']
): void {
  if (transition.settled) return
  transition.settled = true
  transition.outcome = outcome
  sharedRecoveryState().transitions.delete(transition.key)
  transition.notifySettled()
}

function checkTransitionComplete(transition: GroupTransition): void {
  if (transition.settled) return
  for (const participant of transition.participants.values()) {
    if (!participant.reminted && !participant.skipped) return
  }
  if (transition.pendingWaits > 0) return
  settleTransition(transition, 'completed')
}

function failTransition(
  transition: GroupTransition,
  input: {
    reasonKey: ZeroRecoveryReasonKey
    message: string
    signalDeps: ZeroRecoveryDeps
  }
): void {
  if (transition.settled) return
  const groups = [
    ...new Set(
      [...transition.participants.values()]
        .map((participant) => participant.oldGroupID)
        .filter((group): group is string => typeof group === 'string')
    ),
  ]
  settleTransition(transition, 'terminal')
  // the transition failed as a whole: stamp every reason it carried plus the
  // signal's own, so a re-fire on any of them stays silent this window.
  for (const reasonKey of new Set([...transition.reasons, input.reasonKey])) {
    stampTerminal(
      transition.key,
      reasonKey,
      input.signalDeps.guardStorage,
      input.message,
      groups
    )
  }
  emitTerminalOnce(input.signalDeps, transition.key, input.reasonKey, input.message)
}

function makeWrappedRemint(
  transition: GroupTransition,
  participant: TransitionParticipant
): () => Promise<boolean> {
  return async () => {
    if (participant.skipped || participant.reminted) return true
    const remint = participant.deps.recoverInPlace
    if (!remint) {
      participant.skipped = true
      checkTransitionComplete(transition)
      return false
    }
    let ok: boolean
    try {
      ok = await remint()
    } catch (error) {
      // the recovery itself is broken — fail loud once rather than spin.
      const [firstReason] = transition.reasons
      failTransition(transition, {
        reasonKey: firstReason ?? 'NewClientGroup',
        message: `in-place recovery threw (${error instanceof Error ? error.message : String(error)})`,
        signalDeps: participant.deps,
      })
      return false
    }
    // false means "nothing to reconstruct" (the provider unmounted
    // mid-transition): skip the connected wait, don't fail the transition.
    if (!ok) {
      participant.skipped = true
      checkTransitionComplete(transition)
      return false
    }
    participant.reminted = true
    const awaitReconnected = participant.deps.awaitReconnected
    if (awaitReconnected) {
      transition.pendingWaits += 1
      try {
        // released early when the transition settles another way so the
        // reload run awaiting this remint can finish and reopen the latch.
        await Promise.race([awaitReconnected(), transition.settledPromise])
      } catch {
        // a wait that rejects is over either way; the transition resolves
        // from whoever is left, and a still-stale instance re-fires into a
        // new transition.
      } finally {
        transition.pendingWaits -= 1
      }
    }
    checkTransitionComplete(transition)
    return true
  }
}

async function runParticipantImmediate(
  transition: GroupTransition,
  participant: TransitionParticipant
): Promise<void> {
  // claim both flags synchronously: a reload run gathering in a later
  // microtask must skip this participant, never double-run it.
  const doDelete = participant.dropLocalState && !participant.deleteExecuted
  const doRemint = !participant.remintExecuted
  participant.deleteExecuted = true
  participant.remintExecuted = true
  if (doDelete) {
    try {
      await participant.deps.deleteLocalState()
    } catch {}
  }
  if (doRemint) {
    if (participant.wrappedRemint) await participant.wrappedRemint()
    else {
      participant.skipped = true
      checkTransitionComplete(transition)
    }
  }
}

function joinTransition(
  transition: GroupTransition,
  deps: ZeroRecoveryDeps,
  reasonKey: ZeroRecoveryReasonKey,
  dropLocalState: boolean
): void {
  transition.reasons.add(reasonKey)
  const key = participantKey(deps)
  const existing = transition.participants.get(key)
  if (existing) {
    // same instance firing again (or for a second reason) while the
    // transition is open: dedupe, folding a stricter drop need in.
    if (dropLocalState && !existing.dropLocalState) {
      existing.dropLocalState = true
      // the delete already ran without the drop: queue it for the next run
      // rather than strand it.
      if (existing.deleteExecuted) pushGlobalDelete(deps)
    }
    return
  }
  // this instance's recovery now belongs to the transition: take over any
  // globally-queued work so it can neither double-run nor strand.
  removeGlobalDelete(deps.deleteLocalState)
  removeGlobalRemint(deps.recoverInPlace)
  const participant: TransitionParticipant = {
    deps,
    dropLocalState,
    deleteExecuted: false,
    remintExecuted: false,
    reminted: false,
    skipped: false,
    oldGroupID: cachedGroupID(deps),
    wrappedRemint: null,
  }
  participant.wrappedRemint = makeWrappedRemint(transition, participant)
  transition.participants.set(key, participant)
  // joins stay silent (the transition's one recovering already fired), but a
  // join after the reload run gathered must execute now — the run won't come
  // back for it.
  if (transition.remintsExecuted) void runParticipantImmediate(transition, participant)
}

function startTransition(
  deps: ZeroRecoveryDeps,
  reasonKey: ZeroRecoveryReasonKey,
  message: string,
  dropLocalState: boolean
): void {
  const shared = sharedRecoveryState()
  const key = transitionKey(deps)
  let notifySettled!: () => void
  const settledPromise = new Promise<void>((resolve) => {
    notifySettled = resolve
  })
  const transition: GroupTransition = {
    key,
    reasons: new Set([reasonKey]),
    participants: new Map(),
    startSeq: shared.constructionSeq,
    startedAtMs: Date.now(),
    remintsExecuted: false,
    pendingWaits: 0,
    settled: false,
    outcome: null,
    settledPromise,
    notifySettled,
  }
  shared.transitions.set(key, transition)
  joinTransition(transition, deps, reasonKey, dropLocalState)
  console.warn(`[on-zero] ${message} — recovering`)
  deps.zeroEvents.emit({ type: 'recovering', reasonKey, reason: message })

  // a pending (not yet running) reload gathers this transition's work when it
  // runs: piggyback on it. a mid-flight reload already gathered: run ours
  // immediately instead of stranding it.
  if (shared.reloadScheduled && !shared.reloadInProgress) return
  if (shared.reloadInProgress) {
    transition.remintsExecuted = true
    for (const participant of transition.participants.values()) {
      void runParticipantImmediate(transition, participant)
    }
    return
  }
  shared.reloadScheduled = true
  armReloadLatchTimeout()

  const runReload = () => performReload(deps)
  if (deps.scheduleReload) {
    deps.scheduleReload({
      reason: message,
      reasonKey,
      dropLocalState,
      performReload: runReload,
    })
  } else {
    void runReload()
  }
}

// no open transition: decide whether this failure may start one. signals that
// arrive while a transition is in flight join it and never touch the guard —
// only a failure after a completed transition consumes it. verifiable progress
// (the firing instance's group differs from every group on record) always
// starts a new transition, so rapid legitimate changes all land; an unchanged
// group after a reload, or a repeated failure with unreadable groups, goes
// terminal once instead of looping.
function checkTransitionGuard(
  deps: ZeroRecoveryDeps,
  reasonKey: ZeroRecoveryReasonKey
): 'allow' | 'silent' {
  const shared = sharedRecoveryState()
  const key = transitionKey(deps)
  const now = Date.now()
  const myGroup = cachedGroupID(deps)

  const memory = shared.terminalMemory.get(terminalKey(key, reasonKey))
  if (memory && memory.t > 0 && now - memory.t < RECOVER_GUARD_MS) {
    if (
      myGroup !== undefined &&
      memory.groups.length > 0 &&
      !memory.groups.includes(myGroup)
    ) {
      // newer code landed since the terminal: retry.
      shared.terminalMemory.delete(terminalKey(key, reasonKey))
    } else {
      return 'silent'
    }
  }

  const marker = readRecoveryMarker(reasonKey, deps.guardStorage)
  if (marker && marker.t > 0 && now - marker.t < RECOVER_GUARD_MS) {
    const markerGroups =
      marker.groups && marker.groups.length > 0 ? marker.groups : undefined
    if (
      myGroup !== undefined &&
      markerGroups !== undefined &&
      !markerGroups.includes(myGroup)
    ) {
      // the firing instance already runs newer code than the recorded
      // recovery did: clear the marker and start a fresh transition.
      clearRecoveryMarker(reasonKey, deps.guardStorage)
      return 'allow'
    }
    if (marker.terminal) {
      emitTerminalOnce(
        deps,
        key,
        reasonKey,
        marker.msg ??
          `update needed (${reasonKey}) — recovery already failed, not retrying`
      )
      return 'silent'
    }
    if (
      myGroup !== undefined &&
      markerGroups !== undefined &&
      markerGroups.includes(myGroup)
    ) {
      // the recorded reload did not change the client group: the reload
      // can't fix this, so go terminal once with the IDs.
      const message = `update needed (${reasonKey}) — client group ${myGroup} → ${myGroup} (unchanged after reload), not reloading`
      stampTerminal(key, reasonKey, deps.guardStorage, message, markerGroups)
      emitTerminalOnce(deps, key, reasonKey, message)
      return 'silent'
    }
    // unreadable groups plus a fresh reload marker: stay conservative
    // (legacy parity) and go terminal once rather than risk a reload loop.
    const message = `update needed (${reasonKey}) — already recovered once, not reloading`
    stampTerminal(key, reasonKey, deps.guardStorage, message, markerGroups ?? [])
    emitTerminalOnce(deps, key, reasonKey, message)
    return 'silent'
  }

  return 'allow'
}

// an instance built by the open transition's own remints is firing: the
// recovery did not fix it, or even newer code superseded it. an unchanged
// group proves the former and fails the transition terminally; a changed
// group proves the latter and supersedes into a fresh transition.
function handlePostTransitionSignal(
  transition: GroupTransition,
  deps: ZeroRecoveryDeps,
  reasonKey: ZeroRecoveryReasonKey,
  message: string,
  dropLocalState: boolean
): void {
  const shared = sharedRecoveryState()
  const newGroup = cachedGroupID(deps)
  const oldGroups = [
    ...new Set(
      [...transition.participants.values()]
        .map((participant) => participant.oldGroupID)
        .filter((group): group is string => typeof group === 'string')
    ),
  ]
  if (newGroup !== undefined && oldGroups.length > 0) {
    if (!oldGroups.includes(newGroup)) {
      // verifiably newer code: the transition's target is obsolete, not
      // failed. supersede it — no guard consumed — and start fresh.
      settleTransition(transition, 'superseded')
      startTransition(deps, reasonKey, message, dropLocalState)
      return
    }
    failTransition(transition, {
      reasonKey,
      message: `${message} — client group ${newGroup} did not change after recovery (still ${oldGroups.join(', ')}), not retrying`,
      signalDeps: deps,
    })
    return
  }
  // groups unreadable: allow one supersede per window (the overlapping-edit
  // case), then go terminal rather than remint-loop.
  const now = Date.now()
  const budget = shared.unknownPostSignals.get(transition.key)
  const freshBudget =
    budget && now - budget.windowStartMs < RECOVER_GUARD_MS ? budget : null
  if (freshBudget && freshBudget.count >= 1) {
    failTransition(transition, {
      reasonKey,
      message: `${message} — recovery did not resolve the client group, not retrying`,
      signalDeps: deps,
    })
    return
  }
  shared.unknownPostSignals.set(transition.key, {
    count: (freshBudget?.count ?? 0) + 1,
    windowStartMs: freshBudget?.windowStartMs ?? now,
  })
  settleTransition(transition, 'superseded')
  startTransition(deps, reasonKey, message, dropLocalState)
}

function recoverViaGroupTransition(
  deps: ZeroRecoveryDeps,
  reasonKey: ZeroRecoveryReasonKey,
  message: string,
  dropLocalState: boolean
): void {
  const key = transitionKey(deps)
  const open = sharedRecoveryState().transitions.get(key)
  if (open && !open.settled) {
    if (seqOf(deps) > open.startSeq) {
      handlePostTransitionSignal(open, deps, reasonKey, message, dropLocalState)
      return
    }
    joinTransition(open, deps, reasonKey, dropLocalState)
    return
  }
  if (checkTransitionGuard(deps, reasonKey) !== 'allow') return
  startTransition(deps, reasonKey, message, dropLocalState)
}

// deletes every collected instance's local store, awaits beforeReload, then
// reloads. deferred a microtask so sibling instances failing in the same tick
// enqueue their deletes first. idempotent: a consumer that calls performReload
// twice (or after the latch timeout) only runs one reload.
function performReload(deps: ZeroRecoveryDeps): Promise<void> {
  const shared = sharedRecoveryState()
  if (shared.reloadInProgress) return Promise.resolve()
  shared.reloadInProgress = true
  // the consumer has committed to reloading, so the un-latch timeout has no job
  // now — disarm it before the (possibly slow) delete/beforeReload chain so it
  // can't fire mid-flight and re-open scheduling under an in-progress reload.
  disarmReloadLatchTimeout()
  // resolve the reload exactly as @rocicorp/zero does internally
  // (`getBrowserGlobal('location')?.reload()`, zero.js): read location off the
  // global and optional-chain through it. a non-DOM host with a `window` shim
  // but no real `location` (the sootsim tenant render-worker hides `location`
  // for isolation) reconstructs the client in place after the same cleanup.
  const doReload =
    deps.reload ??
    (() => {
      const reload = globalThis.location?.reload
      if (!reload) return false
      reload.call(globalThis.location)
      return true
    })
  // gather in the first microtask so sibling instances failing in the same
  // tick enqueue first: every synchronous signal runs before any microtask
  // does. a transition that joins after the gather runs its own work
  // immediately instead of waiting for a next run.
  let globalDeletes: Array<{ owner: object; run: () => Promise<unknown> }> = []
  let globalRemints: Array<() => Promise<boolean>> = []
  let openTransitions: GroupTransition[] = []
  const transitionDeletes: Array<() => Promise<unknown>> = []
  const transitionRemints: Array<{
    transition: GroupTransition
    run: () => Promise<boolean>
  }> = []
  return Promise.resolve()
    .then(() => {
      globalDeletes = shared.pendingDeletes.splice(0)
      globalRemints = shared.pendingInPlaceRecoveries.splice(0)
      openTransitions = [...shared.transitions.values()].filter(
        (transition) => !transition.settled
      )
      for (const transition of openTransitions) {
        transition.remintsExecuted = true
        for (const participant of transition.participants.values()) {
          if (participant.dropLocalState && !participant.deleteExecuted) {
            participant.deleteExecuted = true
            const runDelete = participant.deps.deleteLocalState
            transitionDeletes.push(() => Promise.resolve().then(runDelete))
          }
          if (!participant.remintExecuted) {
            participant.remintExecuted = true
            if (participant.wrappedRemint) {
              transitionRemints.push({
                transition,
                run: participant.wrappedRemint,
              })
            } else {
              participant.skipped = true
              checkTransitionComplete(transition)
            }
          }
        }
      }
      return Promise.allSettled([
        ...globalDeletes.map((entry) => entry.run()),
        ...transitionDeletes.map((run) => run()),
      ])
    })
    .then(() => deps.beforeReload?.())
    .catch(() => {})
    .then(async () => {
      let reloadStarted: void | boolean = false
      try {
        reloadStarted = doReload()
      } catch {
        // a throwing reload is an unavailable reload: fall through to the
        // in-place path rather than strand the recovery.
        reloadStarted = false
      }
      if (reloadStarted !== false) {
        noteTransitionsReloading(openTransitions)
        return
      }
      try {
        // a transition may have settled (terminal, superseded) while the
        // deletes/beforeReload ran: skip its gathered remints.
        const liveRemints = transitionRemints.filter(
          ({ transition }) => !transition.settled
        )
        const results = await Promise.all([
          ...globalRemints.map((recover) => recover()),
          ...liveRemints.map(({ run }) => run()),
        ])
        const globalResults = results.slice(0, globalRemints.length)
        if (globalRemints.length > 0 && globalResults.some((recovered) => !recovered)) {
          console.error('[on-zero] recovery could not reload or reconstruct the client')
        }
      } catch (error) {
        console.error('[on-zero] in-place recovery failed', error)
      } finally {
        // a page reload tears down this module, but an in-place recovery keeps
        // it alive. reopen the latch so a later, distinct failure can recover.
        shared.reloadInProgress = false
        shared.reloadScheduled = false
      }
    })
}

// the page is really going down for these transitions: persist which groups
// they recovered from so the reloaded page can tell "the reload didn't help"
// (same group → terminal once) from "newer code landed" (changed group → a
// fresh transition is fine).
function noteTransitionsReloading(transitions: readonly GroupTransition[]): void {
  for (const transition of transitions) {
    if (transition.settled) continue
    const groups = [
      ...new Set(
        [...transition.participants.values()]
          .map((participant) => participant.oldGroupID)
          .filter((group): group is string => typeof group === 'string')
      ),
    ]
    const [firstParticipant] = transition.participants.values()
    for (const reasonKey of transition.reasons) {
      writeRecoveryMarker(
        reasonKey,
        { t: Date.now(), groups },
        firstParticipant?.deps.guardStorage
      )
    }
    settleTransition(transition, 'reloading')
  }
}

function recover(
  deps: ZeroRecoveryDeps,
  reasonKey: ZeroRecoveryReasonKey,
  message: string,
  dropLocalState: boolean
): void {
  if (typeof window === 'undefined') return
  deps.onRecovery?.(reasonKey)
  refreshGroupCache(deps)
  if (GROUP_TRANSITION_REASONS.has(reasonKey)) {
    recoverViaGroupTransition(deps, reasonKey, message, dropLocalState)
    return
  }
  const shared = sharedRecoveryState()
  const participant = findOpenTransitionParticipant(deps)
  if (!participant) {
    // each affected instance drops its OWN stale store, even when a reload is
    // already queued — otherwise a sibling's store survives the reload and
    // fatal-loops on the next boot. the single scheduled reload awaits these.
    if (dropLocalState) {
      pushGlobalDelete(deps)
    }
    if (
      deps.recoverInPlace &&
      !shared.pendingInPlaceRecoveries.includes(deps.recoverInPlace)
    ) {
      shared.pendingInPlaceRecoveries.push(deps.recoverInPlace)
    }
  } else if (dropLocalState && !participant.dropLocalState) {
    // the open transition owns this instance's recovery: fold the stricter
    // drop need into it instead of queueing a second delete.
    participant.dropLocalState = true
    if (participant.deleteExecuted) pushGlobalDelete(deps)
  }
  // only ONE reload per page-load; a later trigger just contributes its delete.
  if (shared.reloadScheduled) return
  if (!recoveryGuardOpen(reasonKey, deps.guardStorage)) {
    console.error(`[on-zero] ${message} — already recovered once, not reloading`)
    deps.zeroEvents.emit({ type: 'fatal', reasonKey, reason: message })
    // hosts embedding this app (preview shells, test harnesses) observe the
    // terminal state without sharing the module instance: a plain event on the
    // realm's global scope.
    try {
      globalThis.dispatchEvent?.(
        new CustomEvent('on-zero-fatal', { detail: { reasonKey, reason: message } })
      )
    } catch {}
    return
  }
  shared.reloadScheduled = true
  armReloadLatchTimeout()
  console.warn(`[on-zero] ${message} — recovering`)
  deps.zeroEvents.emit({ type: 'recovering', reasonKey, reason: message })

  const runReload = () => performReload(deps)
  if (deps.scheduleReload) {
    deps.scheduleReload({
      reason: message,
      reasonKey,
      dropLocalState,
      performReload: runReload,
    })
  } else {
    void runReload()
  }
}

// passing our own onUpdateNeeded/onClientStateNotFound DISABLES Zero's built-in
// reloadWithReason, so these handlers are the ONLY recovery — they must cover
// every reason or the app fatal-blanks forever.
export function makeZeroRecovery(deps: ZeroRecoveryDeps) {
  // stamp the construction order now: post-transition instances (built by the
  // transition's own remints) must read newer than any transition they meet.
  seqOf(deps)
  return {
    onUpdateNeeded(reason: UpdateNeededReason) {
      // every update-needed reason needs new client code → reload. but only
      // SchemaVersionNotSupported means the local rows are now incompatible:
      // mirror Zero's own decision (it disables the client group only for that
      // reason). NewClientGroup / VersionNotSupported are code-version mismatches
      // whose data is still valid — and delete() there would wipe the IndexedDB
      // OTHER live tabs of this user share (the store is keyed by user+storageKey,
      // not per-tab), e.g. the newer tab that triggered NewClientGroup.
      const dropLocalState = reason.type === 'SchemaVersionNotSupported'
      recover(
        deps,
        reason.type,
        `update needed (${reason.message || reason.type})`,
        dropLocalState
      )
    },
    onClientStateNotFound() {
      // local/server sync state is gone or rejected — the store is unusable, so
      // drop it and reload into a fresh client.
      recover(deps, 'client-state-not-found', 'client state not found', true)
    },
  }
}

// preserve Zero's default console output: we MUST install a logSink to watch for
// the corruption signature, but doing so replaces Zero's built-in console sink —
// so when the consumer passes none, mirror Zero's key=value context prefix here
// instead of silently swallowing every log.
function logToConsole(
  level: LogLevel,
  context: Context | undefined,
  ...args: unknown[]
): void {
  const prefix = context
    ? Object.entries(context)
        .map(([key, value]) => (value === undefined ? key : `${key}=${value}`))
        .join(' ')
    : ''
  const method = level === 'debug' ? 'debug' : level
  console[method](...(prefix ? [prefix] : []), ...args)
}

function logArgText(arg: unknown): string {
  if (typeof arg === 'string') return arg
  if (arg instanceof Error) return `${arg.name} ${arg.message} ${arg.stack || ''}`
  if (arg && typeof arg === 'object') {
    const message = 'message' in arg ? (arg as { message?: unknown }).message : undefined
    const name = 'name' in arg ? (arg as { name?: unknown }).name : undefined
    if (typeof message === 'string')
      return `${typeof name === 'string' ? name : ''} ${message}`
  }
  return ''
}

function isBenignStoreClosedLog(text: string): boolean {
  if (/Mutator\s+".*"\s+error on server/i.test(text)) return true
  if (/Mutator\s+".*"\s+app error on client/i.test(text)) return true
  return false
}

// the mutation/connection desync class: the local client group is out of sync
// with the server's last-mutation-id / cookie / client record, so the store is
// unusable and must be dropped + recovered. these surface only through the error
// log, never the structured onClientStateNotFound callback. server acknowledgement
// timeouts enter through the mutation lifecycle instead of string classification;
// expected transport startup messages are declared through benignLogPatterns.
function classifyMutationDesync(
  text: string
): Pick<ZeroRecoveryLogClassification, 'reasonKey' | 'message'> | undefined {
  if (text.includes('sent mutation ID') && text.includes('but expected')) {
    return {
      reasonKey: 'mutation-desync',
      message: 'mutation id desync',
    }
  }
  if (
    text.includes('oooMutation') ||
    text.includes('Server reported an out-of-order mutation')
  ) {
    return {
      reasonKey: 'mutation-desync',
      message: 'out-of-order mutation',
    }
  }
  if (text.includes('already processed')) {
    return {
      reasonKey: 'mutation-desync',
      message: 'mutation already processed',
    }
  }
  if (text.includes('InvalidConnectionRequestLastMutationID')) {
    return {
      reasonKey: 'mutation-desync',
      message: 'invalid connection last mutation id',
    }
  }
  if (text.includes('InvalidConnectionRequestBaseCookie')) {
    return {
      reasonKey: 'connection-cookie-invalid',
      message: 'invalid connection base cookie',
    }
  }
  if (text.includes('ClientNotFound') || text.includes('Client not found')) {
    return {
      reasonKey: 'client-not-found',
      message: 'client not found',
    }
  }
  if (text.includes('connection userID mismatch')) {
    return {
      reasonKey: 'connection-userid-mismatch',
      message: 'connection user id mismatch',
    }
  }
  return undefined
}

export function classifyZeroRecoveryLog(
  level: LogLevel | string,
  args: readonly unknown[],
  nowMs = Date.now()
): ZeroRecoveryLogClassification | undefined {
  if (level !== 'error') return undefined
  const text = args.map(logArgText).join(' ')
  if (text.includes(LOCAL_STORE_LOST)) {
    return {
      reasonKey: 'indexeddb-not-found',
      message: 'local store lost',
      dropLocalState: true,
    }
  }
  if (text.includes(SQLITE_ERROR_NAME) && text.includes(SQLITE_STATEMENT_FINALIZED)) {
    return {
      reasonKey: 'sqlite-statement-finalized',
      message: 'sqlite statement finalized',
      dropLocalState: true,
    }
  }
  const desync = classifyMutationDesync(text)
  if (desync) {
    return { ...desync, dropLocalState: true }
  }
  if (text.includes(STORE_CLOSED) && !isBenignStoreClosedLog(text)) {
    const shared = sharedRecoveryState()
    const prevMs = shared.lastStoreClosedAtMs
    shared.lastStoreClosedAtMs = nowMs
    if (
      prevMs > 0 &&
      nowMs - prevMs >= STORE_CLOSED_REPEAT_MIN_MS &&
      nowMs - prevMs <= STORE_CLOSED_REPEAT_MAX_MS
    ) {
      return {
        reasonKey: 'store-closed-repeat',
        message: 'local store closed repeatedly',
        dropLocalState: true,
      }
    }
  }
  return undefined
}

// watch error-level logs for the local-store-lost / desync signatures (the ones
// Zero surfaces only through the log, not the structured callbacks) and trigger
// the same recovery. forwards to the consumer sink, or to the console when there
// is none. wire this ONLY when the consumer didn't pass their own logSink, so a
// consumer that owns log-based recovery isn't double-fired.
export function composeRecoveryLogSink(
  deps: ZeroRecoveryDeps,
  consumerLogSink?: LogSink
): LogSink {
  const consumerFlush = consumerLogSink?.flush
  return {
    log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
      if (consumerLogSink) consumerLogSink.log(level, context, ...args)
      else logToConsole(level, context, ...args)
      const recovery = classifyZeroRecoveryLog(level, args)
      if (!recovery) return
      const text = args.map(logArgText).join(' ')
      if (deps.benignLogPatterns?.some((pattern) => matchesLogPattern(text, pattern)))
        return
      recover(deps, recovery.reasonKey, recovery.message, recovery.dropLocalState)
    },
    // call through the consumer sink so a class-based sink keeps its `this`,
    // rather than handing Zero a detached method reference.
    flush: consumerFlush ? () => consumerFlush.call(consumerLogSink) : undefined,
  }
}

function matchesLogPattern(message: string, pattern: ZeroLogPattern): boolean {
  if (typeof pattern === 'string') return message.includes(pattern)
  pattern.lastIndex = 0
  return pattern.test(message)
}

// generic Zero stale-poke / stale-cookie signatures: the client's view is behind
// the server's snapshot cookie, which a plain reconnect resolves — this is not a
// fatal store-loss, so ConnectionMonitor reconnects instead of recovering.
export function isRecoverableZeroStalePokeMessage(message: string): boolean {
  return (
    message.includes('Server returned unexpected base cookie during sync') ||
    (message.includes('Received cookie') &&
      message.includes('is < than last snapshot cookie') &&
      message.includes('ignoring client view'))
  )
}

// test-only: the reload latch + pending work + in-memory guard + open
// transitions are in-memory (a real page reload clears them); tests simulate
// that reset between successive "page loads". the injectable/sessionStorage
// guard is NOT cleared here — like a real reload, it survives, which is what
// catches an immediate re-fire. the construction sequence stays monotonic so
// pre/post-transition comparisons keep working across resets.
export function resetRecoveryStateForTests() {
  const shared = sharedRecoveryState()
  shared.reloadScheduled = false
  shared.reloadInProgress = false
  if (shared.reloadLatchTimer) {
    clearTimeout(shared.reloadLatchTimer)
    shared.reloadLatchTimer = undefined
  }
  shared.pendingDeletes.length = 0
  shared.pendingInPlaceRecoveries.length = 0
  shared.lastStoreClosedAtMs = 0
  shared.inMemoryGuard.clear()
  shared.transitions.clear()
  shared.terminalMemory.clear()
  shared.terminalEmitted.clear()
  shared.unknownPostSignals.clear()
}
