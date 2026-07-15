import { createHash } from 'node:crypto'

function replaceOnce(
  source: string,
  before: string,
  after: string,
  label: string
): string {
  const start = source.indexOf(before)
  if (start < 0 || source.indexOf(before, start + before.length) >= 0) {
    throw new Error(`Chat data shim shape changed at ${label}`)
  }
  return source.slice(0, start) + after + source.slice(start + before.length)
}

function replaceInRange(
  source: string,
  rangeStart: string,
  rangeEnd: string,
  before: string,
  after: string,
  label: string
): string {
  const start = source.indexOf(rangeStart)
  const end = source.indexOf(rangeEnd, start + rangeStart.length)
  if (start < 0 || end < 0) throw new Error(`Chat data shim class missing at ${label}`)
  const range = source.slice(start, end)
  const next = replaceOnce(range, before, after, label)
  return source.slice(0, start) + next + source.slice(end)
}

const SOURCE_RANGE_START = 'class ZeroSqlDO extends OrezZeroSqlDO {'
const SOURCE_RANGE_END = 'export { ZeroSqlDO, ZeroSqlDO as ZeroDO }'
const CACHE_RANGE_START = 'export class ZeroCacheDO extends DurableObject {'
const CACHE_RANGE_END = '\nfunction doInstanceNameForRequest('

const sourceRequestProfile = `  async fetch(request) {
    const __profileUrl = new URL(request.url)
    this.__profile.setRoute(__profileUrl.pathname)
    if (__profileUrl.pathname === '/__profile_phase') {
      this.__profile.setPhase(__profileUrl.searchParams.get('phase') || 'idle')
      return Response.json({ ok: true })
    }
    if (__profileUrl.pathname === '/__profile_report') {
      return Response.json(this.__profile.report())
    }
    if (__profileUrl.pathname === '/__profile_exec') {
      const body = await request.json()
      const cursor = this.ctx.storage.sql.exec(body.sql, ...(body.params || []))
      return Response.json({ rows: cursor.toArray(), rowsWritten: cursor.rowsWritten || 0 })
    }
`

const cacheRequestProfile = `  async fetch(request) {
    const __profileUrl = new URL(request.url)
    this.__profile.setRoute(__profileUrl.pathname)
    if (__profileUrl.pathname === '/__profile_phase') {
      this.__profile.setPhase(__profileUrl.searchParams.get('phase') || 'idle')
      return Response.json({ ok: true })
    }
    if (__profileUrl.pathname === '/__profile_report') {
      return Response.json(this.__profile.report())
    }
    if (__profileUrl.pathname === '/__profile_state') {
      return Response.json({
        ready: Boolean(this.zeroCache),
        bootPending: Boolean(this.bootDeferred),
        bootAttempts: this.__profileBootAttempts || 0,
        failures: (await this.ctx.storage.get('__chat_boot_failures')) || 0,
        backoffUntil: (await this.ctx.storage.get('__chat_boot_backoff_until')) || 0,
      })
    }
    if (__profileUrl.pathname === '/__profile_stop') {
      await this.ctx.storage.deleteAlarm()
      this.bootDeferred = undefined
      this.ready = undefined
      if (this.zeroCache) {
        const stopping = this.zeroCache
        this.zeroCache = undefined
        await stopping.stop()
      }
      return Response.json({ ok: true })
    }
    if (__profileUrl.pathname === '/__profile_exec') {
      const body = await request.json()
      const cursor = this.ctx.storage.sql.exec(body.sql, ...(body.params || []))
      return Response.json({ rows: cursor.toArray(), rowsWritten: cursor.rowsWritten || 0 })
    }
    if (__profileUrl.pathname === '/__profile_stale_schema_tag') {
      await this.ctx.storage.put('__chat_replica_schema_tag', 'profile-stale')
      return Response.json({ ok: true })
    }
    if (__profileUrl.pathname === '/__profile_drop_replica') {
      return Response.json({ dropped: orezDropReplicaTables(this.ctx.storage.sql) })
    }
    if (__profileUrl.pathname === '/__profile_fail_boots') {
      this.__profileForcedFailures = Number(__profileUrl.searchParams.get('count') || 0)
      return Response.json({ ok: true, count: this.__profileForcedFailures })
    }
    if (__profileUrl.pathname === '/__profile_skip_rank_heal') {
      this.__profileSkipRankHeal = true
      return Response.json({ ok: true })
    }
    if (__profileUrl.pathname === '/__profile_heal_null_rank') {
      await this.healNullReplicaRank(this.env.ZERO_APP_ID || 'zero')
      return Response.json({ ok: true })
    }
`

const profileRouter = `

function __profileForward(request, env, kind) {
  const url = new URL(request.url)
  const match = new RegExp('^/__profile/' + kind + '/([^/]+)(/.*)?$').exec(url.pathname)
  if (!match) return null
  const instance = decodeURIComponent(match[1])
  url.pathname = match[2] || '/'
  const headers = new Headers(request.headers)
  headers.set('x-chat-do-instance', instance)
  const forwarded = new Request(url.toString(), {
    method: request.method,
    headers,
    body: request.body,
    redirect: request.redirect,
  })
  const namespace = kind === 'source' ? env.ZERO_SQL_DO : env.ZERO_CACHE_DO
  return namespace.get(namespace.idFromName(instance)).fetch(forwarded)
}

export default {
  fetch(request, env, ctx) {
    const source = __profileForward(request, env, 'source')
    if (source) return source
    const cache = __profileForward(request, env, 'cache')
    if (cache) return cache
    const url = new URL(request.url)
    if (url.pathname === '/health') return Response.json({ ok: true })
    return __profileDataWorker.fetch(request, env, ctx)
  },
  scheduled(event, env, ctx) {
    return __profileDataWorker.scheduled(event, env, ctx)
  },
}
`

export function instrumentChatDataWorker(source: string): {
  source: string
  sourceHash: string
} {
  const sourceHash = createHash('sha256').update(source).digest('hex')
  let next =
    `import { installLocalProfileGlobals, installSqlProfiler } from '../profile-runtime.ts'\n` +
    `installLocalProfileGlobals()\n` +
    source

  next = replaceInRange(
    next,
    SOURCE_RANGE_START,
    SOURCE_RANGE_END,
    '  constructor(ctx, env) {\n    super(ctx, env)\n',
    '  constructor(ctx, env) {\n    super(ctx, env)\n    this.__profile = installSqlProfiler(ctx.storage.sql)\n',
    'ZeroSqlDO constructor'
  )
  next = replaceInRange(
    next,
    SOURCE_RANGE_START,
    SOURCE_RANGE_END,
    '  async fetch(request) {\n',
    sourceRequestProfile,
    'ZeroSqlDO fetch'
  )
  next = replaceInRange(
    next,
    CACHE_RANGE_START,
    CACHE_RANGE_END,
    '  constructor(ctx, env) {\n    super(ctx, env)\n',
    '  constructor(ctx, env) {\n    super(ctx, env)\n    this.__profile = installSqlProfiler(ctx.storage.sql)\n',
    'ZeroCacheDO constructor'
  )
  next = replaceInRange(
    next,
    CACHE_RANGE_START,
    CACHE_RANGE_END,
    '  async fetch(request) {\n',
    cacheRequestProfile,
    'ZeroCacheDO fetch'
  )
  next = replaceInRange(
    next,
    CACHE_RANGE_START,
    CACHE_RANGE_END,
    '  async bootEmbed() {\n',
    '  async bootEmbed() {\n    this.__profileBootAttempts = (this.__profileBootAttempts || 0) + 1\n',
    'ZeroCacheDO boot counter'
  )
  next = replaceInRange(
    next,
    CACHE_RANGE_START,
    CACHE_RANGE_END,
    '        const startZeroCacheEmbedCF = await getStartZeroCacheEmbedCF()\n',
    `        if (this.__profileForcedFailures > 0) {
          this.__profileForcedFailures--
          throw new Error('profile forced wrapper boot failure')
        }
        const startZeroCacheEmbedCF = await getStartZeroCacheEmbedCF()
`,
    'ZeroCacheDO forced failure'
  )
  next = replaceInRange(
    next,
    CACHE_RANGE_START,
    CACHE_RANGE_END,
    '        await this.healNullReplicaRank(appId)\n',
    '        if (!this.__profileSkipRankHeal) await this.healNullReplicaRank(appId)\n',
    'ZeroCacheDO rank-heal fault injection'
  )
  next = replaceInRange(
    next,
    CACHE_RANGE_START,
    CACHE_RANGE_END,
    '          readyTimeout: 1200000,\n',
    `          readyTimeout:
            Number(this.env.OREZ_PROFILE_READY_TIMEOUT) || 1200000,
`,
    'ZeroCacheDO profile ready timeout'
  )

  const defaultExport = next.lastIndexOf('export default {')
  if (defaultExport < 0) throw new Error('Chat data shim default export is missing')
  next =
    next.slice(0, defaultExport) +
    'const __profileDataWorker = {' +
    next.slice(defaultExport + 'export default {'.length) +
    profileRouter

  return { source: next, sourceHash }
}
