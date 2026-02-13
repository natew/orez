import type { LogLevel } from './config.js'
import type { LogStore } from './admin/log-store.js'

const RESET = '\x1b[0m'
const BOLD = '\x1b[1m'
const DIM = '\x1b[2m'

const COLORS = {
  cyan: '\x1b[36m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  magenta: '\x1b[35m',
  blue: '\x1b[34m',
} as const

const LEVEL_PRIORITY: Record<LogLevel, number> = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3,
}

let currentLevel: LogLevel = 'warn'
let logStore: LogStore | undefined

export function setLogLevel(level: LogLevel) {
  currentLevel = level
}

/** hook up logStore for admin dashboard observability */
export function setLogStore(store: LogStore | undefined) {
  logStore = store
}

function prefix(label: string, color: string): string {
  return `${BOLD}${color}[${label}]${RESET}`
}

/** format a port number with matching dim color */
export function port(n: number, color: keyof typeof COLORS): string {
  return `${DIM}${COLORS[color]}:${n}${RESET}`
}

/** format a url with yellow color */
export function url(u: string): string {
  return `${COLORS.yellow}${u}${RESET}`
}

// map logger labels to logStore source names
const LABEL_TO_SOURCE: Record<string, string> = {
  orez: 'orez',
  'orez:pg': 'orez',
  pglite: 'pglite',
  'pg-proxy': 'proxy',
  'orez:zero': 'zero',
  'orez:s3': 's3',
}

function makeLogger(label: string, color: string, level: LogLevel = 'info') {
  const p = prefix(label, color)
  const source = LABEL_TO_SOURCE[label] || 'orez'
  // zero logs are handled specially in startZeroCache with better level detection
  const skipLogStore = source === 'zero'
  return (...args: unknown[]) => {
    if (LEVEL_PRIORITY[level] <= LEVEL_PRIORITY[currentLevel]) {
      console.info(p, ...args)
    }
    // always push to logStore if available (admin captures all levels)
    if (logStore && !skipLogStore) {
      const msg = args.map((a) => (typeof a === 'string' ? a : String(a))).join(' ')
      logStore.push(source, level, msg)
    }
  }
}

export const log = {
  orez: makeLogger('orez', COLORS.cyan, 'warn'),
  pg: makeLogger('orez:pg', COLORS.green, 'warn'),
  pglite: makeLogger('pglite', COLORS.green, 'warn'),
  proxy: makeLogger('pg-proxy', COLORS.yellow, 'warn'),
  zero: makeLogger('orez:zero', COLORS.magenta, 'warn'),
  s3: makeLogger('orez:s3', COLORS.blue, 'warn'),
  debug: {
    orez: makeLogger('orez', COLORS.cyan, 'debug'),
    pg: makeLogger('orez:pg', COLORS.green, 'debug'),
    pglite: makeLogger('pglite', COLORS.green, 'debug'),
    proxy: makeLogger('pg-proxy', COLORS.yellow, 'debug'),
    zero: makeLogger('orez:zero', COLORS.magenta, 'debug'),
    s3: makeLogger('orez:s3', COLORS.blue, 'debug'),
  },
}
