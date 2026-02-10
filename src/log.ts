import type { LogLevel } from './config.js'

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

export function setLogLevel(level: LogLevel) {
  currentLevel = level
}

function prefix(label: string, color: string): string {
  return `${BOLD}${color}[${label}]${RESET}`
}

/** format a port number with matching dim color */
export function port(n: number, color: keyof typeof COLORS): string {
  return `${DIM}${COLORS[color]}:${n}${RESET}`
}

function makeLogger(label: string, color: string, level: LogLevel = 'info') {
  const p = prefix(label, color)
  return (...args: unknown[]) => {
    if (LEVEL_PRIORITY[level] <= LEVEL_PRIORITY[currentLevel]) {
      console.info(p, ...args)
    }
  }
}

export const log = {
  orez: makeLogger('orez', COLORS.cyan, 'warn'),
  pglite: makeLogger('pglite', COLORS.green, 'warn'),
  proxy: makeLogger('pg-proxy', COLORS.yellow, 'warn'),
  zero: makeLogger('zero-cache', COLORS.magenta, 'warn'),
  s3: makeLogger('orez/s3', COLORS.blue, 'warn'),
  debug: {
    orez: makeLogger('orez', COLORS.cyan, 'debug'),
    pglite: makeLogger('pglite', COLORS.green, 'debug'),
    proxy: makeLogger('pg-proxy', COLORS.yellow, 'debug'),
    zero: makeLogger('zero-cache', COLORS.magenta, 'debug'),
    s3: makeLogger('orez/s3', COLORS.blue, 'debug'),
  },
}
