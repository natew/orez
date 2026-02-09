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

function prefix(label: string, color: string): string {
  return `${BOLD}${color}[${label}]${RESET}`
}

function makeLogger(label: string, color: string) {
  const p = prefix(label, color)
  return (...args: unknown[]) => {
    console.info(p, ...args)
  }
}

export const log = {
  orez: makeLogger('orez', COLORS.cyan),
  pglite: makeLogger('pglite', COLORS.green),
  proxy: makeLogger('pg-proxy', COLORS.yellow),
  zero: makeLogger('zero-cache', COLORS.magenta),
  s3: makeLogger('orez/s3', COLORS.blue),
}
