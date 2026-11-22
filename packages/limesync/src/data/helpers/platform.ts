export const IS_SERVER_RUNTIME: boolean =
  process.env.VITE_ENVIRONMENT === 'ssr' ||
  (process.env.VITE_ENVIRONMENT !== 'client' &&
    typeof process !== 'undefined' &&
    !!process.versions &&
    !!(process.versions.node || (process.versions as any).bun))

const isBrowser =
  process.env.VITE_ENVIRONMENT === 'client' ||
  (process.env.VITE_ENVIRONMENT !== 'ssr' &&
    typeof navigator !== 'undefined' &&
    typeof location !== 'undefined')

export const IS_SERVER: boolean =
  process.env.VITE_ENVIRONMENT === 'ssr' ||
  (process.env.VITE_ENVIRONMENT !== 'client' && !isBrowser)
