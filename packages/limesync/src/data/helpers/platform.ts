const isClientEnvironment =
  process.env.VITE_NATIVE === '1' ||
  process.env.VITE_ENVIRONMENT === 'client' ||
  process.env.VITE_ENVIRONMENT === 'ios' ||
  process.env.VITE_ENVIRONMENT === 'android' ||
  process.env.VITE_ENVIRONMENT === 'macos' ||
  process.env.VITE_ENVIRONMENT === 'linux'

export const IS_SERVER_RUNTIME: boolean =
  process.env.VITE_ENVIRONMENT === 'ssr' ||
  (!isClientEnvironment &&
    typeof process !== 'undefined' &&
    !!process.versions &&
    !!(process.versions.node || (process.versions as any).bun))

const isBrowser =
  isClientEnvironment ||
  (process.env.VITE_ENVIRONMENT !== 'ssr' &&
    typeof navigator !== 'undefined' &&
    typeof location !== 'undefined')

export const IS_SERVER: boolean =
  process.env.VITE_ENVIRONMENT === 'ssr' || (!isClientEnvironment && !isBrowser)
