/**
 * stream shim — re-exports stream-browserify with missing Node.js stream functions.
 *
 * stream-browserify doesn't include getDefaultHighWaterMark which zero-cache uses.
 */

// @ts-expect-error — stream-browserify is CJS
export * from 'stream-browserify'
// @ts-expect-error — stream-browserify is CJS
export { default } from 'stream-browserify'

export function getDefaultHighWaterMark(objectMode: boolean): number {
  return objectMode ? 16 : 16 * 1024
}
