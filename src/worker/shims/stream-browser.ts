/**
 * stream shim — re-exports readable-stream with missing Node.js stream functions.
 *
 * readable-stream/stream-browserify don't include getDefaultHighWaterMark
 * which zero-cache uses (added in Node.js 18+).
 */

// @ts-expect-error — readable-stream is CJS
export * from 'readable-stream'
// @ts-expect-error — readable-stream is CJS
export { default } from 'readable-stream'

export function getDefaultHighWaterMark(objectMode: boolean): number {
  return objectMode ? 16 : 16 * 1024
}
