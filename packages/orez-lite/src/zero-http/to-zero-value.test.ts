import { describe, expect, it } from 'vitest'

import { toZeroValue } from './mount.js'

// canonical zero types timestamps as plain numbers and converts pg timestamp
// text to epoch ms at its replication boundary, throwing on garbage — a client
// never receives a string for a number column. the mount must give native
// sqlite the same guarantee.
describe('toZeroValue number columns', () => {
  it('passes numbers and numeric text through', () => {
    expect(toZeroValue('number', 1784515076835)).toBe(1784515076835)
    expect(toZeroValue('number', '1784515076835')).toBe(1784515076835)
    // cloudflare's DO SQL API binds every JS number as a double; sqlite
    // renders that into TEXT-affinity storage as decimal text
    expect(toZeroValue('number', '1784515076835.0')).toBe(1784515076835)
  })

  it('decodes the stable SQL and ISO timestamp text forms to epoch ms', () => {
    const epoch = Date.parse('2026-07-20T02:37:56.835Z')
    expect(toZeroValue('number', '2026-07-20T02:37:56.835Z')).toBe(epoch)
    // the postgres bridge's timestamptz text
    expect(toZeroValue('number', '2026-07-20 02:37:56.835+00')).toBe(epoch)
    // a missing offset is UTC, matching the rust engine's
    // timestamp_text_to_epoch_ms
    expect(toZeroValue('number', '2026-07-20 02:37:56.835')).toBe(epoch)
    expect(toZeroValue('number', '2026-07-19 16:37:56.835-10')).toBe(epoch)
  })

  it('throws on text that is not a number for the declared type', () => {
    expect(() => toZeroValue('number', 'not-a-timestamp')).toThrow('Error parsing')
    expect(() => toZeroValue('number', '2026-99-99 99:99:99')).toThrow('Error parsing')
  })

  it('keeps null and non-number types unchanged', () => {
    expect(toZeroValue('number', null)).toBeNull()
    expect(toZeroValue('string', 'not-a-timestamp')).toBe('not-a-timestamp')
    expect(toZeroValue('boolean', 1)).toBe(true)
  })
})
