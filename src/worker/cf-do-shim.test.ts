import { describe, expect, it } from 'vitest'

import {
  doInstanceName,
  doInstanceNameForRequest,
  isValidNamespace,
  type NamespaceRoutingOptions,
} from './cf-do-shim.js'

// soot's deployed shim configures these; the tests below assert this module
// reproduces soot's inline doInstanceNameForRequest behaviour exactly.
const sootOpts: NamespaceRoutingOptions = {
  nsHeader: 'x-soot-ns',
  controlPlaneNamespaces: ['soot'],
}

function req(headers: Record<string, string>) {
  return { headers: { get: (name: string) => headers[name] ?? null } }
}
function url(params: Record<string, string>) {
  return { searchParams: { get: (name: string) => params[name] ?? null } }
}

describe('isValidNamespace', () => {
  it('accepts the default scope prefixes', () => {
    expect(isValidNamespace('proj-abc')).toBe(true)
    expect(isValidNamespace('test-abc123')).toBe(true)
    expect(isValidNamespace('proj-' + 'a'.repeat(64))).toBe(true)
  })

  it('rejects unknown scopes, bad chars, and length violations', () => {
    expect(isValidNamespace('')).toBe(false)
    expect(isValidNamespace('proj-')).toBe(false)
    expect(isValidNamespace('server-abc')).toBe(false) // not a default scope
    expect(isValidNamespace('proj-a/b')).toBe(false)
    expect(isValidNamespace('proj-' + 'a'.repeat(65))).toBe(false)
    expect(isValidNamespace('projabc')).toBe(false)
  })

  it('does not let a scope name leak regex metacharacters', () => {
    const opts = { scopes: ['a.b'] }
    expect(isValidNamespace('a.b-x', opts)).toBe(true)
    expect(isValidNamespace('axb-x', opts)).toBe(false)
  })

  it('honours custom scopes (e.g. chat server namespaces)', () => {
    const opts = { scopes: ['server', 'dm'] }
    expect(isValidNamespace('server-123', opts)).toBe(true)
    expect(isValidNamespace('dm-xyz', opts)).toBe(true)
    expect(isValidNamespace('proj-123', opts)).toBe(false)
  })

  // matches the exact regex soot's app-shim inlined at its 3 validation sites.
  it('matches soot inline /^(proj|test)-[A-Za-z0-9_-]{1,64}$/', () => {
    const inline = /^(proj|test)-[a-zA-Z0-9_-]{1,64}$/
    for (const ns of ['proj-a', 'test-Z9_-', 'proj-', 'evil ns', 'x-y', '', 'proj-a/b']) {
      expect(isValidNamespace(ns, { scopes: ['proj', 'test'] })).toBe(inline.test(ns))
    }
  })
})

describe('doInstanceName', () => {
  it('maps empty + control-plane aliases to the singleton', () => {
    expect(doInstanceName('')).toBe('singleton')
    expect(doInstanceName('soot', sootOpts)).toBe('singleton')
  })

  it('maps a valid tenant namespace to ns:<ns>', () => {
    expect(doInstanceName('proj-abc')).toBe('ns:proj-abc')
    expect(doInstanceName('test-9', sootOpts)).toBe('ns:test-9')
  })

  it('returns null for a structurally invalid namespace', () => {
    expect(doInstanceName('bogus')).toBe(null)
    expect(doInstanceName('proj-a/b')).toBe(null)
  })

  it('does not treat a control-plane alias as a tenant namespace', () => {
    expect(doInstanceName('soot')).toBe(null)
  })
})

describe('doInstanceNameForRequest', () => {
  it('reads the configured header, then ?ns=, then defaults to singleton', () => {
    expect(
      doInstanceNameForRequest(req({ 'x-soot-ns': 'proj-a' }), url({}), sootOpts)
    ).toBe('ns:proj-a')
    expect(doInstanceNameForRequest(req({}), url({ ns: 'test-b' }), sootOpts)).toBe(
      'ns:test-b'
    )
    expect(doInstanceNameForRequest(req({}), url({}), sootOpts)).toBe('singleton')
  })

  it('prefers the header over the query param', () => {
    expect(
      doInstanceNameForRequest(
        req({ 'x-soot-ns': 'proj-h' }),
        url({ ns: 'proj-q' }),
        sootOpts
      )
    ).toBe('ns:proj-h')
  })

  it('rejects an invalid namespace with null (caller replies 400)', () => {
    expect(
      doInstanceNameForRequest(req({ 'x-soot-ns': 'evil ns' }), url({}), sootOpts)
    ).toBe(null)
  })

  // reproduces soot's exact inline doInstanceNameForRequest, end to end.
  it('matches soot inline doInstanceNameForRequest across inputs', () => {
    const inlineSoot = (
      headers: Record<string, string>,
      params: Record<string, string>
    ) => {
      const ns = headers['x-soot-ns'] || params['ns'] || ''
      if (!ns || ns === 'soot') return 'singleton'
      if (!/^(proj|test)-[a-zA-Z0-9_-]{1,64}$/.test(ns)) return null
      return 'ns:' + ns
    }
    const cases: [Record<string, string>, Record<string, string>][] = [
      [{ 'x-soot-ns': 'proj-a' }, {}],
      [{}, { ns: 'test-b' }],
      [{}, {}],
      [{ 'x-soot-ns': 'soot' }, {}],
      [{ 'x-soot-ns': 'evil ns' }, {}],
      [{ 'x-soot-ns': 'proj-h' }, { ns: 'proj-q' }],
      [{}, { ns: 'server-1' }],
    ]
    for (const [h, p] of cases) {
      expect(doInstanceNameForRequest(req(h), url(p), sootOpts)).toBe(inlineSoot(h, p))
    }
  })
})
