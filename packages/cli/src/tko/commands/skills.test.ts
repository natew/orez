import { describe, expect, it } from 'bun:test'
import {
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import { transformSkillForHarness } from './skills'

describe('transformSkillForHarness', () => {
  it('maps runner tiers to Claude models for claude harness', () => {
    const inputSm = `---
name: my-skill
model: sm
description: A helpful skill
---

# My skill
`
    const outputSm = transformSkillForHarness(inputSm, 'claude')
    expect(outputSm).toContain('model: haiku')
    expect(outputSm).not.toContain('model: sm')

    const inputMd = `---
name: my-skill
model: md
description: A helpful skill
---
`
    expect(transformSkillForHarness(inputMd, 'claude')).toContain('model: sonnet')

    const inputLg = `---
name: my-skill
model: lg
description: A helpful skill
---
`
    expect(transformSkillForHarness(inputLg, 'claude')).toContain('model: opus')

    const inputXl = `---
name: my-skill
model: xl
description: A helpful skill
---
`
    expect(transformSkillForHarness(inputXl, 'claude')).toContain('model: fable')
  })

  it('leaves models untouched for agents harness', () => {
    const input = `---
name: my-skill
model: sm
description: A helpful skill
---
`
    expect(transformSkillForHarness(input, 'agents')).toBe(input)
  })

  it('leaves already-specific Claude models untouched', () => {
    const input = `---
name: my-skill
model: haiku
description: A helpful skill
---
`
    expect(transformSkillForHarness(input, 'claude')).toBe(input)
  })

  it('leaves files without model untouched', () => {
    const input = `---
name: my-skill
description: A helpful skill
---

# Body
`
    expect(transformSkillForHarness(input, 'claude')).toBe(input)
  })
})

describe('skills generation ownership', () => {
  it('leaves linked skill directories intact during generation, clean and stale pruning', () => {
    const fixture = mkdtempSync(join(tmpdir(), 'orez-skill-ownership-'))
    const cli = resolve(import.meta.dir, '../cli.ts')
    const original =
      '---\nname: upgrade\ndescription: shared owned skill\n---\nkeep this source\n'
    try {
      const shared = join(fixture, '.team-machine/skills/upgrade')
      mkdirSync(shared, { recursive: true })
      writeFileSync(join(shared, 'SKILL.md'), original)
      mkdirSync(join(shared, 'agents'))
      const docs = join(fixture, 'skills')
      mkdirSync(docs)
      for (const harness of ['.claude', '.agents']) {
        const root = join(fixture, harness, 'skills')
        mkdirSync(root, { recursive: true })
        symlinkSync(shared, join(root, 'upgrade'), 'dir')
        symlinkSync(shared, join(root, 'takeout-linked'), 'dir')
        const stale = join(root, 'takeout-stale')
        mkdirSync(stale)
        writeFileSync(join(stale, 'SKILL.md'), 'old generated skill')
      }
      const run = (clean: boolean) => {
        const result = Bun.spawnSync({
          cmd: [
            process.execPath,
            cli,
            'skills',
            'generate',
            ...(clean ? ['--clean'] : []),
          ],
          cwd: fixture,
          stdout: 'pipe',
          stderr: 'pipe',
        })
        expect(result.exitCode).toBe(0)
        expect(result.stderr.toString()).toBe('')
        expect(readFileSync(join(shared, 'SKILL.md'), 'utf8')).toBe(original)
        for (const harness of ['.claude', '.agents']) {
          const root = join(fixture, harness, 'skills')
          expect(lstatSync(join(root, 'upgrade')).isSymbolicLink()).toBe(true)
          expect(lstatSync(join(root, 'takeout-linked')).isSymbolicLink()).toBe(true)
          expect(existsSync(join(root, 'takeout-stale'))).toBe(false)
        }
      }
      run(false)
      writeFileSync(
        join(docs, 'upgrade.md'),
        '---\nname: upgrade\ndescription: a generated name collision\nmodel: md\n---\nreplacement\n'
      )
      writeFileSync(
        join(docs, 'current.md'),
        '---\nname: current\ndescription: actual generated output\nmodel: md\n---\ncurrent source\n'
      )
      run(true)
      expect(
        readFileSync(join(fixture, '.claude/skills/current/SKILL.md'), 'utf8')
      ).toContain('model: sonnet')
      expect(
        readFileSync(join(fixture, '.agents/skills/current/SKILL.md'), 'utf8')
      ).toContain('model: md')
    } finally {
      rmSync(fixture, { recursive: true, force: true })
    }
  })

  it('leaves a linked harness root intact while generating for independent harnesses', () => {
    const fixture = mkdtempSync(join(tmpdir(), 'orez-skill-root-'))
    try {
      const shared = join(fixture, 'shared-skills')
      mkdirSync(join(shared, 'manual'), { recursive: true })
      writeFileSync(join(shared, 'manual/SKILL.md'), 'retain root-owned source')
      mkdirSync(join(fixture, '.claude'))
      symlinkSync(shared, join(fixture, '.claude/skills'), 'dir')
      mkdirSync(join(fixture, 'skills'))
      writeFileSync(
        join(fixture, 'skills/current.md'),
        '---\nname: current\ndescription: generated skill\n---\ncurrent source\n'
      )
      const result = Bun.spawnSync({
        cmd: [
          process.execPath,
          resolve(import.meta.dir, '../cli.ts'),
          'skills',
          'generate',
          '--clean',
        ],
        cwd: fixture,
        stdout: 'pipe',
        stderr: 'pipe',
      })
      expect(result.exitCode).toBe(0)
      expect(result.stderr.toString()).toBe('')
      expect(readFileSync(join(shared, 'manual/SKILL.md'), 'utf8')).toBe(
        'retain root-owned source'
      )
      expect(existsSync(join(shared, 'current'))).toBe(false)
      expect(lstatSync(join(fixture, '.claude/skills')).isSymbolicLink()).toBe(true)
      expect(
        readFileSync(join(fixture, '.agents/skills/current/SKILL.md'), 'utf8')
      ).toContain('current source')
    } finally {
      rmSync(fixture, { recursive: true, force: true })
    }
  })
})
