import { describe, expect, it } from 'bun:test'

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
