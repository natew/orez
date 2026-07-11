import { getMDXComponent } from '@vxrn/mdx-rust/client'
import { createRoute, useLoader } from 'one'
import { useMemo } from 'react'

import { components } from '~/components/MDXComponents'

const route = createRoute<'/docs'>()

export const loader = route.createLoader(async () => {
  const { getMDXBySlug } = await import(/* @vite-ignore */ '@vxrn/mdx-rust')
  const { frontmatter, code } = await getMDXBySlug('data/docs', 'index', {
    expressiveCode: false,
  })
  return { frontmatter, code }
})

export default function DocsIndex() {
  const { code, frontmatter } = useLoader(loader)
  const Component = useMemo(() => getMDXComponent(code), [code])
  return (
    <>
      <title>{`${frontmatter.title} · orez`}</title>
      {!!frontmatter.description && (
        <meta name="description" content={frontmatter.description} />
      )}
      <Component components={components} />
    </>
  )
}
