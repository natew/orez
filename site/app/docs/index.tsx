import { getMDXComponent } from '@vxrn/mdx-rust/client'
import { createRoute, useLoader } from 'one'
import { useMemo } from 'react'

import { components } from '~/components/MDXComponents'
import { siteConfig } from '~/lib/site-config'

const route = createRoute<'/docs'>()

export const loader = route.createLoader(async () => {
  const { getMDXBySlug } = await import(/* @vite-ignore */ '@vxrn/mdx-rust')
  const { frontmatter, code } = await getMDXBySlug(siteConfig.docsRoot, 'index', {
    expressiveCode: false,
  })
  return { frontmatter, code }
})

export default function DocsIndex() {
  const { code, frontmatter } = useLoader(loader)
  const Component = useMemo(() => getMDXComponent(code), [code])
  return (
    <>
      <title>{`${frontmatter.title} · ${siteConfig.titleSuffix}`}</title>
      {!!frontmatter.description && (
        <meta name="description" content={frontmatter.description} />
      )}
      <Component components={components} />
    </>
  )
}
