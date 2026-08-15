import { getMDXComponent } from '@vxrn/mdx-rust/client'
import { createRoute, useLoader } from 'one'
import { useMemo } from 'react'

import { components } from '~/components/MDXComponents'
import { docsPages } from '~/features/docs/docsRoutes'
import { siteConfig } from '~/lib/site-config'

const route = createRoute<'/docs/[...slug]'>()

export async function generateStaticParams() {
  return docsPages.slice(1).map(({ route }) => ({
    slug: route.replace('/docs/', ''),
  }))
}

export const loader = route.createLoader(async ({ params }) => {
  const { getMDXBySlug } = await import(/* @vite-ignore */ '@vxrn/mdx-rust')
  const slug = Array.isArray(params.slug) ? params.slug.join('/') : params.slug

  try {
    return await getMDXBySlug(siteConfig.docsRoot, slug, { expressiveCode: false })
  } catch {
    return getMDXBySlug(siteConfig.docsRoot, `${slug}/index`, {
      expressiveCode: false,
    })
  }
})

export default function DocPage() {
  const { code, frontmatter } = useLoader(loader)
  const Component = useMemo(() => getMDXComponent(code), [code])

  return (
    <>
      <title>{`${frontmatter.title} · ${siteConfig.titleSuffix}`}</title>
      {frontmatter.description ? (
        <meta name="description" content={frontmatter.description} />
      ) : null}
      <Component components={components} />
    </>
  )
}
