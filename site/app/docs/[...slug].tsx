import { getMDXComponent } from '@vxrn/mdx-rust/client'
import { createRoute, useLoader } from 'one'
import { useMemo } from 'react'

import { components } from '~/components/MDXComponents'

const route = createRoute<'/docs/[...slug]'>()

export async function generateStaticParams() {
  const { getAllFrontmatter } = await import(/* @vite-ignore */ '@vxrn/mdx-rust')
  return getAllFrontmatter('data/docs')
    .map(({ slug }) => ({
      slug: slug.replace(/.*docs\//, '').replace(/\/index$/, ''),
    }))
    .filter(({ slug }) => slug.length > 0 && slug !== 'index')
}

export const loader = route.createLoader(async ({ params }) => {
  const { getMDXBySlug } = await import(/* @vite-ignore */ '@vxrn/mdx-rust')
  const slug = Array.isArray(params.slug) ? params.slug.join('/') : params.slug

  try {
    return await getMDXBySlug('data/docs', slug, { expressiveCode: false })
  } catch {
    return getMDXBySlug('data/docs', `${slug}/index`, { expressiveCode: false })
  }
})

export default function DocPage() {
  const { code, frontmatter } = useLoader(loader)
  const Component = useMemo(() => getMDXComponent(code), [code])

  return (
    <>
      <title>{`${frontmatter.title} · Orez docs`}</title>
      {frontmatter.description ? (
        <meta name="description" content={frontmatter.description} />
      ) : null}
      <Component components={components} />
    </>
  )
}
