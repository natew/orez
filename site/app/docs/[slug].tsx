import { getMDXComponent } from '@vxrn/mdx-rust/client'
import { createRoute, useLoader } from 'one'
import { useMemo } from 'react'

import { components } from '~/components/MDXComponents'

const route = createRoute<'/docs/[slug]'>()

export async function generateStaticParams() {
  const { getAllFrontmatter } = await import(/* @vite-ignore */ '@vxrn/mdx-rust')
  return getAllFrontmatter('data/docs')
    .map(({ slug }) => ({ slug: slug.replace(/.*docs\//, '') }))
    .filter((params) => params.slug !== 'index')
}

export const loader = route.createLoader(async ({ params }) => {
  const { getMDXBySlug } = await import(/* @vite-ignore */ '@vxrn/mdx-rust')
  const { frontmatter, code } = await getMDXBySlug('data/docs', params.slug, {
    expressiveCode: false,
  })
  return { frontmatter, code }
})

export default function DocPage() {
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
