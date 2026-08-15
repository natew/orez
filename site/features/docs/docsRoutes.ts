import { isLiteSyncSite } from '~/lib/site-config'

export type DocsPage = {
  title: string
  route: string
  description?: string
  status?: 'experimental' | 'preview'
}

export type DocsSection = {
  title: string
  pages: DocsPage[]
}

const orezDocsSections: DocsSection[] = [
  {
    title: 'Start',
    pages: [
      { title: 'Overview', route: '/docs' },
      { title: 'Get started', route: '/docs/getting-started' },
    ],
  },
  {
    title: 'Node',
    pages: [
      { title: 'Overview', route: '/docs/node' },
      { title: 'Setup', route: '/docs/node/setup' },
      { title: 'Embedded Postgres', route: '/docs/node/embedded-postgres' },
      { title: 'PGlite / WASM', route: '/docs/node/pglite' },
      { title: 'Configuration & CLI', route: '/docs/node/configuration' },
      { title: 'Programmatic API', route: '/docs/node/api' },
      { title: 'Operations', route: '/docs/node/operations' },
      { title: 'Architecture', route: '/docs/node/architecture' },
      { title: 'Testing', route: '/docs/node/testing' },
      { title: 'Limitations', route: '/docs/node/limitations' },
    ],
  },
  {
    title: 'Reference',
    pages: [
      { title: 'Packages', route: '/docs/reference/packages' },
      { title: 'Troubleshooting', route: '/docs/reference/troubleshooting' },
    ],
  },
]

const liteSyncDocsSections: DocsSection[] = [
  {
    title: 'Start',
    pages: [
      { title: 'Overview', route: '/docs', status: 'preview' },
      { title: 'Client transport', route: '/docs/client', status: 'preview' },
      { title: 'Consistency', route: '/docs/consistency' },
    ],
  },
  {
    title: 'Deploy',
    pages: [
      { title: 'Cloudflare setup', route: '/docs/cloudflare' },
      { title: 'Architecture', route: '/docs/architecture' },
      { title: 'Operations', route: '/docs/operations' },
      { title: 'Testing', route: '/docs/testing' },
      { title: 'Limitations', route: '/docs/limitations' },
    ],
  },
  {
    title: 'Reference',
    pages: [{ title: 'Packages', route: '/docs/packages' }],
  },
]

export const docsSections = isLiteSyncSite ? liteSyncDocsSections : orezDocsSections

export const docsPages = docsSections.flatMap((section) => section.pages)

export function getDocsNeighbors(pathname: string) {
  const index = docsPages.findIndex((page) => page.route === pathname)
  return index < 0
    ? {}
    : {
        previous: docsPages[index - 1],
        next: docsPages[index + 1],
      }
}
