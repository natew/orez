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

export const docsSections: DocsSection[] = [
  {
    title: 'Start',
    pages: [
      { title: 'Overview', route: '/docs' },
      { title: 'Get started', route: '/docs/getting-started' },
      { title: 'Choose a runtime', route: '/docs/runtimes' },
      { title: 'Unchanged Zero client', route: '/docs/zero-compatibility' },
      { title: 'Consistency', route: '/docs/consistency' },
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
    title: 'Orez Lite',
    pages: [
      { title: 'Overview', route: '/docs/orez-lite', status: 'preview' },
      { title: 'Cloudflare setup', route: '/docs/orez-lite/cloudflare' },
      { title: 'Architecture', route: '/docs/orez-lite/architecture' },
      { title: 'Operations', route: '/docs/orez-lite/operations' },
      { title: 'Testing', route: '/docs/orez-lite/testing' },
      { title: 'Limitations', route: '/docs/orez-lite/limitations' },
    ],
  },
  {
    title: 'Reference',
    pages: [
      { title: 'pg-to-sqlite', route: '/docs/reference/pg-to-sqlite' },
      { title: 'Packages', route: '/docs/reference/packages' },
      { title: 'Troubleshooting', route: '/docs/reference/troubleshooting' },
    ],
  },
]

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
