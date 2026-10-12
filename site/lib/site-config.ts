export type SiteId = 'orez' | 'lime-sync'

const siteId = import.meta.env.VITE_SITE

if (siteId !== 'orez' && siteId !== 'lime-sync') {
  throw new Error('VITE_SITE must be either "orez" or "lime-sync"')
}

export const isLimeSyncSite = siteId === 'lime-sync'

export const siteConfig = isLimeSyncSite
  ? {
      id: siteId,
      name: 'Lime Sync',
      titleSuffix: 'Lime Sync docs',
      description: "A compact Rust and SQLite sync server for Zero's real client.",
      docsRoot: 'data/lime-sync-docs',
      favicon: '/lime-sync.svg',
      npmHref: 'https://www.npmjs.com/package/orez-lite',
      relatedSite: {
        href: 'https://orez-docs.natewienert.workers.dev',
        label: 'Orez',
      },
    }
  : {
      id: siteId,
      name: 'Orez',
      titleSuffix: 'Orez docs',
      description: 'The easy local Zero runner with one command and one configuration.',
      docsRoot: 'data/docs',
      favicon: '/favicon.svg',
      npmHref: 'https://www.npmjs.com/package/orez',
      relatedSite: {
        href: 'https://lime-sync.natewienert.workers.dev',
        label: 'Lime Sync',
      },
    }
