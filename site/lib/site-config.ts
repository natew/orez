export type SiteId = 'orez' | 'lite-sync'

const siteId = import.meta.env.VITE_SITE

if (siteId !== 'orez' && siteId !== 'lite-sync') {
  throw new Error('VITE_SITE must be either "orez" or "lite-sync"')
}

export const isLiteSyncSite = siteId === 'lite-sync'

export const siteConfig = isLiteSyncSite
  ? {
      id: siteId,
      name: 'Lite Sync',
      titleSuffix: 'Lite Sync docs',
      description: "A compact Rust and SQLite sync server for Zero's real client.",
      docsRoot: 'data/lite-sync-docs',
      showLogo: false,
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
      showLogo: true,
      npmHref: 'https://www.npmjs.com/package/orez',
      relatedSite: {
        href: 'https://lite-sync-docs.natewienert.workers.dev',
        label: 'Lite Sync',
      },
    }
