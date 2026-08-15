import { H1, H2, Paragraph, Text, XStack, YStack } from 'tamagui'

import { AppLink } from '~/components/AppLink'

export function LiteSyncHome() {
  return (
    <>
      <title>Lite Sync · Zero on SQLite</title>
      <meta name="description" content="A Rust sync server for Zero backed by SQLite." />

      <main id="main-content" className="home-page lite-sync-home">
        <section className="lite-sync-hero">
          <YStack gap="$4" maxW={640}>
            <H1 color="$color12" className="lite-sync-title">
              Zero on SQLite
            </H1>
            <Paragraph color="$color10" className="lite-sync-lede">
              Lite Sync is a Rust sync server for Zero backed by SQLite. It uses the stock
              @rocicorp/zero client and replaces zero-cache.
            </Paragraph>
            <XStack className="lite-sync-links" items="center" gap="$5" flexWrap="wrap">
              <AppLink href="/docs" className="lite-sync-link">
                Documentation
              </AppLink>
              <AppLink href="/docs/architecture" className="lite-sync-link">
                Architecture
              </AppLink>
            </XStack>
            <Text color="$color9" fontSize={13}>
              Current preview package: <code>orez-lite</code>
            </Text>
          </YStack>
        </section>

        <section className="lite-sync-section" aria-labelledby="lite-sync-fit-title">
          <YStack gap="$4" maxW={700}>
            <H2 id="lite-sync-fit-title" color="$color12" className="lite-sync-heading">
              How it fits
            </H2>
            <Paragraph color="$color10" className="lite-sync-copy">
              Your application keeps its Zero schema, queries, mutators, authentication,
              and data endpoints. Lite Sync handles the sync protocol and stores its state
              in SQLite.
            </Paragraph>
            <dl className="lite-sync-facts">
              <div>
                <dt>Client</dt>
                <dd>@rocicorp/zero</dd>
              </div>
              <div>
                <dt>Server</dt>
                <dd>Rust</dd>
              </div>
              <div>
                <dt>Storage</dt>
                <dd>SQLite</dd>
              </div>
              <div>
                <dt>Current host</dt>
                <dd>Cloudflare Durable Objects</dd>
              </div>
            </dl>
          </YStack>
        </section>

        <section className="lite-sync-section" aria-labelledby="lite-sync-start-title">
          <YStack gap="$4" maxW={700}>
            <H2 id="lite-sync-start-title" color="$color12" className="lite-sync-heading">
              Start here
            </H2>
            <Paragraph color="$color10" className="lite-sync-copy">
              The Cloudflare host is in preview. Read the setup and limitations before
              deploying it.
            </Paragraph>
            <XStack className="lite-sync-links" items="center" gap="$5" flexWrap="wrap">
              <AppLink href="/docs/cloudflare" className="lite-sync-link">
                Cloudflare setup
              </AppLink>
              <AppLink href="/docs/limitations" className="lite-sync-link">
                Limitations
              </AppLink>
              <AppLink href="/docs/packages" className="lite-sync-link">
                Packages
              </AppLink>
            </XStack>
          </YStack>
        </section>
      </main>
    </>
  )
}
