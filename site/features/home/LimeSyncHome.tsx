import { H1, H2, Paragraph, Text, XStack, YStack } from 'tamagui'

import { AppLink } from '~/components/AppLink'

export function LimeSyncHome() {
  return (
    <>
      <title>Lime Sync · Zero on SQLite</title>
      <meta name="description" content="A Rust sync server for Zero backed by SQLite." />

      <main id="main-content" className="home-page lime-sync-home">
        <section className="lime-sync-hero">
          <YStack gap="$4" maxW={640}>
            <H1 color="$color12" className="lime-sync-title">
              Zero on SQLite
            </H1>
            <Paragraph color="$color10" className="lime-sync-lede">
              Lime Sync is a Rust sync server for Zero backed by SQLite. It uses the stock
              @rocicorp/zero client and replaces zero-cache.
            </Paragraph>
            <XStack className="lime-sync-links" items="center" gap="$5" flexWrap="wrap">
              <AppLink href="/docs" className="lime-sync-link">
                Documentation
              </AppLink>
              <AppLink href="/docs/architecture" className="lime-sync-link">
                Architecture
              </AppLink>
            </XStack>
            <Text color="$color9" fontSize={13}>
              Current preview package: <code>orez-lite</code>
            </Text>
          </YStack>
        </section>

        <section className="lime-sync-section" aria-labelledby="lime-sync-fit-title">
          <YStack gap="$4" maxW={700}>
            <H2 id="lime-sync-fit-title" color="$color12" className="lime-sync-heading">
              How it fits
            </H2>
            <Paragraph color="$color10" className="lime-sync-copy">
              Your application keeps its Zero schema, queries, mutators, authentication,
              and data endpoints. Lime Sync handles the sync protocol and stores its state
              in SQLite.
            </Paragraph>
            <dl className="lime-sync-facts">
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

        <section className="lime-sync-section" aria-labelledby="lime-sync-start-title">
          <YStack gap="$4" maxW={700}>
            <H2 id="lime-sync-start-title" color="$color12" className="lime-sync-heading">
              Start here
            </H2>
            <Paragraph color="$color10" className="lime-sync-copy">
              The Cloudflare host is in preview. Read the setup and limitations before
              deploying it.
            </Paragraph>
            <XStack className="lime-sync-links" items="center" gap="$5" flexWrap="wrap">
              <AppLink href="/docs/cloudflare" className="lime-sync-link">
                Cloudflare setup
              </AppLink>
              <AppLink href="/docs/limitations" className="lime-sync-link">
                Limitations
              </AppLink>
              <AppLink href="/docs/packages" className="lime-sync-link">
                Packages
              </AppLink>
            </XStack>
          </YStack>
        </section>
      </main>
    </>
  )
}
