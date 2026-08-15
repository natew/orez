import { H1, H2, Paragraph, SizableText, Text, XStack, YStack } from 'tamagui'

import { AppLink } from '~/components/AppLink'

export function LiteSyncHome() {
  return (
    <>
      <title>Lite Sync — Zero sync, without zero-cache</title>
      <meta
        name="description"
        content="A compact Rust and SQLite sync server for the real Zero client."
      />

      <main id="main-content" className="home-page">
        <section className="home-hero">
          <YStack gap="$5" maxW={820}>
            <SizableText
              size="$2"
              color="$color9"
              fontWeight="700"
              textTransform="uppercase"
              letterSpacing={1.3}
            >
              SQLite-native Zero sync
            </SizableText>
            <H1 color="$color12" className="home-title">
              Zero sync, without zero-cache.
            </H1>
            <Paragraph color="$color10" className="home-lede">
              Lite Sync is a compact Rust and SQLite server that speaks the Zero protocol
              to the real @rocicorp/zero client. Keep Zero’s queries, mutators, local
              cache, and optimistic UI while replacing its server-side sync machinery.
            </Paragraph>
            <XStack className="hero-actions" items="center" gap="$3" flexWrap="wrap">
              <AppLink href="/docs" className="button-link button-primary">
                Read the docs
              </AppLink>
              <AppLink href="/docs/architecture" className="button-link button-secondary">
                See the architecture
              </AppLink>
              <div className="install-command" aria-label="Current preview package">
                <code>orez-lite · preview</code>
              </div>
            </XStack>
          </YStack>
        </section>

        <section className="home-section" aria-labelledby="lite-sync-contract-title">
          <div className="section-heading">
            <SizableText
              size="$2"
              color="$color9"
              fontWeight="700"
              textTransform="uppercase"
              letterSpacing={1.1}
            >
              Keep the client
            </SizableText>
            <H2 id="lite-sync-contract-title" color="$color12" className="section-title">
              A smaller server behind the same Zero application.
            </H2>
            <Paragraph color="$color10" className="section-lede">
              Lite Sync changes the hosting boundary. Your application still owns its
              schema, named queries, custom mutators, authentication, permissions, and
              authoritative data.
            </Paragraph>
          </div>
          <div className="value-grid">
            <YStack gap="$2">
              <Text color="$color12" fontSize={16} fontWeight="700">
                Real Zero client
              </Text>
              <Paragraph color="$color10">
                Continue using @rocicorp/zero, ZQL, framework bindings, and the optimistic
                local cache.
              </Paragraph>
            </YStack>
            <YStack gap="$2">
              <Text color="$color12" fontSize={16} fontWeight="700">
                Rust sync engine
              </Text>
              <Paragraph color="$color10">
                Deterministic pull, push, query membership, mutation acknowledgements,
                retention, and recovery run in a compact engine.
              </Paragraph>
            </YStack>
            <YStack gap="$2">
              <Text color="$color12" fontSize={16} fontWeight="700">
                SQLite storage
              </Text>
              <Paragraph color="$color10">
                Each namespace keeps ordered sync state and a replica in SQLite, with a
                Cloudflare Durable Object host available today.
              </Paragraph>
            </YStack>
          </div>
        </section>

        <section
          className="home-section home-technical"
          aria-labelledby="sync-flow-title"
        >
          <div className="section-heading">
            <SizableText
              size="$2"
              color="$color9"
              fontWeight="700"
              textTransform="uppercase"
              letterSpacing={1.1}
            >
              One application contract
            </SizableText>
            <H2 id="sync-flow-title" color="$color12" className="section-title">
              Swap the sync server, keep your application logic.
            </H2>
            <Paragraph color="$color10" className="section-lede">
              Authenticated query and mutation work still goes to your application.
              Committed database changes flow back through Lite Sync as Zero row patches.
            </Paragraph>
          </div>
          <div
            className="flow-strip"
            role="img"
            aria-label="Zero client connects through Lite Sync to application endpoints and authoritative data"
          >
            <div>
              <strong>Zero client</strong>
              <span>queries · mutators · optimistic UI</span>
            </div>
            <span className="flow-arrow">→</span>
            <div>
              <strong>Lite Sync</strong>
              <span>Rust · SQLite · Zero protocol</span>
            </div>
            <span className="flow-arrow">→</span>
            <div>
              <strong>Your application</strong>
              <span>auth · endpoints · authoritative data</span>
            </div>
          </div>
        </section>

        <section className="home-cta">
          <YStack gap="$4" maxW={700}>
            <H2 color="$color12" className="section-title">
              Start at the server boundary.
            </H2>
            <Paragraph color="$color10" fontSize={17} lineHeight={27}>
              The Cloudflare host is in preview. Read its deployment and operational
              boundaries before moving an application.
            </Paragraph>
            <XStack gap="$4" items="center" flexWrap="wrap">
              <AppLink href="/docs/cloudflare" className="button-link button-primary">
                Cloudflare setup
              </AppLink>
              <AppLink
                href="/docs/limitations"
                color="$color11"
                fontSize={14}
                fontWeight="600"
              >
                Read the limitations →
              </AppLink>
            </XStack>
          </YStack>
        </section>
      </main>
    </>
  )
}
