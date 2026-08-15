import { H1, H2, Paragraph, SizableText, Text, XStack, YStack } from 'tamagui'

import { AppLink } from '~/components/AppLink'

import type { ReactNode } from 'react'

function DatabaseCard({
  badge,
  children,
  href,
  title,
}: {
  badge: string
  children: ReactNode
  href: string
  title: string
}) {
  return (
    <AppLink href={href} className="runtime-card-link">
      <YStack
        height="100%"
        p="$5"
        gap="$4"
        rounded="$6"
        borderWidth={1}
        borderColor="$color4"
        bg="$color1"
        hoverStyle={{ bg: '$color2', borderColor: '$color7', y: -2 }}
        pressStyle={{ scale: 0.99 }}
      >
        <XStack items="center" justify="space-between" gap="$3">
          <Text color="$color12" fontSize={20} lineHeight={25} fontWeight="700">
            {title}
          </Text>
          <Text
            px="$2"
            py={3}
            rounded="$10"
            bg="$color3"
            color="$blue10"
            fontSize={10}
            lineHeight={14}
            fontWeight="800"
            textTransform="uppercase"
            letterSpacing={0.7}
          >
            {badge}
          </Text>
        </XStack>
        <Paragraph color="$color10" fontSize={15} lineHeight={23} flex={1}>
          {children}
        </Paragraph>
        <Text color="$color11" fontSize={13} fontWeight="600">
          Explore {title} →
        </Text>
      </YStack>
    </AppLink>
  )
}

export function OrezHome() {
  return (
    <>
      <title>Orez — Run Zero locally</title>
      <meta
        name="description"
        content="Run a complete local Zero stack with one command and one configuration."
      />

      <main id="main-content" className="home-page">
        <section className="home-hero">
          <YStack gap="$5" maxW={800}>
            <SizableText
              size="$2"
              color="$color9"
              fontWeight="700"
              textTransform="uppercase"
              letterSpacing={1.3}
            >
              The all-in-one Zero runner
            </SizableText>
            <H1 color="$color12" className="home-title">
              Zero, locally.
            </H1>
            <Paragraph color="$color10" className="home-lede">
              Orez runs stock zero-cache and its local infrastructure from one config and
              one command. Use portable PGlite for the fastest start, or Embedded Postgres
              when you need production-like concurrency.
            </Paragraph>
            <XStack className="hero-actions" items="center" gap="$3" flexWrap="wrap">
              <AppLink
                href="/docs/getting-started"
                className="button-link button-primary"
              >
                Get started
              </AppLink>
              <AppLink href="/docs/node" className="button-link button-secondary">
                Read the docs
              </AppLink>
              <div className="install-command" aria-label="Run Orez">
                <code>bunx orez</code>
              </div>
            </XStack>
          </YStack>
        </section>

        <section className="home-section" aria-labelledby="one-config-title">
          <div className="section-heading">
            <SizableText
              size="$2"
              color="$color9"
              fontWeight="700"
              textTransform="uppercase"
              letterSpacing={1.1}
            >
              Easy first
            </SizableText>
            <H2 id="one-config-title" color="$color12" className="section-title">
              The local Zero stack, glued together for you.
            </H2>
            <Paragraph color="$color10" className="section-lede">
              Orez starts and monitors zero-cache, its databases, migrations, storage,
              admin tools, and application hooks. Your app keeps the normal Zero client
              API.
            </Paragraph>
          </div>
          <div className="value-grid">
            <YStack gap="$2">
              <Text color="$color12" fontSize={16} fontWeight="700">
                One command
              </Text>
              <Paragraph color="$color10">
                Start the coordinated development backend without Docker or a separately
                managed database.
              </Paragraph>
            </YStack>
            <YStack gap="$2">
              <Text color="$color12" fontSize={16} fontWeight="700">
                One configuration
              </Text>
              <Paragraph color="$color10">
                Keep migrations, service addresses, storage, hooks, admin, and Zero
                options in one file.
              </Paragraph>
            </YStack>
            <YStack gap="$2">
              <Text color="$color12" fontSize={16} fontWeight="700">
                Stock Zero
              </Text>
              <Paragraph color="$color10">
                Orez coordinates the real zero-cache server and @rocicorp/zero client
                instead of replacing their application APIs.
              </Paragraph>
            </YStack>
          </div>
        </section>

        <section className="home-section" aria-labelledby="databases-title">
          <div className="section-heading">
            <SizableText
              size="$2"
              color="$color9"
              fontWeight="700"
              textTransform="uppercase"
              letterSpacing={1.1}
            >
              Pick your database
            </SizableText>
            <H2 id="databases-title" color="$color12" className="section-title">
              Portable by default, production-like when you need it.
            </H2>
          </div>
          <div className="runtime-grid">
            <DatabaseCard href="/docs/node/pglite" title="PGlite" badge="Default">
              Start quickly with a portable WASM PostgreSQL profile for local development,
              CI, examples, and disposable environments.
            </DatabaseCard>
            <DatabaseCard
              href="/docs/node/embedded-postgres"
              title="Embedded Postgres"
              badge="Concurrent"
            >
              Run a real PostgreSQL server with stock logical replication when your tests
              need concurrent connections and production-like behavior.
            </DatabaseCard>
          </div>
        </section>

        <section className="home-section home-technical" aria-labelledby="flow-title">
          <div className="section-heading">
            <SizableText
              size="$2"
              color="$color9"
              fontWeight="700"
              textTransform="uppercase"
              letterSpacing={1.1}
            >
              Real Zero, managed locally
            </SizableText>
            <H2 id="flow-title" color="$color12" className="section-title">
              Your application stays a Zero application.
            </H2>
            <Paragraph color="$color10" className="section-lede">
              Orez owns the development lifecycle around Zero. Your schema, queries,
              mutators, optimistic UI, and application endpoints stay unchanged.
            </Paragraph>
          </div>
          <div
            className="flow-strip"
            role="img"
            aria-label="Zero client connects through zero-cache managed by Orez to a local database"
          >
            <div>
              <strong>Zero client</strong>
              <span>queries · mutators · optimistic UI</span>
            </div>
            <span className="flow-arrow">→</span>
            <div>
              <strong>zero-cache</strong>
              <span>started and monitored by Orez</span>
            </div>
            <span className="flow-arrow">→</span>
            <div>
              <strong>Local data</strong>
              <span>PGlite · Embedded Postgres</span>
            </div>
          </div>
        </section>

        <section className="home-cta">
          <YStack gap="$4" maxW={680}>
            <H2 color="$color12" className="section-title">
              Start with one command.
            </H2>
            <Paragraph color="$color10" fontSize={17} lineHeight={27}>
              Run the default PGlite profile now, then switch the same configuration to
              Embedded Postgres when your test needs it.
            </Paragraph>
            <XStack gap="$4" items="center" flexWrap="wrap">
              <AppLink
                href="/docs/getting-started"
                className="button-link button-primary"
              >
                Run Orez
              </AppLink>
              <AppLink
                href="/docs/node/architecture"
                color="$color11"
                fontSize={14}
                fontWeight="600"
              >
                Read the architecture →
              </AppLink>
            </XStack>
          </YStack>
        </section>
      </main>
    </>
  )
}
