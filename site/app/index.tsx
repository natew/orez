import { H1, H2, Paragraph, SizableText, Text, XStack, YStack } from 'tamagui'

import { AppLink } from '~/components/AppLink'

import type { ReactNode } from 'react'

function RuntimeCard({
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
  const experimental = badge === 'Experimental'
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
            color={experimental ? '$orange10' : '$blue10'}
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

export default function HomePage() {
  return (
    <>
      <title>Orez — Zero, anywhere</title>
      <meta
        name="description"
        content="The easy all-in-one Zero runner. One config for local development, CI, and native SQLite infrastructure."
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
              Zero, anywhere.
            </H1>
            <Paragraph color="$color10" className="home-lede">
              Orez runs a complete Zero stack from one config and one command. The Node
              runtime packages stock Zero for local development and CI, while Orez Lite
              brings the same client model to a compact SQLite-native engine.
            </Paragraph>
            <XStack className="hero-actions" items="center" gap="$3" flexWrap="wrap">
              <AppLink
                href="/docs/getting-started"
                className="button-link button-primary"
              >
                Get started
              </AppLink>
              <AppLink href="/docs/runtimes" className="button-link button-secondary">
                Choose a runtime
              </AppLink>
              <div className="install-command" aria-label="Install Orez">
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
              The Zero stack, glued together for you.
            </H2>
            <Paragraph color="$color10" className="section-lede">
              Orez starts and monitors the services in a local Zero stack. The same
              configuration controls migrations, storage, admin tools, and recovery hooks.
              Your application keeps the usual Zero client API.
            </Paragraph>
          </div>
          <div className="value-grid">
            <YStack gap="$2">
              <Text color="$color12" fontSize={16} fontWeight="700">
                One command
              </Text>
              <Paragraph color="$color10">
                Orez manages the service processes and gives them a shared development
                lifecycle.
              </Paragraph>
            </YStack>
            <YStack gap="$2">
              <Text color="$color12" fontSize={16} fontWeight="700">
                One configuration
              </Text>
              <Paragraph color="$color10">
                One file controls service addresses and lifecycle, storage settings,
                admin, and Zero options.
              </Paragraph>
            </YStack>
            <YStack gap="$2">
              <Text color="$color12" fontSize={16} fontWeight="700">
                Still Zero
              </Text>
              <Paragraph color="$color10">
                Node runs stock zero-cache. Orez Lite implements the client model in a
                smaller SQLite-native engine.
              </Paragraph>
            </YStack>
          </div>
        </section>

        <section className="home-section" aria-labelledby="runtimes-title">
          <div className="section-heading">
            <SizableText
              size="$2"
              color="$color9"
              fontWeight="700"
              textTransform="uppercase"
              letterSpacing={1.1}
            >
              Run anywhere
            </SizableText>
            <H2 id="runtimes-title" color="$color12" className="section-title">
              Choose the machinery your environment needs.
            </H2>
          </div>
          <div className="runtime-grid">
            <RuntimeCard href="/docs/node" title="Node" badge="Recommended">
              The standard Zero server, packaged with its local infrastructure. Embedded
              Postgres supports concurrent database work, while PGlite keeps local and CI
              setup portable.
            </RuntimeCard>
            <RuntimeCard href="/docs/orez-lite" title="Orez Lite" badge="Preview">
              Run Orez’s SQLite-native sync engine in a native Rust service or a
              Cloudflare Durable Object.
            </RuntimeCard>
          </div>
        </section>

        <section className="home-section home-technical" aria-labelledby="same-app-title">
          <div className="section-heading">
            <SizableText
              size="$2"
              color="$color9"
              fontWeight="700"
              textTransform="uppercase"
              letterSpacing={1.1}
            >
              One client model
            </SizableText>
            <H2 id="same-app-title" color="$color12" className="section-title">
              One client API across both runtimes.
            </H2>
            <Paragraph color="$color10" className="section-lede">
              Node packages stock Zero into a coordinated runner. Orez Lite provides pull,
              push, permissions, and wake through a smaller SQLite-native engine. Both use
              the Zero client model, with compatibility documented for each runtime.
            </Paragraph>
          </div>
          <div
            className="flow-strip"
            role="img"
            aria-label="Zero client connects through an Orez runtime to its database"
          >
            <div>
              <strong>Zero client</strong>
              <span>queries · mutators · optimistic UI</span>
            </div>
            <span className="flow-arrow">→</span>
            <div>
              <strong>Orez runtime</strong>
              <span>Node · Orez Lite</span>
            </div>
            <span className="flow-arrow">→</span>
            <div>
              <strong>Your data</strong>
              <span>Postgres · PGlite · SQLite</span>
            </div>
          </div>
        </section>

        <section className="home-cta">
          <YStack gap="$4" maxW={680}>
            <H2 color="$color12" className="section-title">
              Start with one command.
            </H2>
            <Paragraph color="$color10" fontSize={17} lineHeight={27}>
              The Node runtime is the default path. Pick Embedded Postgres for the closest
              match to production Zero, or PGlite for the lightest CI and local setup.
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
