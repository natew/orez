import { H1, H2, H3, Paragraph, SizableText, Text, XStack, YStack } from 'tamagui'

import { AppLink } from './AppLink'

import type { ComponentPropsWithoutRef, ReactNode } from 'react'

function A({ href = '', children, ...props }: ComponentPropsWithoutRef<'a'>) {
  return (
    <AppLink
      href={href}
      target={href.startsWith('http') ? '_blank' : undefined}
      className="mdx-link"
      color="$blue10"
      textDecorationLine="underline"
      {...(props as any)}
    >
      {children}
    </AppLink>
  )
}

function Table(props: ComponentPropsWithoutRef<'table'>) {
  return (
    <div
      className="table-scroll"
      role="region"
      aria-label="Scrollable table"
      tabIndex={0}
    >
      <table {...props} />
    </div>
  )
}

function Callout({
  children,
  title,
  tone = 'note',
}: {
  children: ReactNode
  title?: string
  tone?: 'note' | 'warning' | 'experimental'
}) {
  const colors =
    tone === 'warning'
      ? { border: '$yellow7', background: '$yellow2', text: '$yellow11' }
      : tone === 'experimental'
        ? { border: '$orange7', background: '$orange2', text: '$orange11' }
        : { border: '$blue7', background: '$blue2', text: '$blue11' }

  return (
    <YStack
      my="$5"
      p="$4"
      gap="$2"
      rounded="$4"
      borderWidth={1}
      borderColor={colors.border as any}
      bg={colors.background as any}
    >
      {title ? (
        <SizableText size="$3" fontWeight="700" color={colors.text as any}>
          {title}
        </SizableText>
      ) : null}
      <YStack>{children}</YStack>
    </YStack>
  )
}

function Status({
  children,
  tone = 'preview',
}: {
  children: ReactNode
  tone?: 'stable' | 'preview' | 'experimental'
}) {
  const color =
    tone === 'stable' ? '$green10' : tone === 'experimental' ? '$orange10' : '$blue10'
  return (
    <Text
      render="span"
      display="inline-flex"
      px="$2"
      py={3}
      rounded="$10"
      bg="$color3"
      color={color}
      fontSize={11}
      lineHeight={14}
      fontWeight="700"
      textTransform="uppercase"
      letterSpacing={0.6}
    >
      {children}
    </Text>
  )
}

function CardGrid({ children }: { children: ReactNode }) {
  return <div className="doc-card-grid">{children}</div>
}

function DocCard({
  href,
  title,
  children,
  badge,
}: {
  href: string
  title: string
  children: ReactNode
  badge?: string
}) {
  return (
    <AppLink href={href} className="doc-card-link">
      <YStack
        height="100%"
        gap="$3"
        p="$5"
        rounded="$5"
        borderWidth={1}
        borderColor="$color4"
        bg="$color1"
        hoverStyle={{ bg: '$color2', borderColor: '$color6' }}
      >
        <XStack gap="$2" items="center" justify="space-between">
          <Text color="$color12" fontSize={17} lineHeight={22} fontWeight="700">
            {title}
          </Text>
          {badge ? (
            <Status tone={badge === 'Experimental' ? 'experimental' : 'preview'}>
              {badge}
            </Status>
          ) : null}
        </XStack>
        <Paragraph color="$color10" fontSize={14} lineHeight={21}>
          {children}
        </Paragraph>
      </YStack>
    </AppLink>
  )
}

export const components = {
  h1: (props: ComponentPropsWithoutRef<'h1'>) => (
    <H1 className="mdx-h1" color="$color12" {...(props as any)} />
  ),
  h2: (props: ComponentPropsWithoutRef<'h2'>) => (
    <H2 className="mdx-h2" color="$color12" {...(props as any)} />
  ),
  h3: (props: ComponentPropsWithoutRef<'h3'>) => (
    <H3 className="mdx-h3" color="$color12" {...(props as any)} />
  ),
  p: (props: ComponentPropsWithoutRef<'p'>) => (
    <Paragraph className="mdx-paragraph" color="$color11" {...(props as any)} />
  ),
  a: A,
  table: Table,
  Callout,
  Status,
  CardGrid,
  DocCard,
}
