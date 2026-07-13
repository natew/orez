import { usePathname } from 'one'
import { SizableText, Text, XStack, YStack } from 'tamagui'

import { AppLink } from '~/components/AppLink'

import { getDocsNeighbors } from './docsRoutes'

export function DocsPagination() {
  const pathname = usePathname()
  const { previous, next } = getDocsNeighbors(pathname)

  if (!previous && !next) return null

  return (
    <XStack
      className="docs-pagination"
      gap="$3"
      mt="$10"
      pt="$6"
      borderTopWidth={1}
      borderColor="$color4"
    >
      {previous ? (
        <AppLink href={previous.route} className="pagination-card" flex={1}>
          <YStack
            gap="$1"
            p="$4"
            rounded="$4"
            borderWidth={1}
            borderColor="$color4"
            hoverStyle={{ bg: '$color2', borderColor: '$color6' }}
          >
            <SizableText
              size="$1"
              color="$color9"
              textTransform="uppercase"
              letterSpacing={0.8}
            >
              Previous
            </SizableText>
            <Text color="$color12" fontSize={14} fontWeight="600">
              ← {previous.title}
            </Text>
          </YStack>
        </AppLink>
      ) : (
        <YStack flex={1} />
      )}
      {next ? (
        <AppLink href={next.route} className="pagination-card" flex={1}>
          <YStack
            gap="$1"
            p="$4"
            rounded="$4"
            borderWidth={1}
            borderColor="$color4"
            items="flex-end"
            hoverStyle={{ bg: '$color2', borderColor: '$color6' }}
          >
            <SizableText
              size="$1"
              color="$color9"
              textTransform="uppercase"
              letterSpacing={0.8}
            >
              Next
            </SizableText>
            <Text color="$color12" fontSize={14} fontWeight="600" text="right">
              {next.title} →
            </Text>
          </YStack>
        </AppLink>
      ) : null}
    </XStack>
  )
}
