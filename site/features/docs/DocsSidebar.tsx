import { usePathname } from 'one'
import { SizableText, Text, YStack } from 'tamagui'

import { AppLink } from '~/components/AppLink'

import { docsSections } from './docsRoutes'

function SidebarContents() {
  const pathname = usePathname()

  return (
    <YStack gap="$5" pb="$8">
      {docsSections.map((section) => (
        <YStack key={section.title} render="section" gap="$1">
          <SizableText
            px="$3"
            mb="$1"
            size="$2"
            color="$color9"
            fontWeight="700"
            textTransform="uppercase"
            letterSpacing={1.1}
          >
            {section.title}
          </SizableText>
          {section.pages.map((page) => {
            const active = pathname === page.route
            return (
              <AppLink
                key={page.route}
                href={page.route}
                className="docs-sidebar-link"
                aria-current={active ? 'page' : undefined}
                display="flex"
                items="center"
                justify="space-between"
                gap="$2"
                px="$3"
                py="$2"
                rounded="$3"
                color={active ? '$color12' : '$color10'}
                bg={active ? '$color3' : 'transparent'}
                fontSize={14}
                lineHeight={20}
                fontWeight={active ? '600' : '400'}
                hoverStyle={{ bg: active ? '$color3' : '$color2', color: '$color12' }}
              >
                <Text
                  color={active ? '$color12' : '$color10'}
                  fontSize={14}
                  lineHeight={20}
                  flex={1}
                >
                  {page.title}
                </Text>
                {page.status ? (
                  <Text
                    color={page.status === 'experimental' ? '$orange10' : '$blue10'}
                    fontSize={9}
                    fontWeight="700"
                    letterSpacing={0.5}
                    textTransform="uppercase"
                  >
                    {page.status === 'experimental' ? 'Exp' : 'Preview'}
                  </Text>
                ) : null}
              </AppLink>
            )
          })}
        </YStack>
      ))}
    </YStack>
  )
}

export function DocsSidebar() {
  return (
    <>
      <aside className="docs-sidebar" aria-label="Documentation navigation">
        <SidebarContents />
      </aside>
      <details className="docs-mobile-navigation">
        <summary>Browse documentation</summary>
        <nav aria-label="Documentation navigation">
          <SidebarContents />
        </nav>
      </details>
    </>
  )
}
