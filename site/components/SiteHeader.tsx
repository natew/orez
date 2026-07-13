import { usePathname } from 'one'
import { useState } from 'react'
import { Button, SizableText, Text, XStack, YStack } from 'tamagui'

import { AppLink } from './AppLink'
import { ThemeSwitch } from './ThemeSwitch'

function Mark() {
  return (
    <svg className="brand-mark" viewBox="0 0 36 30" aria-hidden="true">
      <path d="M18 2C10.5 2 4.5 10.7 2 22.6c-.5 2.4 1.3 4.4 3.8 4.4h24.4c2.5 0 4.3-2 3.8-4.4C31.5 10.7 25.5 2 18 2Z" />
      <path d="M12.5 21.5h11V27h-11z" />
    </svg>
  )
}

const primaryLinks = [
  { href: '/docs', label: 'Docs' },
  { href: '/docs/runtimes', label: 'Runtimes' },
] as const

export function SiteHeader() {
  const pathname = usePathname()
  const [menuOpen, setMenuOpen] = useState(false)

  return (
    <>
      <XStack
        render="header"
        className="site-header"
        height={68}
        items="center"
        justify="space-between"
        gap="$4"
      >
        <AppLink href="/" aria-label="Orez home" className="brand-link">
          <XStack items="center" gap="$2">
            <Mark />
            <Text color="$color12" fontSize={17} fontWeight="700" letterSpacing={-0.4}>
              orez
            </Text>
          </XStack>
        </AppLink>

        <XStack
          className="desktop-navigation"
          render="nav"
          aria-label="Primary"
          items="center"
          gap="$5"
        >
          {primaryLinks.map((item) => {
            const active =
              item.href === '/docs'
                ? pathname.startsWith('/docs') && !pathname.startsWith('/docs/runtimes')
                : pathname.startsWith(item.href)
            return (
              <AppLink
                key={item.href}
                href={item.href}
                color={active ? '$color12' : '$color10'}
                fontSize={14}
                fontWeight={active ? '600' : '500'}
              >
                {item.label}
              </AppLink>
            )
          })}
          <AppLink
            href="https://github.com/natew/orez"
            target="_blank"
            color="$color10"
            fontSize={14}
            fontWeight="500"
          >
            GitHub ↗
          </AppLink>
          <ThemeSwitch />
        </XStack>

        <XStack className="mobile-navigation" items="center" gap="$2">
          <ThemeSwitch />
          <Button
            unstyled
            height={36}
            px="$3"
            rounded="$10"
            borderWidth={1}
            borderColor="$color4"
            bg="$color1"
            cursor="pointer"
            onPress={() => setMenuOpen((open) => !open)}
            aria-expanded={menuOpen}
            aria-controls="mobile-menu"
          >
            <SizableText color="$color11" size="$3" fontWeight="600">
              {menuOpen ? 'Close' : 'Menu'}
            </SizableText>
          </Button>
        </XStack>
      </XStack>

      {menuOpen ? (
        <YStack
          id="mobile-menu"
          className="mobile-menu-panel"
          gap="$2"
          p="$3"
          bg="$background"
          borderColor="$color4"
        >
          {[
            ...primaryLinks,
            { href: '/docs/node', label: 'Node' },
            { href: '/docs/orez-lite', label: 'Orez Lite' },
          ].map((item) => (
            <AppLink
              key={item.href}
              href={item.href}
              px="$3"
              py="$3"
              rounded="$3"
              color="$color11"
              hoverStyle={{ bg: '$color2', color: '$color12' }}
              onPress={() => setMenuOpen(false)}
            >
              {item.label}
            </AppLink>
          ))}
          <AppLink href="https://github.com/natew/orez" target="_blank" px="$3" py="$3">
            GitHub ↗
          </AppLink>
        </YStack>
      ) : null}
    </>
  )
}
