import '@tamagui/core/reset.css'
import './styles.css'

import { Slot } from 'one'
import { YStack } from 'tamagui'

import { SiteFooter } from '~/components/SiteFooter'
import { SiteHeader } from '~/components/SiteHeader'
import { TamaguiRootProvider } from '~/components/TamaguiRootProvider'

export default function Layout() {
  return (
    <html lang="en-US">
      <head>
        <meta charSet="utf-8" />
        <meta httpEquiv="X-UA-Compatible" content="IE=edge" />
        <meta
          name="viewport"
          content="width=device-width, initial-scale=1, maximum-scale=5"
        />
        <meta name="theme-color" content="#fdfdfc" />
        <link rel="icon" href="/favicon.svg" />
      </head>
      <body>
        <TamaguiRootProvider>
          <YStack className="site-root" minH="100vh" bg="$background">
            <a className="skip-link" href="#main-content">
              Skip to content
            </a>
            <SiteHeader />
            <Slot />
            <SiteFooter />
          </YStack>
        </TamaguiRootProvider>
      </body>
    </html>
  )
}
