import { MetaTheme, SchemeProvider, useUserScheme } from '@vxrn/color-scheme'
import { TamaguiProvider, useTheme } from 'tamagui'

import { config } from '~/tamagui.config'

import type { ReactNode } from 'react'

export function TamaguiRootProvider({ children }: { children: ReactNode }) {
  return (
    <SchemeProvider>
      <ThemedRoot>{children}</ThemedRoot>
    </SchemeProvider>
  )
}

function ThemedRoot({ children }: { children: ReactNode }) {
  const userScheme = useUserScheme()

  return (
    <TamaguiProvider config={config} defaultTheme={userScheme.value}>
      <ThemeMeta />
      {children}
    </TamaguiProvider>
  )
}

function ThemeMeta() {
  const theme = useTheme()
  return (
    <MetaTheme color={theme.background.val} darkColor="#0b0c0c" lightColor="#fdfdfc" />
  )
}
