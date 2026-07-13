import { SizableText, XStack } from 'tamagui'

import { AppLink } from './AppLink'

export function SiteFooter() {
  return (
    <XStack render="footer" className="site-footer" items="center" gap="$5" py="$6">
      <SizableText size="$2" color="$color9" flex={1}>
        Orez · MIT licensed
      </SizableText>
      <AppLink
        href="https://www.npmjs.com/package/orez"
        target="_blank"
        color="$color10"
        fontSize={13}
      >
        npm ↗
      </AppLink>
      <AppLink
        href="https://github.com/natew/orez"
        target="_blank"
        color="$color10"
        fontSize={13}
      >
        source ↗
      </AppLink>
    </XStack>
  )
}
