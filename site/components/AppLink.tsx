import { useLinkTo } from 'one'
import { Text, type TextProps } from 'tamagui'

import type { MouseEvent, ReactNode } from 'react'

type AppLinkProps = TextProps & {
  children: ReactNode
  href: string
  replace?: boolean
  target?: '_blank' | '_self'
}

export function AppLink({ href, replace, target, children, ...props }: AppLinkProps) {
  const linkProps = useLinkTo({ href: href as any, replace })
  const { onPress: linkOnPress, ...anchorProps } = linkProps as typeof linkProps & {
    onPress?: (event: MouseEvent<HTMLAnchorElement>) => void
  }

  return (
    <Text
      render="a"
      cursor="pointer"
      color="$color11"
      textDecorationLine="none"
      hoverStyle={{ color: '$color12' }}
      focusVisibleStyle={{
        outlineWidth: 2,
        outlineColor: '$color8',
        outlineOffset: 3,
        outlineStyle: 'solid',
        rounded: '$2',
      }}
      $platform-web={{
        color: 'inherit',
        fontFamily: 'inherit',
        fontSize: 'inherit',
        lineHeight: 'inherit',
      }}
      {...props}
      {...anchorProps}
      target={target}
      {...(target === '_blank' ? ({ rel: 'noopener noreferrer' } as any) : {})}
      onPress={(event) => {
        props.onPress?.(event as never)
        if (!event.defaultPrevented) linkOnPress?.(event)
      }}
    >
      {children}
    </Text>
  )
}
