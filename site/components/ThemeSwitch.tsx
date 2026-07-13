import { useUserScheme } from '@vxrn/color-scheme'
import { Button, Text } from 'tamagui'

const settings = ['system', 'light', 'dark'] as const

export function ThemeSwitch() {
  const scheme = useUserScheme()
  const next = settings[(settings.indexOf(scheme.setting) + 1) % settings.length]!

  return (
    <Button
      unstyled
      width={36}
      height={36}
      rounded="$10"
      items="center"
      justify="center"
      borderWidth={1}
      borderColor="$color4"
      bg="$color1"
      cursor="pointer"
      hoverStyle={{ bg: '$color3', borderColor: '$color6' }}
      pressStyle={{ scale: 0.96 }}
      onPress={() => scheme.set(next)}
      aria-label={`Theme: ${scheme.setting}. Switch to ${next}.`}
    >
      <Text color="$color11" fontSize={16} lineHeight={20} aria-hidden>
        {scheme.setting === 'dark' ? '☾' : scheme.setting === 'light' ? '☀' : '◐'}
      </Text>
    </Button>
  )
}
