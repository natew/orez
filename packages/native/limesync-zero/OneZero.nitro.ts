import type { CustomType, HybridObject } from 'react-native-nitro-modules'

type ZeroObject = CustomType<object, 'ZeroValue', { include: 'ZeroValue.hpp' }>
type ZeroRequest = CustomType<
  { op: string; [key: string]: unknown },
  'ZeroValue',
  { include: 'ZeroValue.hpp' }
>

export interface OneZero extends HybridObject<{ ios: 'c++'; android: 'c++' }> {
  open(name: string, schema: ZeroObject, clientID: string): Promise<void>
  execute(request: ZeroRequest): Promise<ZeroObject | null | string | number | boolean>
  close(): Promise<void>
}
