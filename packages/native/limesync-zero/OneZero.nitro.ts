import type { HybridObject } from 'react-native-nitro-modules'

export interface OneZero extends HybridObject<{ ios: 'c++'; android: 'c++' }> {
  open(name: string, schemaJson: string, clientID: string): Promise<void>
  // execute is registered in C++ to take ownership of the converted string.
  close(): Promise<void>
}
