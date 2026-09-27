# @o/helpers

Shared browser, React, storage, string, and timing helpers for Orez consumers.

```ts
import { createEmitter, createStorageValue, time } from '@o/helpers'
```

`getClipboardText` lives at `@o/helpers/clipboard`, apart from the main entry: its native variant imports the optional `expo-clipboard` peer, which only an app that ships Expo modules can load.
