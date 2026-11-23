# Native application helpers

The Lime Sync Zero engine is an explicit iOS build input. Install `@o/native`
and `limesync` from the same release. In a One app, include
`node_modules/@o/native/limesync-zero` in `nativeCompiler.iosNitroModules`.
CocoaPods apps can include the `LimeSyncZero` pod from the same directory and
initialize `react-native-nitro-modules` before opening the client.

The client is available through `limesync/zero/native`.
