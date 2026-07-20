#!/usr/bin/env bun

const [file] = process.argv.slice(2)
if (!file) throw new Error('usage: normalize-sync-native-licenses.ts <file>')

const normalized = (await Bun.file(file).text())
  .replaceAll('\r\n', '\n')
  .split('\n')
  .map((line) => line.trimEnd())
  .join('\n')
  .trimEnd()

await Bun.write(file, normalized + '\n')
