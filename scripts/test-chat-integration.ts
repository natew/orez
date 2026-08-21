import { existsSync } from 'node:fs'
import { resolve } from 'node:path'

type PackageJson = {
  dependencies?: Record<string, string>
  devDependencies?: Record<string, string>
}

const orezRoot = resolve(import.meta.dir, '..')
const chatRoot = resolve(process.env.CHAT_REPO ?? resolve(orezRoot, '..', 'chat'))
const chatPackagePath = resolve(chatRoot, 'package.json')
if (!existsSync(chatPackagePath)) {
  throw new Error(`Chat checkout not found at ${chatRoot}; set CHAT_REPO to its path`)
}

const readPackage = async (path: string): Promise<PackageJson> =>
  (await Bun.file(path).json()) as PackageJson
const versionOf = (pkg: PackageJson, name: string) =>
  pkg.dependencies?.[name] ?? pkg.devDependencies?.[name]

const chatPackagePaths = [
  chatPackagePath,
  resolve(chatRoot, 'src/start/package.json'),
  resolve(chatRoot, 'packages/start-server/runtime/package.json'),
]
const [orezPackage, ...chatPackages] = await Promise.all([
  readPackage(resolve(orezRoot, 'package.json')),
  ...chatPackagePaths.map(readPackage),
])
const orezZero = versionOf(orezPackage, '@rocicorp/zero')
const chatZeros = chatPackages.map((pkg) => versionOf(pkg, '@rocicorp/zero'))
if (!orezZero || chatZeros.some((version) => version !== orezZero)) {
  throw new Error(
    `All Chat and Orez Zero pins must match before e2e (orez=${orezZero ?? 'missing'}, chat=${chatZeros.map((version) => version ?? 'missing').join(',')})`
  )
}

const smoke = process.argv.includes('--smoke')
console.log(
  `Running Chat's ${smoke ? 'browser smoke' : 'complete e2e gate'} from ${chatRoot} (Zero ${orezZero})`
)
const child = Bun.spawn(
  ['bun', 'run', 'test', 'e2e', ...(smoke ? ['--skip-unit'] : [])],
  {
    cwd: chatRoot,
    env: process.env,
    stdin: 'inherit',
    stdout: 'inherit',
    stderr: 'inherit',
  }
)
const exitCode = await child.exited
if (exitCode !== 0) process.exit(exitCode)
