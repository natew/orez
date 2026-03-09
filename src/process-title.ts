export function orezTitle(label = 'orez'): string {
  const cwd = process.cwd()
  const home = process.env.HOME || ''
  const dir =
    home && cwd.startsWith(home + '/') ? '~' + cwd.slice(home.length) : cwd.split('/').pop()!
  return `${label} (${dir})`
}
