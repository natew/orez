export function internalSchema(appId: string): string {
  return `${appId}_0`
}

export function publicationName(appId: string): string {
  return `zero_${appId}`
}
