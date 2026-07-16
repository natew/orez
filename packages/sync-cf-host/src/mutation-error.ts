// the app-rejection contract shared by every orez sync host and by
// protocol-neutral authoring layers (on-zero adapters). classification is
// STRUCTURAL, never instanceof: adapters must not depend on orez to reject a
// mutation, and instanceof breaks across package copies (dual-package
// installs, --into tarballs, workspace duplication). any error whose name is
// 'MutationApplicationError' with string details/message is claiming this
// contract deliberately.
export class MutationApplicationError extends Error {
  constructor(
    readonly details: string,
    message = details
  ) {
    super(message)
    this.name = 'MutationApplicationError'
  }
}

export function isMutationApplicationError(
  error: unknown
): error is { name: string; message: string; details: string } {
  if (typeof error !== 'object' || error === null) return false
  const shaped = error as { name?: unknown; message?: unknown; details?: unknown }
  return (
    shaped.name === 'MutationApplicationError' &&
    typeof shaped.message === 'string' &&
    typeof shaped.details === 'string'
  )
}
