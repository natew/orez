export type EvidenceIdentity = 'verified-release' | 'verified-build' | 'unverified'

export function classifyEvidenceIdentity(
  status: string,
  releaseSha: string | null,
  buildSha: string | null
): EvidenceIdentity {
  if (status === 'verified' && releaseSha !== null && releaseSha === buildSha) {
    return 'verified-release'
  }
  if (status === 'verified' && buildSha !== null) return 'verified-build'
  return 'unverified'
}
