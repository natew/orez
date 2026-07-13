export type EvidenceIdentity = 'verified' | 'candidate' | 'unverified'

export function classifyEvidenceIdentity(
  status: string,
  releaseSha: string | null,
  buildSha: string | null
): EvidenceIdentity {
  if (status === 'verified' && releaseSha !== null && releaseSha === buildSha) {
    return 'verified'
  }
  return buildSha === null ? 'unverified' : 'candidate'
}
