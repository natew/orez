// zero's ArrayView settles its result status once. A changed row can repair an
// errored query, but the existing view must be rematerialized to become complete.
export class QueryErrorRecovery {
  private data: string | undefined
  private errored = false

  changed(data: unknown, resultType: string): boolean {
    if (resultType !== 'error') {
      this.errored = false
      this.data = undefined
      return false
    }
    const current = JSON.stringify(data)
    const changed = this.errored && current !== this.data
    this.errored = true
    this.data = current
    return changed
  }
}
