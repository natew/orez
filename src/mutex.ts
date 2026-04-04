// simple mutex for serializing pglite access
// uses head-index instead of Array.shift() for O(1) release
export class Mutex {
  private locked = false
  private queue: Array<() => void> = []
  private head = 0

  /** check if the mutex is currently held (non-blocking, no side effects) */
  get isLocked(): boolean {
    return this.locked
  }

  async acquire(): Promise<void> {
    if (!this.locked) {
      this.locked = true
      return
    }
    return new Promise<void>((resolve) => {
      this.queue.push(resolve)
    })
  }

  // non-blocking acquire: returns true if lock was obtained, false otherwise
  tryAcquire(): boolean {
    if (!this.locked) {
      this.locked = true
      return true
    }
    return false
  }

  release(): void {
    if (this.head < this.queue.length) {
      const next = this.queue[this.head++]!
      // compact periodically to prevent unbounded array growth
      if (this.head > 64) {
        this.queue = this.queue.slice(this.head)
        this.head = 0
      }
      next()
    } else {
      this.queue = []
      this.head = 0
      this.locked = false
    }
  }
}
