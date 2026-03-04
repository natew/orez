// simple mutex for serializing pglite access
export class Mutex {
  private locked = false
  private queue: Array<() => void> = []

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
    const next = this.queue.shift()
    if (next) {
      next()
    } else {
      this.locked = false
    }
  }
}
