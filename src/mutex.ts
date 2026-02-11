// simple mutex for serializing pglite access.
// uses setImmediate/setTimeout between releases to prevent event loop
// starvation when multiple connections queue up — without this, releasing
// the mutex resolves the next waiter as a microtask, which causes a chain
// of synchronous pglite executions that blocks all I/O processing.
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

  release(): void {
    const next = this.queue.shift()
    if (next) {
      // yield to event loop so I/O events (socket reads/writes) are processed
      // before the next waiter acquires the mutex
      if (typeof setImmediate !== 'undefined') {
        setImmediate(next)
      } else {
        setTimeout(next, 0)
      }
    } else {
      this.locked = false
    }
  }
}
