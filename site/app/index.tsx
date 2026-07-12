import { Link } from 'one'

function Mark() {
  return (
    <svg className="mark" viewBox="0 0 36 30" aria-hidden="true">
      <path d="M18 2C10.5 2 4.5 10.7 2 22.6c-.5 2.4 1.3 4.4 3.8 4.4h24.4c2.5 0 4.3-2 3.8-4.4C31.5 10.7 25.5 2 18 2Z" />
      <path d="M12.5 21.5h11V27h-11z" />
    </svg>
  )
}

export default function HomePage() {
  return (
    <>
      <title>orez — Zero without the infrastructure</title>
      <meta
        name="description"
        content="A small, self-contained runtime for Zero, from local development to SQLite at the edge."
      />

      <header className="topbar">
        <Link href="/" className="brand">
          <Mark />
          <span>orez</span>
        </Link>
        <nav aria-label="Primary navigation">
          <Link href="/docs">Docs</Link>
          <Link href="/docs/modes">Modes</Link>
          <a href="https://github.com/natew/orez">GitHub</a>
        </nav>
      </header>

      <main className="home">
        <section className="hero">
          <p className="eyebrow">The small Zero runtime</p>
          <h1>Zero, without the infrastructure.</h1>
          <p className="lede">
            Run a complete Zero backend from one command. Use embedded Postgres locally,
            real Postgres when you need it, or a SQLite-native sync engine at the edge.
          </p>
          <div className="install" aria-label="Install command">
            <code>bunx orez</code>
          </div>
          <div className="hero-links">
            <Link href="/docs/getting-started">Get started</Link>
            <Link href="/docs/modes">Choose a mode</Link>
          </div>
        </section>

        <section className="summary" aria-label="What orez provides">
          <div>
            <h2>Local by default</h2>
            <p>
              PGlite, zero-cache, and SQLite in one process tree. No Docker, database
              install, or native compilation required.
            </p>
          </div>
          <div>
            <h2>Not a toy abstraction</h2>
            <p>
              Switch to embedded Postgres for real logical replication, or deploy the Rust
              sync engine on Cloudflare Durable Objects.
            </p>
          </div>
          <div>
            <h2>SQL that travels</h2>
            <p>
              The separate <Link href="/docs/pg-to-sqlite">pg-to-sqlite</Link> compiler
              translates PostgreSQL syntax using the real PostgreSQL parser.
            </p>
          </div>
        </section>

        <section className="closing">
          <h2>Start simple. Change the machinery when the workload asks for it.</h2>
          <p>
            orez keeps the client contract stable while letting you choose the database,
            SQLite implementation, persistence model, and sync host independently.
          </p>
          <Link href="/docs">Read the documentation →</Link>
        </section>
      </main>

      <footer>
        <span>MIT licensed</span>
        <a href="https://www.npmjs.com/package/orez">npm</a>
        <a href="https://github.com/natew/orez">source</a>
      </footer>
    </>
  )
}
