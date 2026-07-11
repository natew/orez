import { Link } from 'one'

export default function HomePage() {
  return (
    <>
      <header className="topbar">
        <Link href="/" className="brand">
          orez
        </Link>
        <Link href="/docs">docs</Link>
        <a href="https://github.com/natew/orez">github</a>
      </header>

      <main className="page">
        <h1>Run Zero on SQLite.</h1>

        <p className="lede">
          The node and web Zero stack, plus a SQLite-native sync engine that replaces
          zero-cache on Cloudflare.
        </p>

        <pre>
          <code>bunx orez</code>
        </pre>

        <p>
          orez runs <a href="https://zero.rocicorp.dev">Zero</a> locally with no native
          dependencies, and ships a Rust sync engine that speaks Zero's protocol-v51 pull
          dialect over SQLite. On Cloudflare it runs as one Durable Object per namespace,
          holding the sync engine and its storage in the same object.
        </p>

        <ul>
          <li>Zero locally on PGlite or embedded Postgres, nothing to compile</li>
          <li>A Rust sync engine for stock Zero clients, no zero-cache process</li>
          <li>One Durable Object per namespace on Cloudflare, SQLite storage</li>
          <li>
            Delegates auth, query permissions, and writes to your app, the same split
            zero-cache uses
          </li>
        </ul>

        <p>
          <Link href="/docs">Read the docs</Link>
        </p>
      </main>
    </>
  )
}
