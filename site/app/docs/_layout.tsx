import { Link, Slot } from 'one'

const NAV = [
  { href: '/docs', label: 'Overview' },
  { href: '/docs/architecture', label: 'Architecture' },
  { href: '/docs/delegation', label: 'Delegation model' },
  { href: '/docs/configuration', label: 'Configuration' },
  { href: '/docs/testing', label: 'Testing' },
  { href: '/docs/trade-offs', label: 'Trade-offs' },
  { href: '/docs/consumers', label: 'Consumers' },
]

export default function DocsLayout() {
  return (
    <>
      <header className="topbar">
        <Link href="/" className="brand">
          orez
        </Link>
        <Link href="/docs">docs</Link>
        <a href="https://github.com/natew/orez">github</a>
      </header>

      <div className="docs">
        <nav className="docs-nav">
          <div className="nav-label">Rust sync server</div>
          {NAV.map((item) => (
            <Link key={item.href} href={item.href}>
              {item.label}
            </Link>
          ))}
        </nav>
        <div className="content">
          <Slot />
        </div>
      </div>
    </>
  )
}
