import { Link, Slot } from 'one'

const NAV = [
  {
    label: 'Start',
    items: [
      { href: '/docs', label: 'Overview' },
      { href: '/docs/getting-started', label: 'Getting started' },
      { href: '/docs/modes', label: 'Runtime modes' },
      { href: '/docs/configuration', label: 'Configuration' },
    ],
  },
  {
    label: 'Packages',
    items: [
      { href: '/docs/sync-engine', label: 'Sync engine' },
      { href: '/docs/pg-to-sqlite', label: 'pg-to-sqlite' },
    ],
  },
  {
    label: 'Reference',
    items: [
      { href: '/docs/architecture', label: 'Architecture' },
      { href: '/docs/testing', label: 'Testing' },
      { href: '/docs/trade-offs', label: 'Trade-offs' },
    ],
  },
]

export default function DocsLayout() {
  return (
    <>
      <header className="topbar">
        <Link href="/" className="brand">
          <svg className="mark" viewBox="0 0 36 30" aria-hidden="true">
            <path d="M18 2C10.5 2 4.5 10.7 2 22.6c-.5 2.4 1.3 4.4 3.8 4.4h24.4c2.5 0 4.3-2 3.8-4.4C31.5 10.7 25.5 2 18 2Z" />
            <path d="M12.5 21.5h11V27h-11z" />
          </svg>
          <span>orez</span>
        </Link>
        <nav aria-label="Primary navigation">
          <Link href="/docs">Docs</Link>
          <a href="https://github.com/natew/orez">GitHub</a>
        </nav>
      </header>

      <div className="docs">
        <aside className="docs-nav">
          {NAV.map((group) => (
            <section key={group.label}>
              <div className="nav-label">{group.label}</div>
              {group.items.map((item) => (
                <Link key={item.href} href={item.href}>
                  {item.label}
                </Link>
              ))}
            </section>
          ))}
        </aside>
        <article className="content">
          <Slot />
        </article>
      </div>
    </>
  )
}
