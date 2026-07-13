import { Slot } from 'one'

import { DocsPagination } from '~/features/docs/DocsPagination'
import { DocsSidebar } from '~/features/docs/DocsSidebar'

export default function DocsLayout() {
  return (
    <main id="main-content" className="docs-shell">
      <DocsSidebar />
      <article className="docs-content">
        <Slot />
        <DocsPagination />
      </article>
    </main>
  )
}
