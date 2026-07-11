import { Link } from 'one'

// internal links get client-side navigation; everything else is a plain anchor.
// all other tags render as their default HTML elements, styled by styles.css.
function A({ href = '', children, ...props }: any) {
  if (typeof href === 'string' && href.startsWith('/')) {
    return (
      <Link href={href} {...props}>
        {children}
      </Link>
    )
  }
  return (
    <a href={href} {...props}>
      {children}
    </a>
  )
}

export const components = { a: A }
