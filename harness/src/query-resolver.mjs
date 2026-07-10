// Runtime-only bridge used by the production-shaped CF harness. Keeping this
// as JavaScript prevents the host package's narrower TypeScript project from
// re-checking the full Zero client fixture with a different generic context.
export { queryNameToAst } from './fixture.ts'
