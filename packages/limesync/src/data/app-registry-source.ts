export function addAppRegistryModules(files: Record<string, string>, source?: string) {
  if (source) {
    files['schema.ts'] = `import { createSchema } from "limesync/zero"
import * as source from ${JSON.stringify(source)}
export const schema = createSchema(source.schema)
`
  }
  const aggregates = files['aggregates.ts']
    ? 'import { aggregates } from "./aggregates"\n'
    : ''
  files['app.ts'] = `import { createDataApp } from 'limesync/data'
import { schema } from './schema'
import { models } from './models'
import * as groupedQueries from './groupedQueries'
${aggregates}export const app = createDataApp({ schema, models, groupedQueries${aggregates ? ', aggregates' : ''} })
export { schema, models, groupedQueries }
export { instances } from './instances'
`
  const validators = files['syncedMutations.ts']
    ? 'import { mutationValidators } from "./syncedMutations"\n'
    : 'const mutationValidators = {}\n'
  files['app-server.ts'] = `import { schema } from './schema'
import { models } from './models'
import { queries } from './syncedQueries'
${validators}export const app = { schema, models, queries, mutationValidators }
export { queries, mutationValidators }
`
}
