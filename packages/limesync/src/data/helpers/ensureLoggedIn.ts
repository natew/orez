import { ensure } from './ensure.js'
import { getAuth } from './getAuth.js'

import type { AuthData } from '../types.js'

// the mutator context is ambient only where the runtime can scope it, which is
// the server. on the client and in react native a mutator's authData is the
// same value getAuth() resolves — createMutators seeds the client context from
// it — so reading through getAuth keeps this working in mutators on every
// environment.
export const ensureLoggedIn = (): AuthData => {
  const authData = getAuth()
  ensure(authData, 'logged in')
  return authData
}
