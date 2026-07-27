import { test, expect } from '@playwright/test'

import { clearTodos } from './helpers'

const API = process.env.BASE_URL || 'http://localhost:3457'

test.beforeEach(async () => {
  await clearTodos(API)
})
