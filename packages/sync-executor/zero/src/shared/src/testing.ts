import {assert} from './asserts.ts';

export const TESTING = false;

export function assertTesting(msg = 'Expected to be in test mode'): void {
  assert(TESTING, msg);
}
