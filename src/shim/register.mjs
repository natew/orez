// --import entrypoint that registers the esm loader hooks.
// __HOOKS_URL__ is replaced at runtime by orez before writing to tmpdir.
import { register } from 'node:module';
register('__HOOKS_URL__', import.meta.url);
