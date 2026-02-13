/**
 * sqlite-mode module - unified sqlite mode handling for orez
 *
 * this module provides:
 * - type definitions for sqlite modes
 * - mode resolution from config
 * - shim generation (single source of truth)
 * - safe mode application with backup/restore
 */

export * from './types.js'
export * from './resolve-mode.js'
export * from './apply-mode.js'
export * from './shim-template.js'
