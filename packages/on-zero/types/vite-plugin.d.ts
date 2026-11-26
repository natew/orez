import { type GenerateOptions } from './generate';
import type { Plugin } from 'vite';
export interface OnZeroPluginOptions extends Omit<GenerateOptions, 'dir' | 'silent'> {
    /** base data directory. defaults to src/data */
    dir?: string;
    /** additional paths to apply HMR fix to */
    hmrInclude?: string[];
    /** disable code generation (HMR only) */
    disableGenerate?: boolean;
}
export declare function onZeroPlugin(options?: OnZeroPluginOptions): Plugin[];
export declare const onZeroHmrPlugin: (options?: {
    include?: string[];
}) => Plugin;
export default onZeroPlugin;
//# sourceMappingURL=vite-plugin.d.ts.map