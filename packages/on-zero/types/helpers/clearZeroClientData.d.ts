export type ClearZeroClientDataOptions = {
    /** close the zero instance before clearing */
    closeZero?: () => Promise<void>;
    /** called with info about what was cleared */
    onCleared?: (info: {
        count: number;
        names: string[];
    }) => void;
    /** called on error */
    onError?: (error: unknown) => void;
    /** reload the page after clearing (default: true) */
    reload?: boolean;
    /** delay before reload in ms (default: 1000) */
    reloadDelay?: number;
};
export declare function clearZeroClientData(options?: ClearZeroClientDataOptions): Promise<void>;
//# sourceMappingURL=clearZeroClientData.d.ts.map