import { type ClearZeroClientDataOptions } from './clearZeroClientData';
export type ZeroClientErrorInfo = {
    key: string;
    title: string;
    description: string;
    /** reload the page (marks error timestamp) */
    reload: () => void;
    /** true if a recent reload already happened (user should be offered a hard reset) */
    shouldOfferReset: boolean;
    /** clear client data and reload */
    reset: () => Promise<void>;
};
export type ShowZeroClientErrorOptions = {
    key?: string;
    title?: string;
    description: string;
    /** app-specific handler — receives error info and action helpers */
    onError: (info: ZeroClientErrorInfo) => void;
    /** options passed to clearZeroClientData when reset is triggered */
    clearOptions?: ClearZeroClientDataOptions;
};
export declare function showZeroClientErrorOnce({ key, title, description, onError, clearOptions, }: ShowZeroClientErrorOptions): void;
export declare function resetShownZeroClientError(key?: string): void;
//# sourceMappingURL=showZeroClientError.d.ts.map