type MutatorResultLike = {
    client: Promise<unknown>;
    server: Promise<unknown>;
};
export type MutationError = {
    scope: 'client' | 'server';
    kind: 'app' | 'zero';
    message: string;
    details?: unknown;
};
export type MutationState = {
    pending: boolean;
    error: MutationError | null;
    reset: () => void;
};
export declare function onMutationError(cb: (error: MutationError) => void): () => void;
/**
 * Wire a mutation result's optimistic-client and authoritative-server phases to
 * normalized errors, without awaiting either. Every error reaches the global
 * `onMutationError` catch (deduped — client and server can surface the same
 * failure); an optional `onError` sink receives them too (the hook uses it for
 * local state). Use this directly for a fire-and-forget call outside React:
 *
 *   observeMutation(zero.mutate.post.delete({ id }))
 *
 * Resolves once both phases settle. Never rejects.
 */
export declare function observeMutation(result: MutatorResultLike, onError?: (error: MutationError) => void): Promise<void>;
/**
 * Bind one Zero mutator to local pending/error state without ever awaiting it.
 *
 *   const [insertPost, state] = useMutation(zero.mutate.post.insert)
 *   insertPost({ ... })          // fires optimistically, returns immediately
 *   state.error                  // render inline; client OR server failures land here
 *   state.pending                // only to guard a re-submit, not to gate the UI
 *
 * The returned mutator has the exact same signature as the one passed in, so arg
 * types are preserved. It returns Zero's native `{ client, server }` for the rare
 * authoritative-wait escape hatch — product code should not await it. Every error
 * also flows to `onMutationError` so a fire-and-forget call is never silent.
 *
 * For N writes use one custom mutator that loops `tx.mutate` in a single
 * transaction, not N calls — that keeps it atomic and a single state.
 */
export declare function useMutation<Fn extends (...args: any[]) => MutatorResultLike>(mutator: Fn): [Fn, MutationState];
export {};
//# sourceMappingURL=useMutation.d.ts.map