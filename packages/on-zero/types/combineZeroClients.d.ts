import { run } from './run';
import type { CombinedMutationLifecycle, MutationLifecycle } from './helpers/mutationLifecycle';
import type { ZeroEventsEmitter } from './types';
import type { ReactNode } from 'react';
type ControlQueriesProps = {
    children: ReactNode;
    action?: 'enable' | 'disable';
    whenDisabled?: 'empty' | 'last-value';
};
type CombinableZeroClient = {
    instanceName: string;
    useQuery: (...args: any[]) => any;
    useQueryDirect: (...args: any[]) => any;
    usePermission: (...args: any[]) => any;
    usePermissionDirect: (...args: any[]) => any;
    zero: any;
    preload: (...args: any[]) => any;
    getQuery: (...args: any[]) => any;
    mutationLifecycle: MutationLifecycle;
    zeroEvents: ZeroEventsEmitter;
    reloadPage: () => boolean;
    ControlQueries: (props: ControlQueriesProps) => ReactNode;
};
export type CombineZeroClientsOptions = {
    inner?: string;
};
type UnionToIntersection<U> = (U extends any ? (x: U) => void : never) extends (x: infer I) => void ? I : never;
type WithoutMutate<Zero> = Zero extends unknown ? Omit<Zero, 'mutate'> : never;
type MutationsOf<Zero> = Zero extends {
    mutate: infer Mutations;
} ? Mutations : never;
type CombinedZero<Clients extends readonly CombinableZeroClient[]> = UnionToIntersection<WithoutMutate<Clients[number]['zero']>> & {
    mutate: UnionToIntersection<MutationsOf<Clients[number]['zero']>>;
};
export type CombinedZeroClients<Clients extends readonly CombinableZeroClient[]> = {
    useQuery: UnionToIntersection<Clients[number]['useQuery']>;
    usePermission: UnionToIntersection<Clients[number]['usePermission']>;
    zero: CombinedZero<Clients>;
    preload: UnionToIntersection<Clients[number]['preload']>;
    getQuery: UnionToIntersection<Clients[number]['getQuery']>;
    run: typeof run;
    zeroEvents: ZeroEventsEmitter;
    reloadPage: () => boolean;
    ControlQueries: (props: ControlQueriesProps) => ReactNode;
} & CombinedMutationLifecycle;
export declare function combineZeroClients<const Clients extends readonly [CombinableZeroClient, ...CombinableZeroClient[]]>(...clientsAndOptions: [...Clients] | [...Clients, CombineZeroClientsOptions]): CombinedZeroClients<Clients>;
export {};
//# sourceMappingURL=combineZeroClients.d.ts.map