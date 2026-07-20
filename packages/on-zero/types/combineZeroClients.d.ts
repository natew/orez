import { type Emitter } from './helpers/emitter';
import { run } from './run';
import type { ZeroEvent } from './types';
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
    zeroEvents: Emitter<ZeroEvent | null>;
    ControlQueries: (props: ControlQueriesProps) => ReactNode;
};
export type CombineZeroClientsOptions = {
    inner?: string;
};
type UnionToIntersection<U> = (U extends any ? (x: U) => void : never) extends (x: infer I) => void ? I : never;
export type CombinedZeroClients<Clients extends readonly CombinableZeroClient[]> = {
    useQuery: UnionToIntersection<Clients[number]['useQuery']>;
    usePermission: UnionToIntersection<Clients[number]['usePermission']>;
    zero: UnionToIntersection<Clients[number]['zero']>;
    preload: UnionToIntersection<Clients[number]['preload']>;
    getQuery: UnionToIntersection<Clients[number]['getQuery']>;
    run: typeof run;
    zeroEvents: Emitter<ZeroEvent | null>;
    ControlQueries: (props: ControlQueriesProps) => ReactNode;
};
export declare function combineZeroClients<const Clients extends readonly [CombinableZeroClient, ...CombinableZeroClient[]]>(...clientsAndOptions: [...Clients] | [...Clients, CombineZeroClientsOptions]): CombinedZeroClients<Clients>;
export {};
//# sourceMappingURL=combineZeroClients.d.ts.map