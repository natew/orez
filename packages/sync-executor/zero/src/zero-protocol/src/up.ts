import * as v from '../../shared/src/valita.ts';
import {changeDesiredQueriesMessageSchema} from './change-desired-queries.ts';
import {deleteClientsMessageSchema} from './delete-clients.ts';
import {pingMessageSchema} from './ping.ts';
import {pullRequestMessageSchema} from './pull.ts';
import {ackMutationResponsesMessageSchema, pushMessageSchema} from './push.ts';
import {updateAuthMessageSchema} from './update-auth.ts';

export const upstreamSchema = v.union(
  pingMessageSchema,
  deleteClientsMessageSchema,
  changeDesiredQueriesMessageSchema,
  pullRequestMessageSchema,
  updateAuthMessageSchema,
  pushMessageSchema,
  ackMutationResponsesMessageSchema,
);

export type Upstream = v.Infer<typeof upstreamSchema>;
