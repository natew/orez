import * as v from '../../shared/src/valita.ts';

export const connectedBodySchema = v.object({
  wsid: v.string(),
  timestamp: v.number().optional(),
});

export const connectedMessageSchema = v.tuple([
  v.literal('connected'),
  connectedBodySchema,
]);

export type ConnectedMessage = v.Infer<typeof connectedMessageSchema>;
