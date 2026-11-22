import {callDefaultFetch} from './call-default-fetch.ts';
import type {Puller, PullerResult} from './puller.ts';
import type {PullRequest} from './sync/pull.ts';

/**
 * This creates a default puller which uses HTTP POST to send the pull request.
 */
export function getDefaultPuller(rep: {pullURL: string; auth: string}): Puller {
  async function puller(
    requestBody: PullRequest,
    requestID: string,
  ): Promise<PullerResult> {
    const [response, httpRequestInfo] = await callDefaultFetch(
      rep.pullURL,
      rep.auth,
      requestID,
      requestBody,
    );
    if (!response) {
      return {httpRequestInfo};
    }

    return {
      response: await response.json(),
      httpRequestInfo,
    };
  }

  return puller;
}
