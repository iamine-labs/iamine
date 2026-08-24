import type { AgentPermissionReviewDataSource } from '../contracts/view-models/agentPermissionReview';
import { agentPermissionReviewFixtures } from './agentPermissionReviewFixtures';

export const agentPermissionReviewMockDataSource: AgentPermissionReviewDataSource =
  {
    kind: 'mock',
    load: (agentId) =>
      Promise.resolve(agentPermissionReviewFixtures[agentId] ?? null),
  };
