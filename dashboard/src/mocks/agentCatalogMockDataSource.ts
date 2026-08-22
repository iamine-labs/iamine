import type { AgentCatalogDataSource } from '../contracts/view-models/agentCatalog';
import { agentCatalogMockViewModel } from './agentCatalogFixtures';

export const agentCatalogMockDataSource: AgentCatalogDataSource = {
  kind: 'mock',
  load: () => Promise.resolve(agentCatalogMockViewModel),
};
