import type { NodesDataSource } from '../contracts/view-models/nodes';
import { nodesMockViewModel } from './nodesFixtures';

export const nodesMockDataSource: NodesDataSource = {
  kind: 'mock',
  load: () => Promise.resolve(nodesMockViewModel),
};
