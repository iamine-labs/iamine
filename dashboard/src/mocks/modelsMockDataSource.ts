import type { ModelsDataSource } from '../contracts/view-models/models';
import { modelsMockViewModel } from './modelsFixtures';

export const modelsMockDataSource: ModelsDataSource = {
  kind: 'mock',
  load: () => Promise.resolve(modelsMockViewModel),
};
