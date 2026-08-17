import type { OverviewDataSource } from '../contracts/view-models/overview';
import { overviewMockViewModel } from './overviewFixtures';

export const overviewMockDataSource: OverviewDataSource = {
  kind: 'mock',
  load() {
    return Promise.resolve(overviewMockViewModel);
  },
};
