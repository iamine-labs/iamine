import type { ActivityDataSource } from '../contracts/view-models/activity';
import { activityMockViewModel } from './activityFixtures';

export const activityMockDataSource: ActivityDataSource = {
  kind: 'mock',
  load: () => Promise.resolve(activityMockViewModel),
};
