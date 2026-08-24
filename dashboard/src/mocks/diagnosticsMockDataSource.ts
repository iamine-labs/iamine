import type { DiagnosticsDataSource } from '../contracts/view-models/diagnostics';
import { diagnosticsMockViewModel } from './diagnosticsFixtures';

export const diagnosticsMockDataSource: DiagnosticsDataSource = {
  kind: 'mock',
  load: () => Promise.resolve(diagnosticsMockViewModel),
};
