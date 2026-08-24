export type DiagnosticsStatus = 'healthy' | 'attention' | 'unavailable';

export type DiagnosticsCategory = 'runtime' | 'network' | 'models' | 'security';

export interface DiagnosticsMetric {
  label: string;
  value: string;
}

export interface DiagnosticsCheck {
  id: string;
  title: string;
  category: DiagnosticsCategory;
  categoryLabel: string;
  status: DiagnosticsStatus;
  statusLabel: string;
  summary: string;
  observation: string;
  safeCode: string;
  nextStep: string;
  scopeLabel: string;
}

export interface DiagnosticsViewModel {
  schemaVersion: 'iamine.dashboard.diagnostics.preview-0.1';
  title: string;
  subtitle: string;
  provenance: {
    label: string;
    authoritative: false;
  };
  metrics: readonly DiagnosticsMetric[];
  checks: readonly DiagnosticsCheck[];
}

export interface DiagnosticsDataSource {
  kind: 'mock';
  load: () => Promise<DiagnosticsViewModel | null>;
}
