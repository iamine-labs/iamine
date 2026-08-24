export type ActivityPreviewSignal = 'informational' | 'attention' | 'boundary';

export type ActivityPreviewCategory = 'system' | 'agent' | 'model' | 'network';

export interface ActivityPreviewMetric {
  label: string;
  value: string;
}

export interface ActivityPreviewLabel {
  id: string;
  label: string;
}

export interface ActivityPreviewItem {
  id: string;
  name: string;
  sequenceLabel: string;
  category: ActivityPreviewCategory;
  categoryLabel: string;
  signal: ActivityPreviewSignal;
  signalLabel: string;
  summary: string;
  contextLabel: string;
  sourceLabel: string;
  detailLabels: readonly ActivityPreviewLabel[];
  notes: readonly string[];
}

export interface ActivityViewModel {
  schemaVersion: 'iamine.dashboard.activity.preview-0.1';
  title: string;
  subtitle: string;
  provenance: {
    label: string;
    authoritative: false;
  };
  metrics: readonly ActivityPreviewMetric[];
  items: readonly ActivityPreviewItem[];
}

export interface ActivityDataSource {
  readonly kind: 'mock';
  load: () => Promise<ActivityViewModel | null>;
}
