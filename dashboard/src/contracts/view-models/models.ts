export type ModelPreviewState = 'shown' | 'attention' | 'unavailable';

export type ModelPreviewCategory =
  'general' | 'reasoning' | 'code' | 'multimodal';

export interface ModelPreviewMetric {
  label: string;
  value: string;
}

export interface ModelPreviewLabel {
  id: string;
  label: string;
}

export interface ModelPreviewItem {
  id: string;
  name: string;
  category: ModelPreviewCategory;
  categoryLabel: string;
  previewState: ModelPreviewState;
  previewStateLabel: string;
  summary: string;
  displayClassLabel: string;
  sourceLabel: string;
  artifactLabel: 'Not represented';
  useLabels: readonly ModelPreviewLabel[];
  notes: readonly string[];
}

export interface ModelsViewModel {
  schemaVersion: 'iamine.dashboard.models.preview-0.1';
  title: string;
  subtitle: string;
  provenance: {
    label: string;
    authoritative: false;
  };
  metrics: readonly ModelPreviewMetric[];
  models: readonly ModelPreviewItem[];
}

export interface ModelsDataSource {
  readonly kind: 'mock';
  load: () => Promise<ModelsViewModel | null>;
}
