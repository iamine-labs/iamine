export type NodePreviewStatus = 'available' | 'limited' | 'offline';

export type NodePreviewCapability = 'compute' | 'acceleration' | 'storage';

export interface NodePreviewMetric {
  label: string;
  value: string;
}

export interface NodePreviewCapabilityLabel {
  id: NodePreviewCapability;
  label: string;
}

export interface NodePreviewNode {
  id: string;
  name: string;
  roleLabel: string;
  environmentLabel: string;
  status: NodePreviewStatus;
  statusLabel: string;
  summary: string;
  capacityLabel: string;
  visibilityLabel: string;
  capabilities: readonly NodePreviewCapabilityLabel[];
  notes: readonly string[];
}

export interface NodesViewModel {
  schemaVersion: 'iamine.dashboard.nodes.preview-0.1';
  title: string;
  subtitle: string;
  provenance: {
    label: string;
    authoritative: false;
  };
  metrics: readonly NodePreviewMetric[];
  nodes: readonly NodePreviewNode[];
}

export interface NodesDataSource {
  readonly kind: 'mock';
  load: () => Promise<NodesViewModel | null>;
}
