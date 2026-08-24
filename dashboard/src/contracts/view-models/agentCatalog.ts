export type AgentCatalogStage = 'reference' | 'next' | 'planned';

export interface AgentCatalogMetric {
  label: string;
  value: string;
}

export interface AgentCatalogEntry {
  id: string;
  name: string;
  description: string;
  stage: AgentCatalogStage;
  stageLabel: string;
  roleLabel: string;
  operatingMode: string;
  platformLabel: string;
  packageStage: string;
  capabilities: readonly string[];
  boundaries: readonly string[];
}

export interface AgentCatalogViewModel {
  schemaVersion: 'iamine.dashboard.agent-catalog.preview-0.1';
  title: string;
  subtitle: string;
  provenance: {
    label: string;
    authoritative: false;
  };
  metrics: readonly AgentCatalogMetric[];
  agents: readonly AgentCatalogEntry[];
}

export interface AgentCatalogDataSource {
  kind: 'mock';
  load: () => Promise<AgentCatalogViewModel | null>;
}
