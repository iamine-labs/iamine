export type OverviewMetricTone = 'blue' | 'copper' | 'green' | 'red' | 'teal';

export interface OverviewFact {
  label: string;
  value: string;
}

export interface OverviewResourceMetric extends OverviewFact {
  percent: number;
  detail: string;
  tone: Exclude<OverviewMetricTone, 'red'>;
}

export interface OverviewTrendMetric {
  label: string;
  count: string;
  tone: Exclude<OverviewMetricTone, 'teal'>;
  points: readonly number[];
}

export interface OverviewActivityItem {
  time: string;
  event: string;
}

export interface OverviewViewModel {
  provenance: {
    kind: 'mock';
    label: string;
    authoritative: false;
  };
  operational: {
    title: string;
    summary: string;
    online: boolean;
  };
  localNode: {
    name: string;
    statusLabel: string;
    facts: readonly OverviewFact[];
  };
  resources: {
    metrics: readonly OverviewResourceMetric[];
    allocationLabel: string;
  };
  activeAgent: {
    name: string;
    stateLabel: string;
    operationLabel: string;
    facts: readonly OverviewFact[];
    progressPercent: number;
  };
  queue: {
    items: readonly OverviewTrendMetric[];
    capacityLabel: string;
  };
  nodeStatus: {
    items: readonly (OverviewFact & {
      tone: Exclude<OverviewMetricTone, 'teal'>;
    })[];
  };
  traffic: {
    periodLabel: string;
    totals: readonly OverviewFact[];
    inbound: readonly number[];
    outbound: readonly number[];
  };
  inferences: {
    periodLabel: string;
    total: number;
    completed: number;
    pending: number;
    failed: number;
  };
  activity: readonly OverviewActivityItem[];
  logs: readonly string[];
}

export interface OverviewDataSource {
  readonly kind: 'mock';
  load: () => Promise<OverviewViewModel | null>;
}
