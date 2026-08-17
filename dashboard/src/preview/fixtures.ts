export type DashboardView =
  'overview' | 'agents' | 'nodes' | 'models' | 'activity' | 'marketplace';

export interface NavigationItem {
  id: DashboardView;
  label: string;
}

export interface ResourceMetric {
  label: string;
  value: number;
  amount: string;
  detail: string;
  tone: 'copper' | 'blue' | 'green' | 'teal';
}

export interface ActivityEntry {
  time: string;
  event: string;
}

export const navigationItems: readonly NavigationItem[] = [
  { id: 'overview', label: 'Overview' },
  { id: 'agents', label: 'Agents' },
  { id: 'nodes', label: 'Nodes' },
  { id: 'models', label: 'Models' },
  { id: 'activity', label: 'Activity' },
  { id: 'marketplace', label: 'Marketplace' },
];

export const resourceMetrics: readonly ResourceMetric[] = [
  { label: 'GPU', value: 74, amount: '74%', detail: '42 C', tone: 'copper' },
  {
    label: 'VRAM',
    value: 60,
    amount: '11.2 / 16 GB',
    detail: '70%',
    tone: 'copper',
  },
  { label: 'CPU', value: 42, amount: '42%', detail: '16 cores', tone: 'blue' },
  {
    label: 'RAM',
    value: 42,
    amount: '13.4 / 32 GB',
    detail: '42%',
    tone: 'teal',
  },
  {
    label: 'Storage',
    value: 60,
    amount: '1.2 / 2 TB',
    detail: '60%',
    tone: 'green',
  },
];

export const activityEntries: readonly ActivityEntry[] = [
  { time: '12:45', event: 'Inference completed #A098 - Coder Agent - 0.8s' },
  { time: '12:44', event: 'Node NODE-OMEGA-07 connected' },
  { time: '12:43', event: 'Model IAMINE Vision 68 loaded successfully' },
  { time: '12:43', event: 'Vision Agent updated to v1.3.2' },
];

export const systemLogs = [
  '12:45:12 INFO  inference.completed id=A098 agent=coder time=0.8s',
  '12:44:03 INFO  node.connected node=NODE-OMEGA-07 ip=redacted',
  '12:43:11 INFO  model.loaded model=iamine-vision-68',
  '12:42:22 WARN  high_vram_usage usage=70% temp=70C',
] as const;

export const trafficSeries = {
  inbound: [18, 22, 20, 28, 24, 35, 31, 44, 40, 52, 46, 58],
  outbound: [12, 15, 13, 19, 17, 25, 22, 29, 28, 35, 33, 38],
} as const;

export const queueSeries = {
  running: [10, 12, 11, 15, 14, 18, 17, 21],
  pending: [8, 9, 7, 10, 8, 12, 11, 14],
  completed: [4, 7, 9, 12, 15, 19, 24, 28],
  failed: [7, 5, 6, 4, 5, 3, 4, 2],
} as const;
