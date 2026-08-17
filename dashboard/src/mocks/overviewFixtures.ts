import type { OverviewViewModel } from '../contracts/view-models/overview';

export const overviewMockViewModel: OverviewViewModel = {
  provenance: {
    kind: 'mock',
    label: 'Mock Overview; not authoritative',
    authoritative: false,
  },
  operational: {
    title: 'System operational',
    summary: '24 nodes online · 18 active agents · 98.6% availability',
    online: true,
  },
  localNode: {
    name: 'NODE-LOCAL-01',
    statusLabel: 'Online',
    facts: [
      { label: 'Uptime', value: '12d 6h 24m' },
      { label: 'Version', value: '1.2.0-beta' },
      { label: 'Role', value: 'Compute · Inference' },
    ],
  },
  resources: {
    metrics: [
      {
        label: 'GPU',
        percent: 74,
        value: '74%',
        detail: '42 C',
        tone: 'copper',
      },
      {
        label: 'VRAM',
        percent: 60,
        value: '11.2 / 16 GB',
        detail: '70%',
        tone: 'copper',
      },
      {
        label: 'CPU',
        percent: 42,
        value: '42%',
        detail: '16 cores',
        tone: 'blue',
      },
      {
        label: 'RAM',
        percent: 42,
        value: '13.4 / 32 GB',
        detail: '42%',
        tone: 'teal',
      },
      {
        label: 'Storage',
        percent: 60,
        value: '1.2 / 2 TB',
        detail: '60%',
        tone: 'green',
      },
    ],
    allocationLabel: 'Network allocation 50%',
  },
  activeAgent: {
    name: 'Coder Agent',
    stateLabel: 'Inference',
    operationLabel: 'Inference #A492',
    facts: [
      { label: 'Model', value: 'StarCoder' },
      { label: 'Elapsed', value: '00:00:51.8' },
      { label: 'Origin node', value: 'NODE-DELTA-09' },
    ],
    progressPercent: 68,
  },
  queue: {
    items: [
      {
        label: 'Running',
        count: '3',
        tone: 'copper',
        points: [10, 12, 11, 15, 14, 18, 17, 21],
      },
      {
        label: 'Pending',
        count: '12',
        tone: 'blue',
        points: [8, 9, 7, 10, 8, 12, 11, 14],
      },
      {
        label: 'Completed',
        count: '148',
        tone: 'green',
        points: [4, 7, 9, 12, 15, 19, 24, 28],
      },
      {
        label: 'Failed',
        count: '2',
        tone: 'red',
        points: [7, 5, 6, 4, 5, 3, 4, 2],
      },
    ],
    capacityLabel: 'Capacity within preview threshold',
  },
  nodeStatus: {
    items: [
      { label: 'Online', value: '21', tone: 'green' },
      { label: 'Degraded', value: '2', tone: 'copper' },
      { label: 'Maintenance', value: '1', tone: 'blue' },
      { label: 'Offline', value: '0', tone: 'red' },
    ],
  },
  traffic: {
    periodLabel: '24h',
    totals: [
      { label: 'Inbound', value: '2.34 TB' },
      { label: 'Outbound', value: '1.78 TB' },
    ],
    inbound: [18, 22, 20, 28, 24, 35, 31, 44, 40, 52, 46, 58],
    outbound: [12, 15, 13, 19, 17, 25, 22, 29, 28, 35, 33, 38],
  },
  inferences: {
    periodLabel: '24h',
    total: 1264,
    completed: 1048,
    pending: 164,
    failed: 52,
  },
  activity: [
    { time: '12:45', event: 'Inference completed #A098 - Coder Agent - 0.8s' },
    { time: '12:44', event: 'Node NODE-OMEGA-07 connected' },
    { time: '12:43', event: 'Model IAMINE Vision 68 loaded successfully' },
    { time: '12:43', event: 'Vision Agent updated to v1.3.2' },
  ],
  logs: [
    '12:45:12 INFO  inference.completed id=A098 agent=coder time=0.8s',
    '12:44:03 INFO  node.connected node=NODE-OMEGA-07 ip=redacted',
    '12:43:11 INFO  model.loaded model=iamine-vision-68',
    '12:42:22 WARN  high_vram_usage usage=70% temp=70C',
  ],
};
