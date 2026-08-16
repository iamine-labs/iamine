import type { StatusTone } from '../components';

export interface PreviewCheck {
  id: string;
  check: string;
  source: string;
  status: string;
  tone: StatusTone;
  duration: string;
}

export const previewChecks: readonly PreviewCheck[] = [
  {
    id: 'check-network',
    check: 'Local transport',
    source: 'Fixture A',
    status: 'Available',
    tone: 'success',
    duration: '18 ms',
  },
  {
    id: 'check-models',
    check: 'Model inventory',
    source: 'Fixture B',
    status: 'Attention',
    tone: 'warning',
    duration: '42 ms',
  },
  {
    id: 'check-agents',
    check: 'Agent catalog',
    source: 'Fixture C',
    status: 'Unavailable',
    tone: 'neutral',
    duration: '—',
  },
];
