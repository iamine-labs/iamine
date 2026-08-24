import type { AgentCatalogViewModel } from '../contracts/view-models/agentCatalog';

export const agentCatalogMockViewModel: AgentCatalogViewModel = {
  schemaVersion: 'iamine.dashboard.agent-catalog.preview-0.1',
  title: 'Agent catalog',
  subtitle: 'Official roles across the current P0 preview sequence.',
  provenance: {
    label: 'Preview catalog; not local node state',
    authoritative: false,
  },
  metrics: [
    { label: 'Official roles', value: '6' },
    { label: 'Reference', value: '1' },
    { label: 'Next candidate', value: '1' },
    { label: 'Planned', value: '4' },
  ],
  agents: [
    {
      id: 'node-doctor',
      name: 'Node Doctor',
      description:
        'Formats bounded node evidence into a privacy-safe diagnostic report.',
      stage: 'reference',
      stageLabel: 'Reference',
      roleLabel: 'Node diagnostics',
      operatingMode: 'Local read-only',
      platformLabel: 'macOS and Linux',
      packageStage: 'Functional reference',
      capabilities: [
        'Review bounded node evidence',
        'Classify readiness signals',
        'Produce a redacted diagnostic report',
      ],
      boundaries: [
        'No automatic repair',
        'No network or worker startup',
        'No model execution',
      ],
    },
    {
      id: 'support-reporter',
      name: 'Privacy-Safe Reporter',
      description:
        'Turns operator-approved redacted evidence into a bounded support report.',
      stage: 'next',
      stageLabel: 'Next',
      roleLabel: 'Support reporting',
      operatingMode: 'Local read-only',
      platformLabel: 'Cross-platform',
      packageStage: 'Sequential candidate',
      capabilities: [
        'Format approved evidence codes',
        'Report missing evidence safely',
        'Request human review for unsupported claims',
      ],
      boundaries: [
        'No evidence collection',
        'No report export or upload',
        'No free-form private input',
      ],
    },
    {
      id: 'lan-file-share',
      name: 'LAN File Share Assistant',
      description:
        'Prepares bounded local share plans without reading files or credentials.',
      stage: 'planned',
      stageLabel: 'Planned',
      roleLabel: 'LAN share planning',
      operatingMode: 'Local planning',
      platformLabel: 'macOS, Linux, Windows',
      packageStage: 'Skeleton contract',
      capabilities: [
        'Explain supported share choices',
        'Prepare a bounded operator plan',
        'Identify required confirmations',
      ],
      boundaries: [
        'No file access',
        'No credential handling',
        'No share creation',
      ],
    },
    {
      id: 'photo-library-organizer',
      name: 'Photo Library Organizer',
      description:
        'Prepares privacy-safe organization plans for operator-selected libraries.',
      stage: 'planned',
      stageLabel: 'Planned',
      roleLabel: 'Library organization',
      operatingMode: 'Local planning',
      platformLabel: 'macOS and Windows',
      packageStage: 'Skeleton contract',
      capabilities: [
        'Describe organization strategies',
        'Surface duplicate-handling choices',
        'Prepare reversible plan summaries',
      ],
      boundaries: [
        'No library access',
        'No media analysis',
        'No file mutation',
      ],
    },
    {
      id: 'home-network-assistant',
      name: 'Home Network Assistant',
      description:
        'Frames local network troubleshooting plans from operator-provided facts.',
      stage: 'planned',
      stageLabel: 'Planned',
      roleLabel: 'Network guidance',
      operatingMode: 'Local planning',
      platformLabel: 'Cross-platform',
      packageStage: 'Skeleton contract',
      capabilities: [
        'Organize reported network symptoms',
        'Prepare bounded diagnostic steps',
        'Identify operator-owned decisions',
      ],
      boundaries: [
        'No network discovery',
        'No router access',
        'No configuration changes',
      ],
    },
    {
      id: 'windows-optimizer',
      name: 'Windows Optimizer Assistant',
      description:
        'Prepares conservative optimization plans without inspecting or changing Windows.',
      stage: 'planned',
      stageLabel: 'Planned',
      roleLabel: 'System planning',
      operatingMode: 'Local planning',
      platformLabel: 'Windows',
      packageStage: 'Skeleton contract',
      capabilities: [
        'Explain bounded optimization options',
        'Order reversible operator steps',
        'Highlight restart and rollback needs',
      ],
      boundaries: [
        'No system inspection',
        'No registry or service changes',
        'No process execution',
      ],
    },
  ],
};
