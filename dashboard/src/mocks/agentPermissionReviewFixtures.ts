import type { AgentPermissionReviewViewModel } from '../contracts/view-models/agentPermissionReview';

type FixtureInput = Pick<
  AgentPermissionReviewViewModel,
  | 'agentId'
  | 'agentName'
  | 'operationLabel'
  | 'summary'
  | 'permissionProfileLabel'
  | 'risk'
  | 'riskLabel'
  | 'permissions'
>;

function createFixture(input: FixtureInput): AgentPermissionReviewViewModel {
  return {
    schemaVersion: 'iamine.dashboard.agent-permission-review.preview-0.1',
    title: 'Permission review',
    subtitle: 'Requested access and blocked boundaries for operator review.',
    provenance: {
      label: 'Preview decision; no authorization issued',
      authoritative: false,
    },
    authority: {
      persisted: false,
      emitted: false,
      containsPayload: false,
      authorizesAction: false,
    },
    facts: [
      { label: 'Default policy', value: 'Deny' },
      { label: 'Local gate', value: 'Preview only' },
      { label: 'Audit', value: 'Not persisted' },
      { label: 'Runtime', value: 'Blocked' },
    ],
    auditEvents: [
      {
        sequenceLabel: '01',
        eventLabel: 'Request displayed',
        outcomeLabel: 'Review pending',
      },
    ],
    ...input,
  };
}

export const agentPermissionReviewFixtures: Readonly<
  Record<string, AgentPermissionReviewViewModel>
> = {
  'node-doctor': createFixture({
    agentId: 'node-doctor',
    agentName: 'Node Doctor',
    operationLabel: 'Review diagnostic evidence',
    summary:
      'Review operator-approved diagnostic evidence and produce a redacted report preview.',
    permissionProfileLabel: 'Local read-only',
    risk: 'low',
    riskLabel: 'Low risk preview',
    permissions: [
      {
        id: 'diagnostic-evidence',
        categoryLabel: 'Diagnostic evidence',
        accessLabel: 'Read-only',
        scopeLabel: 'Operator-approved redacted summary',
        disposition: 'requested',
        dispositionLabel: 'Requested',
      },
      {
        id: 'network',
        categoryLabel: 'Network',
        accessLabel: 'None',
        scopeLabel: 'No network or worker startup',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
      {
        id: 'process',
        categoryLabel: 'Process and services',
        accessLabel: 'None',
        scopeLabel: 'No shell, process, or service mutation',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
    ],
  }),
  'support-reporter': createFixture({
    agentId: 'support-reporter',
    agentName: 'Privacy-Safe Reporter',
    operationLabel: 'Prepare support report',
    summary:
      'Format operator-approved evidence codes into a bounded support report preview.',
    permissionProfileLabel: 'Local read-only',
    risk: 'low',
    riskLabel: 'Low risk preview',
    permissions: [
      {
        id: 'redacted-evidence',
        categoryLabel: 'Redacted evidence',
        accessLabel: 'Read-only',
        scopeLabel: 'Operator-approved evidence codes only',
        disposition: 'requested',
        dispositionLabel: 'Requested',
      },
      {
        id: 'report-output',
        categoryLabel: 'Report output',
        accessLabel: 'Preview',
        scopeLabel: 'No export or upload',
        disposition: 'requested',
        dispositionLabel: 'Requested',
      },
      {
        id: 'network',
        categoryLabel: 'Network',
        accessLabel: 'None',
        scopeLabel: 'No remote support submission',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
    ],
  }),
  'lan-file-share': createFixture({
    agentId: 'lan-file-share',
    agentName: 'LAN File Share Assistant',
    operationLabel: 'Prepare local share plan',
    summary:
      'Organize operator-provided share requirements into a bounded planning preview.',
    permissionProfileLabel: 'Local planning',
    risk: 'moderate',
    riskLabel: 'Moderate risk preview',
    permissions: [
      {
        id: 'operator-input',
        categoryLabel: 'Operator input',
        accessLabel: 'Read-only',
        scopeLabel: 'Share requirements supplied in the review',
        disposition: 'requested',
        dispositionLabel: 'Requested',
      },
      {
        id: 'filesystem',
        categoryLabel: 'Filesystem',
        accessLabel: 'None',
        scopeLabel: 'No file or credential access',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
      {
        id: 'network',
        categoryLabel: 'Network',
        accessLabel: 'None',
        scopeLabel: 'No discovery or share creation',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
    ],
  }),
  'photo-library-organizer': createFixture({
    agentId: 'photo-library-organizer',
    agentName: 'Photo Library Organizer',
    operationLabel: 'Prepare library plan',
    summary:
      'Review operator-provided library goals and prepare an organization preview.',
    permissionProfileLabel: 'Local planning',
    risk: 'moderate',
    riskLabel: 'Moderate risk preview',
    permissions: [
      {
        id: 'operator-input',
        categoryLabel: 'Operator input',
        accessLabel: 'Read-only',
        scopeLabel: 'Selected-library description only',
        disposition: 'requested',
        dispositionLabel: 'Requested',
      },
      {
        id: 'media-library',
        categoryLabel: 'Media library',
        accessLabel: 'None',
        scopeLabel: 'No media inspection or analysis',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
      {
        id: 'filesystem',
        categoryLabel: 'Filesystem mutation',
        accessLabel: 'None',
        scopeLabel: 'No rename, move, or delete action',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
    ],
  }),
  'home-network-assistant': createFixture({
    agentId: 'home-network-assistant',
    agentName: 'Home Network Assistant',
    operationLabel: 'Prepare troubleshooting plan',
    summary:
      'Review operator-provided symptoms and prepare bounded diagnostic steps.',
    permissionProfileLabel: 'Local planning',
    risk: 'moderate',
    riskLabel: 'Moderate risk preview',
    permissions: [
      {
        id: 'operator-input',
        categoryLabel: 'Operator input',
        accessLabel: 'Read-only',
        scopeLabel: 'Reported network symptoms only',
        disposition: 'requested',
        dispositionLabel: 'Requested',
      },
      {
        id: 'network',
        categoryLabel: 'Network',
        accessLabel: 'None',
        scopeLabel: 'No discovery or router access',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
      {
        id: 'configuration',
        categoryLabel: 'Configuration',
        accessLabel: 'None',
        scopeLabel: 'No device or network changes',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
    ],
  }),
  'windows-optimizer': createFixture({
    agentId: 'windows-optimizer',
    agentName: 'Windows Optimizer Assistant',
    operationLabel: 'Prepare optimization plan',
    summary:
      'Review operator-provided system goals and prepare conservative steps.',
    permissionProfileLabel: 'Local planning',
    risk: 'elevated',
    riskLabel: 'Elevated risk preview',
    permissions: [
      {
        id: 'operator-input',
        categoryLabel: 'Operator input',
        accessLabel: 'Read-only',
        scopeLabel: 'Optimization goals supplied in the review',
        disposition: 'requested',
        dispositionLabel: 'Requested',
      },
      {
        id: 'process',
        categoryLabel: 'Processes and services',
        accessLabel: 'None',
        scopeLabel: 'No process, service, or registry access',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
      {
        id: 'system-mutation',
        categoryLabel: 'System mutation',
        accessLabel: 'None',
        scopeLabel: 'No configuration or system changes',
        disposition: 'blocked',
        dispositionLabel: 'Blocked',
      },
    ],
  }),
};
