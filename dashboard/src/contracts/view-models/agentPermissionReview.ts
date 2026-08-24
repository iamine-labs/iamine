export type AgentPermissionDisposition = 'requested' | 'blocked';
export type AgentPermissionDecision = 'pending' | 'confirmed' | 'denied';
export type AgentPermissionRisk = 'low' | 'moderate' | 'elevated';

export interface AgentPermissionFact {
  label: string;
  value: string;
}

export interface AgentPermissionItem {
  id: string;
  categoryLabel: string;
  accessLabel: string;
  scopeLabel: string;
  disposition: AgentPermissionDisposition;
  dispositionLabel: string;
}

export interface AgentPermissionAuditEvent {
  sequenceLabel: string;
  eventLabel: string;
  outcomeLabel: string;
}

export interface AgentPermissionReviewViewModel {
  schemaVersion: 'iamine.dashboard.agent-permission-review.preview-0.1';
  agentId: string;
  agentName: string;
  title: string;
  subtitle: string;
  operationLabel: string;
  summary: string;
  permissionProfileLabel: string;
  risk: AgentPermissionRisk;
  riskLabel: string;
  provenance: {
    label: string;
    authoritative: false;
  };
  authority: {
    persisted: false;
    emitted: false;
    containsPayload: false;
    authorizesAction: false;
  };
  facts: readonly AgentPermissionFact[];
  permissions: readonly AgentPermissionItem[];
  auditEvents: readonly AgentPermissionAuditEvent[];
}

export interface AgentPermissionReviewDataSource {
  kind: 'mock';
  load: (agentId: string) => Promise<AgentPermissionReviewViewModel | null>;
}
