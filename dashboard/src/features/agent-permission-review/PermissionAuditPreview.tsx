import { CircleDot, ShieldCheck, ShieldX } from 'lucide-react';

import type {
  AgentPermissionAuditEvent,
  AgentPermissionDecision,
} from '../../contracts/view-models/agentPermissionReview';
import styles from './PermissionAuditPreview.module.css';

function decisionEvent(
  decision: AgentPermissionDecision,
): AgentPermissionAuditEvent | null {
  if (decision === 'pending') return null;

  return {
    sequenceLabel: '02',
    eventLabel:
      decision === 'confirmed'
        ? 'Preview confirmation recorded'
        : 'Preview denial recorded',
    outcomeLabel: 'Not persisted · not emitted · no authority',
  };
}

export function PermissionAuditPreview({
  baselineEvents,
  decision,
}: {
  baselineEvents: readonly AgentPermissionAuditEvent[];
  decision: AgentPermissionDecision;
}) {
  const outcomeEvent = decisionEvent(decision);
  const events = outcomeEvent
    ? [...baselineEvents, outcomeEvent]
    : baselineEvents;

  return (
    <section className={styles.section} aria-labelledby="audit-preview">
      <header>
        <div>
          <span>Audit projection</span>
          <h3 id="audit-preview">Preview event sequence</h3>
        </div>
        <span>{events.length} events</span>
      </header>

      <ol>
        {events.map((event) => {
          const outcome = event.sequenceLabel === '02';
          const Icon = outcome
            ? decision === 'confirmed'
              ? ShieldCheck
              : ShieldX
            : CircleDot;

          return (
            <li key={`${event.sequenceLabel}-${event.eventLabel}`}>
              <span className={styles.sequence}>{event.sequenceLabel}</span>
              <span
                className={styles.eventIcon}
                data-decision={outcome ? decision : 'pending'}
                aria-hidden="true"
              >
                <Icon size={15} />
              </span>
              <div>
                <strong>{event.eventLabel}</strong>
                <span>{event.outcomeLabel}</span>
              </div>
            </li>
          );
        })}
      </ol>
    </section>
  );
}
