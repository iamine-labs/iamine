import { RotateCcw, ShieldCheck, ShieldX } from 'lucide-react';

import { Button, StatusBadge, type StatusTone } from '../../components';
import type { AgentPermissionDecision } from '../../contracts/view-models/agentPermissionReview';
import styles from './PermissionDecisionPanel.module.css';

const decisionLabels: Record<
  AgentPermissionDecision,
  { label: string; tone: StatusTone }
> = {
  pending: { label: 'Pending review', tone: 'warning' },
  confirmed: { label: 'Confirmed preview', tone: 'info' },
  denied: { label: 'Denied preview', tone: 'danger' },
};

interface PermissionDecisionPanelProps {
  acknowledged: boolean;
  decision: AgentPermissionDecision;
  onAcknowledgedChange: (acknowledged: boolean) => void;
  onConfirm: () => void;
  onDeny: () => void;
  onReset: () => void;
}

export function PermissionDecisionPanel({
  acknowledged,
  decision,
  onAcknowledgedChange,
  onConfirm,
  onDeny,
  onReset,
}: PermissionDecisionPanelProps) {
  const terminal = decision !== 'pending';
  const outcome = decisionLabels[decision];

  return (
    <section className={styles.panel} aria-labelledby="permission-decision">
      <header className={styles.header}>
        <div>
          <span>Local preview</span>
          <h3 id="permission-decision">Decision</h3>
        </div>
        <StatusBadge tone={outcome.tone}>{outcome.label}</StatusBadge>
      </header>

      <div className={styles.body} aria-live="polite">
        {terminal ? (
          <div className={styles.outcome} data-decision={decision}>
            <span className={styles.outcomeIcon} aria-hidden="true">
              {decision === 'confirmed' ? (
                <ShieldCheck size={22} />
              ) : (
                <ShieldX size={22} />
              )}
            </span>
            <strong>{outcome.label}</strong>
            <p>No permission or runtime authority was created.</p>
            <dl>
              <div>
                <dt>Authorization</dt>
                <dd>None</dd>
              </div>
              <div>
                <dt>Persistence</dt>
                <dd>None</dd>
              </div>
              <div>
                <dt>Runtime dispatch</dt>
                <dd>None</dd>
              </div>
            </dl>
            <Button
              size="small"
              variant="secondary"
              leadingIcon={<RotateCcw size={15} />}
              onClick={onReset}
            >
              Reset preview
            </Button>
          </div>
        ) : (
          <>
            <label className={styles.acknowledgement}>
              <input
                type="checkbox"
                checked={acknowledged}
                onChange={(event) => onAcknowledgedChange(event.target.checked)}
              />
              <span>
                <strong>I reviewed this preview request</strong>
                <small>Confirmation remains local and non-authoritative.</small>
              </span>
            </label>

            <div className={styles.actions}>
              <Button
                variant="danger"
                leadingIcon={<ShieldX size={16} />}
                onClick={onDeny}
              >
                Deny preview
              </Button>
              <Button
                variant="primary"
                leadingIcon={<ShieldCheck size={16} />}
                disabled={!acknowledged}
                onClick={onConfirm}
              >
                Confirm preview
              </Button>
            </div>
          </>
        )}
      </div>

      <footer className={styles.footer}>
        Local UI state only · no session · no API · no execution
      </footer>
    </section>
  );
}
