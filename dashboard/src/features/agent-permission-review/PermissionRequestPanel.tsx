import { Bot, KeyRound, ShieldX } from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  AgentPermissionDisposition,
  AgentPermissionReviewViewModel,
  AgentPermissionRisk,
} from '../../contracts/view-models/agentPermissionReview';
import styles from './PermissionRequestPanel.module.css';

const riskTones: Record<AgentPermissionRisk, StatusTone> = {
  low: 'info',
  moderate: 'warning',
  elevated: 'danger',
};

const dispositionTones: Record<AgentPermissionDisposition, StatusTone> = {
  requested: 'warning',
  blocked: 'neutral',
};

export function PermissionRequestPanel({
  review,
}: {
  review: AgentPermissionReviewViewModel;
}) {
  return (
    <section className={styles.panel} aria-labelledby="permission-request">
      <header className={styles.header}>
        <span className={styles.agentIcon} aria-hidden="true">
          <Bot size={20} />
        </span>
        <div>
          <span>Agent request</span>
          <h3 id="permission-request">{review.agentName}</h3>
        </div>
        <StatusBadge tone={riskTones[review.risk]}>
          {review.riskLabel}
        </StatusBadge>
      </header>

      <div className={styles.requestSummary}>
        <span>{review.operationLabel}</span>
        <p>{review.summary}</p>
        <dl>
          <div>
            <dt>Permission profile</dt>
            <dd>{review.permissionProfileLabel}</dd>
          </div>
          <div>
            <dt>Authority</dt>
            <dd>Preview only</dd>
          </div>
        </dl>
      </div>

      <div className={styles.permissionList}>
        <h4>Declared permission surface</h4>
        <ul>
          {review.permissions.map((permission) => {
            const Icon =
              permission.disposition === 'requested' ? KeyRound : ShieldX;

            return (
              <li key={permission.id}>
                <span
                  className={styles.permissionIcon}
                  data-disposition={permission.disposition}
                  aria-hidden="true"
                >
                  <Icon size={16} />
                </span>
                <div>
                  <strong>{permission.categoryLabel}</strong>
                  <span>{permission.scopeLabel}</span>
                </div>
                <span className={styles.access}>{permission.accessLabel}</span>
                <StatusBadge tone={dispositionTones[permission.disposition]}>
                  {permission.dispositionLabel}
                </StatusBadge>
              </li>
            );
          })}
        </ul>
      </div>
    </section>
  );
}
