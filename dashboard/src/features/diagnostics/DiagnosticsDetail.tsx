import {
  CircleCheckBig,
  CircleDashed,
  ShieldCheck,
  TriangleAlert,
} from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  DiagnosticsCheck,
  DiagnosticsStatus,
} from '../../contracts/view-models/diagnostics';
import styles from './DiagnosticsDetail.module.css';

const statusTones: Record<DiagnosticsStatus, StatusTone> = {
  healthy: 'success',
  attention: 'warning',
  unavailable: 'neutral',
};

const statusIcons = {
  healthy: CircleCheckBig,
  attention: TriangleAlert,
  unavailable: CircleDashed,
} as const;

export function DiagnosticsDetail({ check }: { check: DiagnosticsCheck }) {
  const StatusIcon = statusIcons[check.status];

  return (
    <aside className={styles.panel} aria-labelledby="selected-check-name">
      <header className={styles.header}>
        <span className={styles.icon} aria-hidden="true">
          <StatusIcon size={22} />
        </span>
        <div>
          <span>Selected preview</span>
          <h3 id="selected-check-name">{check.title}</h3>
        </div>
        <StatusBadge tone={statusTones[check.status]}>
          {check.statusLabel}
        </StatusBadge>
      </header>

      <p className={styles.summary}>{check.summary}</p>

      <dl className={styles.facts}>
        <div>
          <dt>Category</dt>
          <dd>{check.categoryLabel}</dd>
        </div>
        <div>
          <dt>Scope</dt>
          <dd>{check.scopeLabel}</dd>
        </div>
        <div>
          <dt>Safe code</dt>
          <dd>
            <code>{check.safeCode}</code>
          </dd>
        </div>
      </dl>

      <section className={styles.detailSection}>
        <h4>Fixture observation</h4>
        <p>{check.observation}</p>
      </section>

      <section className={styles.detailSection}>
        <h4>Bounded next step</h4>
        <p>{check.nextStep}</p>
      </section>

      <footer className={styles.footer}>
        <ShieldCheck size={14} aria-hidden="true" />
        Synthetic metadata only. No machine evidence was read.
      </footer>
    </aside>
  );
}
