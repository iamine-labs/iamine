import {
  Activity,
  CircleDashed,
  Info,
  ShieldCheck,
  TriangleAlert,
} from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  ActivityPreviewItem,
  ActivityPreviewSignal,
} from '../../contracts/view-models/activity';
import styles from './ActivityDetail.module.css';

const signalTones: Record<ActivityPreviewSignal, StatusTone> = {
  informational: 'info',
  attention: 'warning',
  boundary: 'neutral',
};

const signalIcons = {
  informational: Info,
  attention: TriangleAlert,
  boundary: CircleDashed,
} as const;

export function ActivityDetail({ item }: { item: ActivityPreviewItem }) {
  const SignalIcon = signalIcons[item.signal];

  return (
    <aside className={styles.panel} aria-labelledby="selected-activity-name">
      <header className={styles.header}>
        <span className={styles.icon} aria-hidden="true">
          <Activity size={22} />
        </span>
        <div>
          <span>Selected preview</span>
          <h3 id="selected-activity-name">{item.name}</h3>
        </div>
        <StatusBadge tone={signalTones[item.signal]}>
          {item.signalLabel}
        </StatusBadge>
      </header>

      <p className={styles.summary}>{item.summary}</p>

      <dl className={styles.facts}>
        <div>
          <dt>Moment label</dt>
          <dd>{item.sequenceLabel}</dd>
        </div>
        <div>
          <dt>Category label</dt>
          <dd>{item.categoryLabel}</dd>
        </div>
        <div>
          <dt>Context</dt>
          <dd>{item.contextLabel}</dd>
        </div>
        <div>
          <dt>Source</dt>
          <dd>{item.sourceLabel}</dd>
        </div>
      </dl>

      <section className={styles.detailSection}>
        <h4>Presentation labels</h4>
        <ul className={styles.labels}>
          {item.detailLabels.map((label) => (
            <li key={label.id}>{label.label}</li>
          ))}
        </ul>
      </section>

      <section className={styles.detailSection}>
        <h4>Fixture boundary</h4>
        <ul className={styles.notes}>
          {item.notes.map((note) => (
            <li key={note}>{note}</li>
          ))}
        </ul>
      </section>

      <footer className={styles.footer}>
        <ShieldCheck size={14} aria-hidden="true" />
        <span>
          <SignalIcon size={13} aria-hidden="true" /> Synthetic metadata only.
          No event source was read.
        </span>
      </footer>
    </aside>
  );
}
