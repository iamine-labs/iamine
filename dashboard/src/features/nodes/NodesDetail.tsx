import {
  Boxes,
  CircleCheckBig,
  CircleDashed,
  ShieldCheck,
  TriangleAlert,
} from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  NodePreviewNode,
  NodePreviewStatus,
} from '../../contracts/view-models/nodes';
import styles from './NodesDetail.module.css';

const statusTones: Record<NodePreviewStatus, StatusTone> = {
  available: 'success',
  limited: 'warning',
  offline: 'neutral',
};

const statusIcons = {
  available: CircleCheckBig,
  limited: TriangleAlert,
  offline: CircleDashed,
} as const;

export function NodesDetail({ node }: { node: NodePreviewNode }) {
  const StatusIcon = statusIcons[node.status];

  return (
    <aside className={styles.panel} aria-labelledby="selected-node-name">
      <header className={styles.header}>
        <span className={styles.icon} aria-hidden="true">
          <Boxes size={22} />
        </span>
        <div>
          <span>Selected preview</span>
          <h3 id="selected-node-name">{node.name}</h3>
        </div>
        <StatusBadge tone={statusTones[node.status]}>
          {node.statusLabel}
        </StatusBadge>
      </header>

      <p className={styles.summary}>{node.summary}</p>

      <dl className={styles.facts}>
        <div>
          <dt>Role label</dt>
          <dd>{node.roleLabel}</dd>
        </div>
        <div>
          <dt>Environment</dt>
          <dd>{node.environmentLabel}</dd>
        </div>
        <div>
          <dt>Capacity</dt>
          <dd>{node.capacityLabel}</dd>
        </div>
        <div>
          <dt>Visibility</dt>
          <dd>{node.visibilityLabel}</dd>
        </div>
      </dl>

      <section className={styles.detailSection}>
        <h4>Capability labels</h4>
        <ul className={styles.capabilities}>
          {node.capabilities.map((capability) => (
            <li key={capability.id}>{capability.label}</li>
          ))}
        </ul>
      </section>

      <section className={styles.detailSection}>
        <h4>Fixture boundary</h4>
        <ul className={styles.notes}>
          {node.notes.map((note) => (
            <li key={note}>{note}</li>
          ))}
        </ul>
      </section>

      <footer className={styles.footer}>
        <ShieldCheck size={14} aria-hidden="true" />
        <span>
          <StatusIcon size={13} aria-hidden="true" /> Synthetic metadata only.
          No node was discovered.
        </span>
      </footer>
    </aside>
  );
}
