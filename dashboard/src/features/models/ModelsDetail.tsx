import {
  Box,
  CircleCheckBig,
  CircleDashed,
  ShieldCheck,
  TriangleAlert,
} from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  ModelPreviewItem,
  ModelPreviewState,
} from '../../contracts/view-models/models';
import styles from './ModelsDetail.module.css';

const stateTones: Record<ModelPreviewState, StatusTone> = {
  shown: 'success',
  attention: 'warning',
  unavailable: 'neutral',
};

const stateIcons = {
  shown: CircleCheckBig,
  attention: TriangleAlert,
  unavailable: CircleDashed,
} as const;

export function ModelsDetail({ model }: { model: ModelPreviewItem }) {
  const StateIcon = stateIcons[model.previewState];

  return (
    <aside className={styles.panel} aria-labelledby="selected-model-name">
      <header className={styles.header}>
        <span className={styles.icon} aria-hidden="true">
          <Box size={22} />
        </span>
        <div>
          <span>Selected preview</span>
          <h3 id="selected-model-name">{model.name}</h3>
        </div>
        <StatusBadge tone={stateTones[model.previewState]}>
          {model.previewStateLabel}
        </StatusBadge>
      </header>

      <p className={styles.summary}>{model.summary}</p>

      <dl className={styles.facts}>
        <div>
          <dt>Category label</dt>
          <dd>{model.categoryLabel}</dd>
        </div>
        <div>
          <dt>Display class</dt>
          <dd>{model.displayClassLabel}</dd>
        </div>
        <div>
          <dt>Artifact</dt>
          <dd>{model.artifactLabel}</dd>
        </div>
        <div>
          <dt>Source</dt>
          <dd>{model.sourceLabel}</dd>
        </div>
      </dl>

      <section className={styles.detailSection}>
        <h4>Use labels</h4>
        <ul className={styles.labels}>
          {model.useLabels.map((label) => (
            <li key={label.id}>{label.label}</li>
          ))}
        </ul>
      </section>

      <section className={styles.detailSection}>
        <h4>Fixture boundary</h4>
        <ul className={styles.notes}>
          {model.notes.map((note) => (
            <li key={note}>{note}</li>
          ))}
        </ul>
      </section>

      <footer className={styles.footer}>
        <ShieldCheck size={14} aria-hidden="true" />
        <span>
          <StateIcon size={13} aria-hidden="true" /> Synthetic metadata only. No
          model registry was read.
        </span>
      </footer>
    </aside>
  );
}
