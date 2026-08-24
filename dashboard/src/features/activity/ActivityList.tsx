import { Activity, ChevronRight } from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  ActivityPreviewItem,
  ActivityPreviewSignal,
} from '../../contracts/view-models/activity';
import styles from './ActivityList.module.css';

const signalTones: Record<ActivityPreviewSignal, StatusTone> = {
  informational: 'info',
  attention: 'warning',
  boundary: 'neutral',
};

interface ActivityListProps {
  items: readonly ActivityPreviewItem[];
  onSelect: (id: string) => void;
  selectedId: string;
}

export function ActivityList({
  items,
  onSelect,
  selectedId,
}: ActivityListProps) {
  return (
    <section className={styles.panel} aria-labelledby="activity-results">
      <header className={styles.header}>
        <div>
          <h3 id="activity-results">Synthetic activity stream</h3>
          <p>
            Presentation sequence only, disconnected from every event source.
          </p>
        </div>
        <span>{items.length} shown</span>
      </header>
      <ol className={styles.list}>
        {items.map((item) => {
          const selected = item.id === selectedId;

          return (
            <li key={item.id} data-selected={selected}>
              <button
                className={styles.itemButton}
                type="button"
                aria-label={`Select ${item.name}`}
                aria-pressed={selected}
                onClick={() => onSelect(item.id)}
              >
                <span className={styles.eventIcon} aria-hidden="true">
                  <Activity size={17} />
                </span>
                <span className={styles.itemContent}>
                  <span className={styles.itemHeading}>
                    <strong>{item.name}</strong>
                    <small>{item.sequenceLabel}</small>
                  </span>
                  <span className={styles.summary}>{item.summary}</span>
                  <span className={styles.category}>{item.categoryLabel}</span>
                </span>
                <StatusBadge tone={signalTones[item.signal]}>
                  {item.signalLabel}
                </StatusBadge>
                <ChevronRight size={16} aria-hidden="true" />
              </button>
            </li>
          );
        })}
      </ol>
    </section>
  );
}
