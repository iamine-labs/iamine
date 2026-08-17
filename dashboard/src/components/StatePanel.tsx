import { AlertTriangle, Inbox, LoaderCircle } from 'lucide-react';

import { Button } from './Button';
import styles from './StatePanel.module.css';

type PanelState = 'loading' | 'empty' | 'error';

interface StatePanelProps {
  state: PanelState;
  title: string;
  detail: string;
  onRetry?: () => void;
}

const icons = {
  loading: <LoaderCircle size={18} />,
  empty: <Inbox size={18} />,
  error: <AlertTriangle size={18} />,
};

export function StatePanel({ detail, onRetry, state, title }: StatePanelProps) {
  return (
    <section
      className={`${styles.panel} ${styles[state]}`}
      aria-live={state === 'error' ? 'assertive' : 'polite'}
      aria-busy={state === 'loading' || undefined}
    >
      <span className={styles.icon} aria-hidden="true">
        {icons[state]}
      </span>
      <div className={styles.copy}>
        <h3>{title}</h3>
        <p>{detail}</p>
      </div>
      {state === 'error' && onRetry && (
        <Button size="small" variant="secondary" onClick={onRetry}>
          Retry preview
        </Button>
      )}
    </section>
  );
}
