import { CircleOff, MonitorDot, ShieldCheck } from 'lucide-react';

import { StatusBadge } from '../components';
import styles from './DashboardShell.module.css';

export function DashboardStatusBar() {
  return (
    <footer
      className={styles.statusbar}
      aria-label="Dashboard connection status"
      tabIndex={0}
    >
      <span className={styles.version}>IAMINE dashboard</span>
      <span>
        <MonitorDot size={13} aria-hidden="true" />
        Data: <strong>non-authoritative preview</strong>
      </span>
      <span>
        <CircleOff size={13} aria-hidden="true" />
        Core connection: <strong>not configured</strong>
      </span>
      <span className={styles.statusEnd}>
        <ShieldCheck size={13} aria-hidden="true" />
        Live actions
        <StatusBadge tone="neutral">Unavailable</StatusBadge>
      </span>
    </footer>
  );
}
