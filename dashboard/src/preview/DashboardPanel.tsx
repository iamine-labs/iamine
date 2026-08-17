import type { ReactNode } from 'react';

import styles from './DashboardPanel.module.css';

interface DashboardPanelProps {
  action?: ReactNode;
  children: ReactNode;
  className?: string;
  title: string;
}

export function DashboardPanel({
  action,
  children,
  className = '',
  title,
}: DashboardPanelProps) {
  return (
    <section className={`${styles.panel} ${className}`}>
      <div className={styles.header}>
        <h2>{title}</h2>
        {action}
      </div>
      {children}
    </section>
  );
}
