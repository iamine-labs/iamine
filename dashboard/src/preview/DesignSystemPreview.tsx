import { ArrowLeft, Construction, RotateCw } from 'lucide-react';
import { useState } from 'react';

import { Button, StatusBadge } from '../components';
import { DashboardChrome } from './DashboardChrome';
import { type DashboardView, navigationItems } from './fixtures';
import { OverviewSummary } from './OverviewSummary';
import { OverviewTelemetry } from './OverviewTelemetry';
import styles from './DesignSystemPreview.module.css';

export function DesignSystemPreview() {
  const [activeView, setActiveView] = useState<DashboardView>('overview');
  const [drawerOpen, setDrawerOpen] = useState(false);

  const navigate = (view: DashboardView) => {
    setActiveView(view);
    setDrawerOpen(false);
  };

  const activeLabel =
    navigationItems.find((item) => item.id === activeView)?.label ?? 'Overview';

  return (
    <div className={styles.shell}>
      <DashboardChrome
        activeView={activeView}
        drawerOpen={drawerOpen}
        onDrawerToggle={() => setDrawerOpen((open) => !open)}
        onNavigate={navigate}
      />

      <main className={styles.main}>
        <h1 className="sr-only">IAMINE {activeLabel} dashboard preview</h1>
        {activeView === 'overview' ? (
          <div className={styles.overviewGrid}>
            <OverviewSummary onNavigate={navigate} />
            <OverviewTelemetry />
          </div>
        ) : (
          <section className={styles.placeholder}>
            <span className={styles.placeholderIcon} aria-hidden="true">
              <Construction size={34} />
            </span>
            <StatusBadge tone="info">Preview boundary</StatusBadge>
            <h2>{activeLabel}</h2>
            <p>
              This official navigation destination is reserved for its own
              feature. No node request or fictitious endpoint was created.
            </p>
            <Button
              leadingIcon={<ArrowLeft size={16} />}
              onClick={() => navigate('overview')}
            >
              Return to Overview
            </Button>
          </section>
        )}
      </main>

      <footer
        className={styles.statusbar}
        aria-label="Preview node status"
        tabIndex={0}
      >
        <span className={styles.version}>IAMINE Core v1.3.2</span>
        <span>
          Node: <strong>NODE-LOCAL-01</strong>
          <StatusBadge tone="success">Online</StatusBadge>
        </span>
        <span>Uptime: 6d 18h 24m</span>
        <span className={styles.statusMetric}>
          System load: 42%
          <i aria-hidden="true">
            <b style={{ width: '42%' }} />
          </i>
        </span>
        <span>RAM: 13.4 / 32 GB</span>
        <span>VRAM: 11.2 / 16 GB</span>
        <span className={styles.statusEnd}>
          <RotateCw size={13} aria-hidden="true" />
          CPU: 42 C
        </span>
      </footer>
    </div>
  );
}
