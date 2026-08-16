import {
  Bell,
  Check,
  Download,
  MoreHorizontal,
  RefreshCw,
  Search,
  ShieldCheck,
} from 'lucide-react';
import { useMemo, useState } from 'react';

import {
  BrandMark,
  Button,
  DataTable,
  type DataColumn,
  IconButton,
  ProgressBar,
  SegmentedControl,
  type SegmentOption,
  StatePanel,
  StatusBadge,
  TextField,
  Toggle,
} from '../components';
import { previewChecks, type PreviewCheck } from './fixtures';
import styles from './DesignSystemPreview.module.css';

type Density = 'comfortable' | 'compact';

const densityOptions: readonly SegmentOption<Density>[] = [
  { value: 'comfortable', label: 'Comfortable' },
  { value: 'compact', label: 'Compact' },
];

const columns: readonly DataColumn<PreviewCheck>[] = [
  { key: 'check', header: 'Check', render: (row) => row.check },
  { key: 'source', header: 'Source', render: (row) => row.source },
  {
    key: 'status',
    header: 'Status',
    render: (row) => <StatusBadge tone={row.tone}>{row.status}</StatusBadge>,
  },
  {
    key: 'duration',
    header: 'Duration',
    align: 'end',
    render: (row) => <span className={styles.mono}>{row.duration}</span>,
  },
];

export function DesignSystemPreview() {
  const [density, setDensity] = useState<Density>('comfortable');
  const [notifications, setNotifications] = useState(true);
  const [query, setQuery] = useState('');

  const filteredChecks = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) return previewChecks;
    return previewChecks.filter((item) =>
      item.check.toLowerCase().includes(normalized),
    );
  }, [query]);

  return (
    <div className={styles.shell} data-density={density}>
      <header className={styles.topbar}>
        <div className={styles.topbarInner}>
          <BrandMark />
          <div className={styles.topbarActions}>
            <StatusBadge tone="info">Preview data</StatusBadge>
            <IconButton
              label="Preview notifications"
              icon={<Bell size={17} />}
            />
            <IconButton
              label="More preview options"
              icon={<MoreHorizontal size={18} />}
            />
          </div>
        </div>
      </header>

      <main className={styles.main}>
        <section className={styles.intro} aria-labelledby="preview-title">
          <div className={styles.introCopy}>
            <p className={styles.eyebrow}>Local operator interface</p>
            <h1 id="preview-title">Design system review</h1>
            <p className={styles.subtitle}>
              Non-authoritative fixtures for visual and interaction validation.
            </p>
          </div>
          <div className={styles.actionRow}>
            <Button variant="secondary" leadingIcon={<Download size={16} />}>
              Export sample
            </Button>
            <Button variant="primary" leadingIcon={<Check size={16} />}>
              Confirm preview
            </Button>
          </div>
        </section>

        <section
          className={styles.metricGrid}
          aria-label="Preview status summary"
        >
          <article className={styles.metric}>
            <div className={styles.metricHeader}>
              <span>Local node</span>
              <StatusBadge tone="success">Ready</StatusBadge>
            </div>
            <strong>Available</strong>
            <span>Fixture evidence</span>
          </article>
          <article className={styles.metric}>
            <div className={styles.metricHeader}>
              <span>Network</span>
              <StatusBadge tone="warning">Attention</StatusBadge>
            </div>
            <strong>3 peers</strong>
            <span>Sample topology</span>
          </article>
          <article className={styles.metric}>
            <div className={styles.metricHeader}>
              <span>Agents</span>
              <StatusBadge tone="neutral">Unavailable</StatusBadge>
            </div>
            <strong>Not connected</strong>
            <span>No execution adapter</span>
          </article>
        </section>

        <section className={styles.section} aria-labelledby="controls-title">
          <div className={styles.sectionHeading}>
            <div>
              <p className={styles.eyebrow}>Interaction</p>
              <h2 id="controls-title">Controls</h2>
            </div>
            <SegmentedControl
              label="Preview density"
              options={densityOptions}
              value={density}
              onChange={setDensity}
            />
          </div>

          <div className={styles.controlGrid}>
            <div className={styles.controlBlock}>
              <h3>Commands</h3>
              <div className={styles.buttonGroup}>
                <Button variant="primary" leadingIcon={<RefreshCw size={16} />}>
                  Refresh sample
                </Button>
                <Button variant="secondary">Secondary</Button>
                <Button variant="danger">Remove sample</Button>
                <Button variant="quiet">Dismiss</Button>
              </div>
            </div>

            <div className={styles.controlBlock}>
              <h3>Input</h3>
              <TextField
                label="Filter checks"
                description={`${filteredChecks.length} sample rows visible`}
                placeholder="Type a check name"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
              />
            </div>

            <div className={styles.controlBlock}>
              <h3>Preference</h3>
              <Toggle
                checked={notifications}
                label="Preview notifications"
                description="Changes local component state only."
                onChange={setNotifications}
              />
            </div>
          </div>
        </section>

        <section className={styles.section} aria-labelledby="status-title">
          <div className={styles.sectionHeading}>
            <div>
              <p className={styles.eyebrow}>System feedback</p>
              <h2 id="status-title">Status and capacity</h2>
            </div>
            <IconButton
              label="Search preview records"
              icon={<Search size={17} />}
            />
          </div>
          <div className={styles.capacityGrid}>
            <ProgressBar
              label="Memory allocation"
              value={68}
              valueLabel="5.4 / 8 GB"
            />
            <ProgressBar
              label="Storage"
              value={43}
              valueLabel="43%"
              tone="info"
            />
            <ProgressBar
              label="Queue pressure"
              value={81}
              valueLabel="High"
              tone="warning"
            />
          </div>
        </section>

        <section className={styles.section} aria-labelledby="records-title">
          <div className={styles.sectionHeading}>
            <div>
              <p className={styles.eyebrow}>Typed fixtures</p>
              <h2 id="records-title">Recent checks</h2>
            </div>
            <StatusBadge tone="neutral">
              {filteredChecks.length} records
            </StatusBadge>
          </div>
          <DataTable
            caption="Preview check results"
            columns={columns}
            getRowKey={(row) => row.id}
            rows={filteredChecks}
          />
        </section>

        <section className={styles.section} aria-labelledby="states-title">
          <div className={styles.sectionHeading}>
            <div>
              <p className={styles.eyebrow}>Bounded outcomes</p>
              <h2 id="states-title">Application states</h2>
            </div>
            <ShieldCheck
              className={styles.headingIcon}
              size={20}
              aria-hidden="true"
            />
          </div>
          <div className={styles.stateGrid}>
            <StatePanel
              state="loading"
              title="Loading evidence"
              detail="Waiting for a bounded preview response."
            />
            <StatePanel
              state="empty"
              title="No records"
              detail="The fixture returned a valid empty collection."
            />
            <StatePanel
              state="error"
              title="Preview unavailable"
              detail="No real request was attempted."
              onRetry={() => undefined}
            />
          </div>
        </section>
      </main>
    </div>
  );
}
