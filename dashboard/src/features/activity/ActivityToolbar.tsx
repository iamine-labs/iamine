import { RotateCcw } from 'lucide-react';

import { IconButton, SegmentedControl, TextField } from '../../components';
import type {
  ActivityPreviewCategory,
  ActivityPreviewSignal,
} from '../../contracts/view-models/activity';
import styles from './ActivityToolbar.module.css';

export type ActivitySignalFilter = 'all' | ActivityPreviewSignal;
export type ActivityCategoryFilter = 'all' | ActivityPreviewCategory;

const signalOptions: readonly {
  value: ActivitySignalFilter;
  label: string;
}[] = [
  { value: 'all', label: 'All' },
  { value: 'informational', label: 'Informational' },
  { value: 'attention', label: 'Attention' },
  { value: 'boundary', label: 'Boundary' },
];

interface ActivityToolbarProps {
  category: ActivityCategoryFilter;
  onCategoryChange: (category: ActivityCategoryFilter) => void;
  onQueryChange: (query: string) => void;
  onReset: () => void;
  onSignalChange: (signal: ActivitySignalFilter) => void;
  query: string;
  resultCount: number;
  signal: ActivitySignalFilter;
  totalCount: number;
}

export function ActivityToolbar({
  category,
  onCategoryChange,
  onQueryChange,
  onReset,
  onSignalChange,
  query,
  resultCount,
  signal,
  totalCount,
}: ActivityToolbarProps) {
  const filtersActive =
    category !== 'all' || signal !== 'all' || query.length > 0;

  return (
    <section className={styles.toolbar} aria-label="Activity preview filters">
      <div className={styles.searchField}>
        <TextField
          label="Search activity"
          type="search"
          value={query}
          placeholder="Event, category, or label"
          onChange={(event) => onQueryChange(event.target.value)}
        />
      </div>
      <div className={styles.signalFilter}>
        <span>Preview signal</span>
        <SegmentedControl
          label="Filter activity by preview signal"
          options={signalOptions}
          value={signal}
          onChange={onSignalChange}
        />
      </div>
      <label className={styles.categoryFilter}>
        <span>Category label</span>
        <select
          aria-label="Filter activity by category label"
          value={category}
          onChange={(event) =>
            onCategoryChange(event.target.value as ActivityCategoryFilter)
          }
        >
          <option value="all">All labels</option>
          <option value="system">System</option>
          <option value="agent">Agent</option>
          <option value="model">Model</option>
          <option value="network">Network</option>
        </select>
      </label>
      <div className={styles.resultCount} aria-live="polite">
        <span>
          {resultCount} of {totalCount}
        </span>
        <IconButton
          size="small"
          label="Clear activity filters"
          icon={<RotateCcw size={15} />}
          disabled={!filtersActive}
          onClick={onReset}
        />
      </div>
    </section>
  );
}
