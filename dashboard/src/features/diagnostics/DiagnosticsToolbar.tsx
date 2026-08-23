import { RotateCcw } from 'lucide-react';

import { IconButton, SegmentedControl, TextField } from '../../components';
import type { DiagnosticsStatus } from '../../contracts/view-models/diagnostics';
import styles from './DiagnosticsToolbar.module.css';

export type DiagnosticsFilter = 'all' | DiagnosticsStatus;

const statusOptions: readonly {
  value: DiagnosticsFilter;
  label: string;
}[] = [
  { value: 'all', label: 'All' },
  { value: 'healthy', label: 'Healthy' },
  { value: 'attention', label: 'Attention' },
  { value: 'unavailable', label: 'Unavailable' },
];

interface DiagnosticsToolbarProps {
  filter: DiagnosticsFilter;
  onFilterChange: (filter: DiagnosticsFilter) => void;
  onQueryChange: (query: string) => void;
  onReset: () => void;
  query: string;
  resultCount: number;
  totalCount: number;
}

export function DiagnosticsToolbar({
  filter,
  onFilterChange,
  onQueryChange,
  onReset,
  query,
  resultCount,
  totalCount,
}: DiagnosticsToolbarProps) {
  const filtersActive = filter !== 'all' || query.length > 0;

  return (
    <section className={styles.toolbar} aria-label="Diagnostics filters">
      <div className={styles.searchField}>
        <TextField
          label="Search checks"
          type="search"
          value={query}
          placeholder="Check, category, or code"
          onChange={(event) => onQueryChange(event.target.value)}
        />
      </div>
      <div className={styles.statusFilter}>
        <span>Preview status</span>
        <SegmentedControl
          label="Filter checks by preview status"
          options={statusOptions}
          value={filter}
          onChange={onFilterChange}
        />
      </div>
      <div className={styles.resultCount} aria-live="polite">
        <span>
          {resultCount} of {totalCount}
        </span>
        <IconButton
          size="small"
          label="Clear diagnostics filters"
          icon={<RotateCcw size={15} />}
          disabled={!filtersActive}
          onClick={onReset}
        />
      </div>
    </section>
  );
}
