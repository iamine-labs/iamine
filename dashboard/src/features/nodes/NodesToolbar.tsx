import { RotateCcw } from 'lucide-react';

import { IconButton, SegmentedControl, TextField } from '../../components';
import type {
  NodePreviewCapability,
  NodePreviewStatus,
} from '../../contracts/view-models/nodes';
import styles from './NodesToolbar.module.css';

export type NodesStatusFilter = 'all' | NodePreviewStatus;
export type NodesCapabilityFilter = 'all' | NodePreviewCapability;

const statusOptions: readonly {
  value: NodesStatusFilter;
  label: string;
}[] = [
  { value: 'all', label: 'All' },
  { value: 'available', label: 'Available' },
  { value: 'limited', label: 'Limited' },
  { value: 'offline', label: 'Offline' },
];

interface NodesToolbarProps {
  capability: NodesCapabilityFilter;
  onCapabilityChange: (capability: NodesCapabilityFilter) => void;
  onQueryChange: (query: string) => void;
  onReset: () => void;
  onStatusChange: (status: NodesStatusFilter) => void;
  query: string;
  resultCount: number;
  status: NodesStatusFilter;
  totalCount: number;
}

export function NodesToolbar({
  capability,
  onCapabilityChange,
  onQueryChange,
  onReset,
  onStatusChange,
  query,
  resultCount,
  status,
  totalCount,
}: NodesToolbarProps) {
  const filtersActive =
    capability !== 'all' || status !== 'all' || query.length > 0;

  return (
    <section className={styles.toolbar} aria-label="Nodes preview filters">
      <div className={styles.searchField}>
        <TextField
          label="Search nodes"
          type="search"
          value={query}
          placeholder="Name, role, or environment"
          onChange={(event) => onQueryChange(event.target.value)}
        />
      </div>
      <div className={styles.statusFilter}>
        <span>Preview status</span>
        <SegmentedControl
          label="Filter nodes by preview status"
          options={statusOptions}
          value={status}
          onChange={onStatusChange}
        />
      </div>
      <label className={styles.capabilityFilter}>
        <span>Capability label</span>
        <select
          aria-label="Filter nodes by capability label"
          value={capability}
          onChange={(event) =>
            onCapabilityChange(event.target.value as NodesCapabilityFilter)
          }
        >
          <option value="all">All labels</option>
          <option value="compute">Compute</option>
          <option value="acceleration">Acceleration</option>
          <option value="storage">Storage</option>
        </select>
      </label>
      <div className={styles.resultCount} aria-live="polite">
        <span>
          {resultCount} of {totalCount}
        </span>
        <IconButton
          size="small"
          label="Clear nodes filters"
          icon={<RotateCcw size={15} />}
          disabled={!filtersActive}
          onClick={onReset}
        />
      </div>
    </section>
  );
}
