import { RotateCcw } from 'lucide-react';

import { IconButton, SegmentedControl, TextField } from '../../components';
import type { AgentCatalogStage } from '../../contracts/view-models/agentCatalog';
import styles from './AgentCatalogToolbar.module.css';

export type AgentCatalogFilter = 'all' | AgentCatalogStage;

const stageOptions: readonly {
  value: AgentCatalogFilter;
  label: string;
}[] = [
  { value: 'all', label: 'All' },
  { value: 'reference', label: 'Reference' },
  { value: 'next', label: 'Next' },
  { value: 'planned', label: 'Planned' },
];

interface AgentCatalogToolbarProps {
  filter: AgentCatalogFilter;
  onFilterChange: (filter: AgentCatalogFilter) => void;
  onQueryChange: (query: string) => void;
  onReset: () => void;
  query: string;
  resultCount: number;
  totalCount: number;
}

export function AgentCatalogToolbar({
  filter,
  onFilterChange,
  onQueryChange,
  onReset,
  query,
  resultCount,
  totalCount,
}: AgentCatalogToolbarProps) {
  const filtersActive = filter !== 'all' || query.length > 0;

  return (
    <section className={styles.toolbar} aria-label="Agent catalog filters">
      <div className={styles.searchField}>
        <TextField
          label="Search agents"
          type="search"
          value={query}
          placeholder="Name, role, or platform"
          onChange={(event) => onQueryChange(event.target.value)}
        />
      </div>
      <div className={styles.stageFilter}>
        <span>Catalog stage</span>
        <SegmentedControl
          label="Filter agents by catalog stage"
          options={stageOptions}
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
          label="Clear agent catalog filters"
          icon={<RotateCcw size={15} />}
          disabled={!filtersActive}
          onClick={onReset}
        />
      </div>
    </section>
  );
}
