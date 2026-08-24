import { RotateCcw } from 'lucide-react';

import { IconButton, SegmentedControl, TextField } from '../../components';
import type {
  ModelPreviewCategory,
  ModelPreviewState,
} from '../../contracts/view-models/models';
import styles from './ModelsToolbar.module.css';

export type ModelsStateFilter = 'all' | ModelPreviewState;
export type ModelsCategoryFilter = 'all' | ModelPreviewCategory;

const stateOptions: readonly {
  value: ModelsStateFilter;
  label: string;
}[] = [
  { value: 'all', label: 'All' },
  { value: 'shown', label: 'Shown' },
  { value: 'attention', label: 'Review' },
  { value: 'unavailable', label: 'Unavailable' },
];

interface ModelsToolbarProps {
  category: ModelsCategoryFilter;
  onCategoryChange: (category: ModelsCategoryFilter) => void;
  onQueryChange: (query: string) => void;
  onReset: () => void;
  onStateChange: (state: ModelsStateFilter) => void;
  query: string;
  resultCount: number;
  state: ModelsStateFilter;
  totalCount: number;
}

export function ModelsToolbar({
  category,
  onCategoryChange,
  onQueryChange,
  onReset,
  onStateChange,
  query,
  resultCount,
  state,
  totalCount,
}: ModelsToolbarProps) {
  const filtersActive =
    category !== 'all' || state !== 'all' || query.length > 0;

  return (
    <section className={styles.toolbar} aria-label="Models preview filters">
      <div className={styles.searchField}>
        <TextField
          label="Search models"
          type="search"
          value={query}
          placeholder="Name, category, or label"
          onChange={(event) => onQueryChange(event.target.value)}
        />
      </div>
      <div className={styles.stateFilter}>
        <span>Preview state</span>
        <SegmentedControl
          label="Filter models by preview state"
          options={stateOptions}
          value={state}
          onChange={onStateChange}
        />
      </div>
      <label className={styles.categoryFilter}>
        <span>Category label</span>
        <select
          aria-label="Filter models by category label"
          value={category}
          onChange={(event) =>
            onCategoryChange(event.target.value as ModelsCategoryFilter)
          }
        >
          <option value="all">All labels</option>
          <option value="general">General</option>
          <option value="reasoning">Reasoning</option>
          <option value="code">Code</option>
          <option value="multimodal">Multimodal</option>
        </select>
      </label>
      <div className={styles.resultCount} aria-live="polite">
        <span>
          {resultCount} of {totalCount}
        </span>
        <IconButton
          size="small"
          label="Clear models filters"
          icon={<RotateCcw size={15} />}
          disabled={!filtersActive}
          onClick={onReset}
        />
      </div>
    </section>
  );
}
