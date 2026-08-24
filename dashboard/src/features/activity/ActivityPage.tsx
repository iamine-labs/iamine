import { Eye } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';

import { StatePanel, StatusBadge } from '../../components';
import type {
  ActivityDataSource,
  ActivityViewModel,
} from '../../contracts/view-models/activity';
import { activityMockDataSource } from '../../mocks/activityMockDataSource';
import { ActivityDetail } from './ActivityDetail';
import { ActivityList } from './ActivityList';
import {
  ActivityToolbar,
  type ActivityCategoryFilter,
  type ActivitySignalFilter,
} from './ActivityToolbar';
import styles from './ActivityPage.module.css';

type ActivityState =
  | { status: 'loading' }
  | { status: 'ready'; viewModel: ActivityViewModel }
  | { status: 'empty' }
  | { status: 'error' };

export function ActivityPage({
  dataSource = activityMockDataSource,
}: {
  dataSource?: ActivityDataSource;
}) {
  const [attempt, setAttempt] = useState(0);
  const [signal, setSignal] = useState<ActivitySignalFilter>('all');
  const [category, setCategory] = useState<ActivityCategoryFilter>('all');
  const [query, setQuery] = useState('');
  const [selectedId, setSelectedId] = useState('');
  const [state, setState] = useState<ActivityState>({ status: 'loading' });

  useEffect(() => {
    let active = true;

    void dataSource
      .load()
      .then((viewModel) => {
        if (!active) return;
        setState(
          viewModel ? { status: 'ready', viewModel } : { status: 'empty' },
        );
        setSelectedId(viewModel?.items[0]?.id ?? '');
      })
      .catch(() => {
        if (active) setState({ status: 'error' });
      });

    return () => {
      active = false;
    };
  }, [attempt, dataSource]);

  const filteredItems = useMemo(() => {
    if (state.status !== 'ready') return [];
    const normalizedQuery = query.trim().toLocaleLowerCase();

    return state.viewModel.items.filter((item) => {
      const matchesSignal = signal === 'all' || item.signal === signal;
      const matchesCategory = category === 'all' || item.category === category;
      const searchable = [
        item.name,
        item.sequenceLabel,
        item.categoryLabel,
        item.summary,
        item.contextLabel,
        ...item.detailLabels.map((label) => label.label),
      ]
        .join(' ')
        .toLocaleLowerCase();
      return (
        matchesSignal && matchesCategory && searchable.includes(normalizedQuery)
      );
    });
  }, [category, query, signal, state]);

  if (state.status === 'loading') {
    return (
      <div className={styles.state}>
        <StatePanel
          state="loading"
          title="Loading activity preview"
          detail="Preparing deterministic local presentation data."
        />
      </div>
    );
  }

  if (state.status === 'empty') {
    return (
      <div className={styles.state}>
        <StatePanel
          state="empty"
          title="No activity preview data"
          detail="The local mock source returned no presentation data."
        />
      </div>
    );
  }

  if (state.status === 'error') {
    const retry = () => {
      setState({ status: 'loading' });
      setAttempt((value) => value + 1);
    };

    return (
      <div className={styles.state}>
        <StatePanel
          state="error"
          title="Activity preview unavailable"
          detail="The local presentation source could not be loaded."
          onRetry={retry}
        />
      </div>
    );
  }

  const selectedItem =
    filteredItems.find((item) => item.id === selectedId) ?? filteredItems[0];

  const resetFilters = () => {
    setSignal('all');
    setCategory('all');
    setQuery('');
  };

  return (
    <div className={styles.page} data-source={dataSource.kind}>
      <header className={styles.pageHeader}>
        <div>
          <StatusBadge tone="info">Preview data</StatusBadge>
          <h2>{state.viewModel.title}</h2>
          <p>{state.viewModel.subtitle}</p>
        </div>
        <span className={styles.provenance}>
          <Eye size={15} aria-hidden="true" />
          {state.viewModel.provenance.label}
        </span>
      </header>

      <dl className={styles.metrics} aria-label="Activity preview summary">
        {state.viewModel.metrics.map((metric) => (
          <div key={metric.label}>
            <dt>{metric.label}</dt>
            <dd>{metric.value}</dd>
          </div>
        ))}
      </dl>

      <ActivityToolbar
        category={category}
        query={query}
        resultCount={filteredItems.length}
        signal={signal}
        totalCount={state.viewModel.items.length}
        onCategoryChange={setCategory}
        onQueryChange={setQuery}
        onReset={resetFilters}
        onSignalChange={setSignal}
      />

      {selectedItem ? (
        <div className={styles.content}>
          <ActivityList
            items={filteredItems}
            selectedId={selectedItem.id}
            onSelect={setSelectedId}
          />
          <ActivityDetail item={selectedItem} />
        </div>
      ) : (
        <div className={styles.noMatches}>
          <StatePanel
            state="empty"
            title="No matching activity"
            detail="Adjust the search or preview filters."
          />
        </div>
      )}
    </div>
  );
}
