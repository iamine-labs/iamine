import { Eye } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';

import { StatePanel, StatusBadge } from '../../components';
import type {
  DiagnosticsDataSource,
  DiagnosticsViewModel,
} from '../../contracts/view-models/diagnostics';
import { diagnosticsMockDataSource } from '../../mocks/diagnosticsMockDataSource';
import { DiagnosticsDetail } from './DiagnosticsDetail';
import { DiagnosticsList } from './DiagnosticsList';
import {
  DiagnosticsToolbar,
  type DiagnosticsFilter,
} from './DiagnosticsToolbar';
import styles from './DiagnosticsPage.module.css';

type DiagnosticsState =
  | { status: 'loading' }
  | { status: 'ready'; viewModel: DiagnosticsViewModel }
  | { status: 'empty' }
  | { status: 'error' };

export function DiagnosticsPage({
  dataSource = diagnosticsMockDataSource,
}: {
  dataSource?: DiagnosticsDataSource;
}) {
  const [attempt, setAttempt] = useState(0);
  const [filter, setFilter] = useState<DiagnosticsFilter>('all');
  const [query, setQuery] = useState('');
  const [selectedId, setSelectedId] = useState('');
  const [state, setState] = useState<DiagnosticsState>({ status: 'loading' });

  useEffect(() => {
    let active = true;

    void dataSource
      .load()
      .then((viewModel) => {
        if (!active) return;
        setState(
          viewModel ? { status: 'ready', viewModel } : { status: 'empty' },
        );
        setSelectedId(viewModel?.checks[0]?.id ?? '');
      })
      .catch(() => {
        if (active) setState({ status: 'error' });
      });

    return () => {
      active = false;
    };
  }, [attempt, dataSource]);

  const filteredChecks = useMemo(() => {
    if (state.status !== 'ready') return [];
    const normalizedQuery = query.trim().toLocaleLowerCase();

    return state.viewModel.checks.filter((check) => {
      const matchesStatus = filter === 'all' || check.status === filter;
      const searchable = [
        check.title,
        check.categoryLabel,
        check.summary,
        check.safeCode,
        check.scopeLabel,
      ]
        .join(' ')
        .toLocaleLowerCase();
      return matchesStatus && searchable.includes(normalizedQuery);
    });
  }, [filter, query, state]);

  if (state.status === 'loading') {
    return (
      <div className={styles.state}>
        <StatePanel
          state="loading"
          title="Loading diagnostics preview"
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
          title="No diagnostics preview data"
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
          title="Diagnostics preview unavailable"
          detail="The local presentation source could not be loaded."
          onRetry={retry}
        />
      </div>
    );
  }

  const selectedCheck =
    filteredChecks.find((check) => check.id === selectedId) ??
    filteredChecks[0];

  const resetFilters = () => {
    setFilter('all');
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

      <dl className={styles.metrics} aria-label="Diagnostics preview summary">
        {state.viewModel.metrics.map((metric) => (
          <div key={metric.label}>
            <dt>{metric.label}</dt>
            <dd>{metric.value}</dd>
          </div>
        ))}
      </dl>

      <DiagnosticsToolbar
        filter={filter}
        query={query}
        resultCount={filteredChecks.length}
        totalCount={state.viewModel.checks.length}
        onFilterChange={setFilter}
        onQueryChange={setQuery}
        onReset={resetFilters}
      />

      {selectedCheck ? (
        <div className={styles.content}>
          <DiagnosticsList
            checks={filteredChecks}
            selectedId={selectedCheck.id}
            onSelect={setSelectedId}
          />
          <DiagnosticsDetail check={selectedCheck} />
        </div>
      ) : (
        <div className={styles.noMatches}>
          <StatePanel
            state="empty"
            title="No matching checks"
            detail="Adjust the search or preview-status filter."
          />
        </div>
      )}
    </div>
  );
}
