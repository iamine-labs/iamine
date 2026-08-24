import { Eye } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';

import { StatePanel, StatusBadge } from '../../components';
import type {
  ModelsDataSource,
  ModelsViewModel,
} from '../../contracts/view-models/models';
import { modelsMockDataSource } from '../../mocks/modelsMockDataSource';
import { ModelsDetail } from './ModelsDetail';
import { ModelsList } from './ModelsList';
import {
  ModelsToolbar,
  type ModelsCategoryFilter,
  type ModelsStateFilter,
} from './ModelsToolbar';
import styles from './ModelsPage.module.css';

type ModelsState =
  | { status: 'loading' }
  | { status: 'ready'; viewModel: ModelsViewModel }
  | { status: 'empty' }
  | { status: 'error' };

export function ModelsPage({
  dataSource = modelsMockDataSource,
}: {
  dataSource?: ModelsDataSource;
}) {
  const [attempt, setAttempt] = useState(0);
  const [previewState, setPreviewState] = useState<ModelsStateFilter>('all');
  const [category, setCategory] = useState<ModelsCategoryFilter>('all');
  const [query, setQuery] = useState('');
  const [selectedId, setSelectedId] = useState('');
  const [state, setState] = useState<ModelsState>({ status: 'loading' });

  useEffect(() => {
    let active = true;

    void dataSource
      .load()
      .then((viewModel) => {
        if (!active) return;
        setState(
          viewModel ? { status: 'ready', viewModel } : { status: 'empty' },
        );
        setSelectedId(viewModel?.models[0]?.id ?? '');
      })
      .catch(() => {
        if (active) setState({ status: 'error' });
      });

    return () => {
      active = false;
    };
  }, [attempt, dataSource]);

  const filteredModels = useMemo(() => {
    if (state.status !== 'ready') return [];
    const normalizedQuery = query.trim().toLocaleLowerCase();

    return state.viewModel.models.filter((model) => {
      const matchesState =
        previewState === 'all' || model.previewState === previewState;
      const matchesCategory = category === 'all' || model.category === category;
      const searchable = [
        model.name,
        model.categoryLabel,
        model.summary,
        model.displayClassLabel,
        ...model.useLabels.map((label) => label.label),
      ]
        .join(' ')
        .toLocaleLowerCase();
      return (
        matchesState && matchesCategory && searchable.includes(normalizedQuery)
      );
    });
  }, [category, previewState, query, state]);

  if (state.status === 'loading') {
    return (
      <div className={styles.state}>
        <StatePanel
          state="loading"
          title="Loading models preview"
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
          title="No models preview data"
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
          title="Models preview unavailable"
          detail="The local presentation source could not be loaded."
          onRetry={retry}
        />
      </div>
    );
  }

  const selectedModel =
    filteredModels.find((model) => model.id === selectedId) ??
    filteredModels[0];

  const resetFilters = () => {
    setPreviewState('all');
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

      <dl className={styles.metrics} aria-label="Models preview summary">
        {state.viewModel.metrics.map((metric) => (
          <div key={metric.label}>
            <dt>{metric.label}</dt>
            <dd>{metric.value}</dd>
          </div>
        ))}
      </dl>

      <ModelsToolbar
        category={category}
        query={query}
        resultCount={filteredModels.length}
        state={previewState}
        totalCount={state.viewModel.models.length}
        onCategoryChange={setCategory}
        onQueryChange={setQuery}
        onReset={resetFilters}
        onStateChange={setPreviewState}
      />

      {selectedModel ? (
        <div className={styles.content}>
          <ModelsList
            models={filteredModels}
            selectedId={selectedModel.id}
            onSelect={setSelectedId}
          />
          <ModelsDetail model={selectedModel} />
        </div>
      ) : (
        <div className={styles.noMatches}>
          <StatePanel
            state="empty"
            title="No matching models"
            detail="Adjust the search or preview filters."
          />
        </div>
      )}
    </div>
  );
}
