import { Eye } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';

import { StatePanel, StatusBadge } from '../../components';
import type {
  AgentCatalogDataSource,
  AgentCatalogViewModel,
} from '../../contracts/view-models/agentCatalog';
import { agentCatalogMockDataSource } from '../../mocks/agentCatalogMockDataSource';
import { AgentCatalogDetail } from './AgentCatalogDetail';
import { AgentCatalogList } from './AgentCatalogList';
import {
  AgentCatalogToolbar,
  type AgentCatalogFilter,
} from './AgentCatalogToolbar';
import styles from './AgentCatalogPage.module.css';

type AgentCatalogState =
  | { status: 'loading' }
  | { status: 'ready'; viewModel: AgentCatalogViewModel }
  | { status: 'empty' }
  | { status: 'error' };

export function AgentCatalogPage({
  dataSource = agentCatalogMockDataSource,
}: {
  dataSource?: AgentCatalogDataSource;
}) {
  const [attempt, setAttempt] = useState(0);
  const [filter, setFilter] = useState<AgentCatalogFilter>('all');
  const [query, setQuery] = useState('');
  const [selectedId, setSelectedId] = useState('');
  const [state, setState] = useState<AgentCatalogState>({ status: 'loading' });

  useEffect(() => {
    let active = true;

    void dataSource
      .load()
      .then((viewModel) => {
        if (!active) return;
        setState(
          viewModel ? { status: 'ready', viewModel } : { status: 'empty' },
        );
        setSelectedId(viewModel?.agents[0]?.id ?? '');
      })
      .catch(() => {
        if (active) setState({ status: 'error' });
      });

    return () => {
      active = false;
    };
  }, [attempt, dataSource]);

  const filteredAgents = useMemo(() => {
    if (state.status !== 'ready') return [];
    const normalizedQuery = query.trim().toLocaleLowerCase();

    return state.viewModel.agents.filter((agent) => {
      const matchesStage = filter === 'all' || agent.stage === filter;
      const searchable = [
        agent.name,
        agent.description,
        agent.roleLabel,
        agent.operatingMode,
        agent.platformLabel,
      ]
        .join(' ')
        .toLocaleLowerCase();
      return matchesStage && searchable.includes(normalizedQuery);
    });
  }, [filter, query, state]);

  if (state.status === 'loading') {
    return (
      <div className={styles.state}>
        <StatePanel
          state="loading"
          title="Loading agent catalog preview"
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
          title="No agent catalog preview data"
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
          title="Agent catalog preview unavailable"
          detail="The local presentation source could not be loaded."
          onRetry={retry}
        />
      </div>
    );
  }

  const selectedAgent =
    filteredAgents.find((agent) => agent.id === selectedId) ??
    filteredAgents[0];

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

      <dl className={styles.metrics} aria-label="Agent catalog preview summary">
        {state.viewModel.metrics.map((metric) => (
          <div key={metric.label}>
            <dt>{metric.label}</dt>
            <dd>{metric.value}</dd>
          </div>
        ))}
      </dl>

      <AgentCatalogToolbar
        filter={filter}
        query={query}
        resultCount={filteredAgents.length}
        totalCount={state.viewModel.agents.length}
        onFilterChange={setFilter}
        onQueryChange={setQuery}
        onReset={resetFilters}
      />

      {selectedAgent ? (
        <div className={styles.content}>
          <AgentCatalogList
            agents={filteredAgents}
            selectedId={selectedAgent.id}
            onSelect={setSelectedId}
          />
          <AgentCatalogDetail agent={selectedAgent} />
        </div>
      ) : (
        <div className={styles.noMatches}>
          <StatePanel
            state="empty"
            title="No matching agents"
            detail="Adjust the search or catalog-stage filter."
          />
        </div>
      )}
    </div>
  );
}
