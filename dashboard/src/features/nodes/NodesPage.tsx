import { Eye } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';

import { StatePanel, StatusBadge } from '../../components';
import type {
  NodesDataSource,
  NodesViewModel,
} from '../../contracts/view-models/nodes';
import { nodesMockDataSource } from '../../mocks/nodesMockDataSource';
import { NodesDetail } from './NodesDetail';
import { NodesList } from './NodesList';
import {
  NodesToolbar,
  type NodesCapabilityFilter,
  type NodesStatusFilter,
} from './NodesToolbar';
import styles from './NodesPage.module.css';

type NodesState =
  | { status: 'loading' }
  | { status: 'ready'; viewModel: NodesViewModel }
  | { status: 'empty' }
  | { status: 'error' };

export function NodesPage({
  dataSource = nodesMockDataSource,
}: {
  dataSource?: NodesDataSource;
}) {
  const [attempt, setAttempt] = useState(0);
  const [status, setStatus] = useState<NodesStatusFilter>('all');
  const [capability, setCapability] = useState<NodesCapabilityFilter>('all');
  const [query, setQuery] = useState('');
  const [selectedId, setSelectedId] = useState('');
  const [state, setState] = useState<NodesState>({ status: 'loading' });

  useEffect(() => {
    let active = true;

    void dataSource
      .load()
      .then((viewModel) => {
        if (!active) return;
        setState(
          viewModel ? { status: 'ready', viewModel } : { status: 'empty' },
        );
        setSelectedId(viewModel?.nodes[0]?.id ?? '');
      })
      .catch(() => {
        if (active) setState({ status: 'error' });
      });

    return () => {
      active = false;
    };
  }, [attempt, dataSource]);

  const filteredNodes = useMemo(() => {
    if (state.status !== 'ready') return [];
    const normalizedQuery = query.trim().toLocaleLowerCase();

    return state.viewModel.nodes.filter((node) => {
      const matchesStatus = status === 'all' || node.status === status;
      const matchesCapability =
        capability === 'all' ||
        node.capabilities.some((item) => item.id === capability);
      const searchable = [
        node.name,
        node.roleLabel,
        node.environmentLabel,
        node.summary,
        node.capacityLabel,
        ...node.capabilities.map((item) => item.label),
      ]
        .join(' ')
        .toLocaleLowerCase();
      return (
        matchesStatus &&
        matchesCapability &&
        searchable.includes(normalizedQuery)
      );
    });
  }, [capability, query, state, status]);

  if (state.status === 'loading') {
    return (
      <div className={styles.state}>
        <StatePanel
          state="loading"
          title="Loading nodes preview"
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
          title="No nodes preview data"
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
          title="Nodes preview unavailable"
          detail="The local presentation source could not be loaded."
          onRetry={retry}
        />
      </div>
    );
  }

  const selectedNode =
    filteredNodes.find((node) => node.id === selectedId) ?? filteredNodes[0];

  const resetFilters = () => {
    setStatus('all');
    setCapability('all');
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

      <dl className={styles.metrics} aria-label="Nodes preview summary">
        {state.viewModel.metrics.map((metric) => (
          <div key={metric.label}>
            <dt>{metric.label}</dt>
            <dd>{metric.value}</dd>
          </div>
        ))}
      </dl>

      <NodesToolbar
        capability={capability}
        query={query}
        resultCount={filteredNodes.length}
        status={status}
        totalCount={state.viewModel.nodes.length}
        onCapabilityChange={setCapability}
        onQueryChange={setQuery}
        onReset={resetFilters}
        onStatusChange={setStatus}
      />

      {selectedNode ? (
        <div className={styles.content}>
          <NodesList
            nodes={filteredNodes}
            selectedId={selectedNode.id}
            onSelect={setSelectedId}
          />
          <NodesDetail node={selectedNode} />
        </div>
      ) : (
        <div className={styles.noMatches}>
          <StatePanel
            state="empty"
            title="No matching nodes"
            detail="Adjust the search or preview filters."
          />
        </div>
      )}
    </div>
  );
}
