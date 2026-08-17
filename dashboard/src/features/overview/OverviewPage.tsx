import { useEffect, useState } from 'react';

import type {
  OverviewDataSource,
  OverviewViewModel,
} from '../../contracts/view-models/overview';
import { overviewMockDataSource } from '../../mocks/overviewMockDataSource';
import { StatePanel } from '../../components';
import { OverviewSummary } from './OverviewSummary';
import { OverviewTelemetry } from './OverviewTelemetry';
import styles from './OverviewPage.module.css';

type OverviewState =
  | { status: 'loading' }
  | { status: 'ready'; viewModel: OverviewViewModel }
  | { status: 'empty' }
  | { status: 'error' };

interface OverviewPageProps {
  dataSource?: OverviewDataSource;
  onOpenNodes: () => void;
}

export function OverviewPage({
  dataSource = overviewMockDataSource,
  onOpenNodes,
}: OverviewPageProps) {
  const [attempt, setAttempt] = useState(0);
  const [state, setState] = useState<OverviewState>({ status: 'loading' });

  useEffect(() => {
    let active = true;

    void dataSource
      .load()
      .then((viewModel) => {
        if (!active) return;
        setState(
          viewModel ? { status: 'ready', viewModel } : { status: 'empty' },
        );
      })
      .catch(() => {
        if (active) setState({ status: 'error' });
      });

    return () => {
      active = false;
    };
  }, [attempt, dataSource]);

  if (state.status === 'loading') {
    return (
      <div className={styles.state}>
        <StatePanel
          state="loading"
          title="Loading overview preview"
          detail="Preparing deterministic local preview data."
        />
      </div>
    );
  }

  if (state.status === 'empty') {
    return (
      <div className={styles.state}>
        <StatePanel
          state="empty"
          title="No overview preview data"
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
          title="Overview preview unavailable"
          detail="The local presentation source could not be loaded."
          onRetry={retry}
        />
      </div>
    );
  }

  return (
    <div className={styles.overviewGrid} data-source={dataSource.kind}>
      <p className="sr-only">{state.viewModel.provenance.label}</p>
      <OverviewSummary viewModel={state.viewModel} onOpenNodes={onOpenNodes} />
      <OverviewTelemetry viewModel={state.viewModel} />
    </div>
  );
}
