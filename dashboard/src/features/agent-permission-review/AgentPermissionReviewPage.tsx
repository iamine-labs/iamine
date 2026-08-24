import { ArrowLeft, Eye } from 'lucide-react';
import { useEffect, useState } from 'react';

import { Button, StatePanel, StatusBadge } from '../../components';
import type {
  AgentPermissionDecision,
  AgentPermissionReviewDataSource,
  AgentPermissionReviewViewModel,
} from '../../contracts/view-models/agentPermissionReview';
import { agentPermissionReviewMockDataSource } from '../../mocks/agentPermissionReviewMockDataSource';
import { PermissionAuditPreview } from './PermissionAuditPreview';
import { PermissionDecisionPanel } from './PermissionDecisionPanel';
import { PermissionRequestPanel } from './PermissionRequestPanel';
import styles from './AgentPermissionReviewPage.module.css';

type PermissionReviewState =
  | { status: 'loading' }
  | { status: 'ready'; viewModel: AgentPermissionReviewViewModel }
  | { status: 'empty' }
  | { status: 'error' };

interface AgentPermissionReviewPageProps {
  agentId: string;
  dataSource?: AgentPermissionReviewDataSource;
  onBack: () => void;
}

export function AgentPermissionReviewPage({
  agentId,
  dataSource = agentPermissionReviewMockDataSource,
  onBack,
}: AgentPermissionReviewPageProps) {
  const [attempt, setAttempt] = useState(0);
  const [acknowledged, setAcknowledged] = useState(false);
  const [decision, setDecision] = useState<AgentPermissionDecision>('pending');
  const [state, setState] = useState<PermissionReviewState>({
    status: 'loading',
  });

  useEffect(() => {
    let active = true;

    void dataSource
      .load(agentId)
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
  }, [agentId, attempt, dataSource]);

  const retry = () => {
    setState({ status: 'loading' });
    setAttempt((value) => value + 1);
  };

  const reset = () => {
    setAcknowledged(false);
    setDecision('pending');
  };

  if (state.status === 'loading') {
    return (
      <div className={styles.state}>
        <StatePanel
          state="loading"
          title="Loading permission review preview"
          detail="Preparing deterministic local presentation data."
        />
      </div>
    );
  }

  if (state.status === 'empty') {
    return (
      <div className={styles.stateStack}>
        <StatePanel
          state="empty"
          title="Permission preview unavailable for this agent"
          detail="No exact local presentation fixture matches this agent."
        />
        <Button
          size="small"
          variant="secondary"
          leadingIcon={<ArrowLeft size={15} />}
          onClick={onBack}
        >
          Return to Agent catalog
        </Button>
      </div>
    );
  }

  if (state.status === 'error') {
    return (
      <div className={styles.stateStack}>
        <StatePanel
          state="error"
          title="Permission review preview unavailable"
          detail="The local presentation source could not be loaded."
          onRetry={retry}
        />
        <Button
          size="small"
          variant="quiet"
          leadingIcon={<ArrowLeft size={15} />}
          onClick={onBack}
        >
          Return to Agent catalog
        </Button>
      </div>
    );
  }

  const review = state.viewModel;

  return (
    <div
      className={styles.page}
      data-source={dataSource.kind}
      data-decision={decision}
    >
      <div className={styles.backAction}>
        <Button
          size="small"
          variant="quiet"
          leadingIcon={<ArrowLeft size={15} />}
          onClick={onBack}
        >
          Agent catalog
        </Button>
      </div>

      <header className={styles.pageHeader}>
        <div>
          <StatusBadge tone="info">Preview data</StatusBadge>
          <h2>{review.title}</h2>
          <p>{review.subtitle}</p>
        </div>
        <span className={styles.provenance}>
          <Eye size={15} aria-hidden="true" />
          {review.provenance.label}
        </span>
      </header>

      <dl className={styles.facts} aria-label="Permission preview boundaries">
        {review.facts.map((fact) => (
          <div key={fact.label}>
            <dt>{fact.label}</dt>
            <dd>{fact.value}</dd>
          </div>
        ))}
      </dl>

      <div className={styles.reviewGrid}>
        <PermissionRequestPanel review={review} />
        <PermissionDecisionPanel
          acknowledged={acknowledged}
          decision={decision}
          onAcknowledgedChange={setAcknowledged}
          onConfirm={() => {
            if (acknowledged) setDecision('confirmed');
          }}
          onDeny={() => setDecision('denied')}
          onReset={reset}
        />
      </div>

      <PermissionAuditPreview
        baselineEvents={review.auditEvents}
        decision={decision}
      />
    </div>
  );
}
