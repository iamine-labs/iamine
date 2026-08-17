import { Clock3, MoreHorizontal } from 'lucide-react';

import { Button, IconButton } from '../../components';
import type { OverviewViewModel } from '../../contracts/view-models/overview';
import { DashboardPanel } from './DashboardPanel';
import { DonutChart, Sparkline } from './OverviewCharts';
import styles from './OverviewPanels.module.css';

interface OverviewTelemetryProps {
  viewModel: OverviewViewModel;
}

export function OverviewTelemetry({ viewModel }: OverviewTelemetryProps) {
  const { activity, inferences, logs, nodeStatus, traffic } = viewModel;

  return (
    <>
      <DashboardPanel
        title="Node status"
        action={
          <Button size="small" variant="quiet" disabled>
            View all
          </Button>
        }
      >
        <div className={styles.nodeStatusGrid}>
          {nodeStatus.items.map((item) => (
            <div key={item.label}>
              <strong className={styles[`${item.tone}Text`]}>
                {item.value}
              </strong>
              <span>{item.label}</span>
            </div>
          ))}
        </div>
      </DashboardPanel>

      <DashboardPanel
        title="Network traffic"
        action={
          <span className={styles.periodLabel}>{traffic.periodLabel}</span>
        }
      >
        <div className={styles.trafficSummary}>
          {traffic.totals.map((total) => (
            <span key={total.label}>
              {total.label} <strong>{total.value}</strong>
            </span>
          ))}
        </div>
        <div className={styles.trafficChart}>
          <Sparkline
            ariaLabel="Inbound traffic sample trend"
            color="green"
            points={traffic.inbound}
          />
          <Sparkline
            ariaLabel="Outbound traffic sample trend"
            color="copper"
            points={traffic.outbound}
          />
        </div>
      </DashboardPanel>

      <DashboardPanel title={`Inferences (${inferences.periodLabel})`}>
        <div className={styles.inferenceContent}>
          <DonutChart
            total={inferences.total}
            completed={inferences.completed}
            pending={inferences.pending}
            failed={inferences.failed}
          />
          <dl>
            <div>
              <dt className={styles.greenText}>Completed</dt>
              <dd>{inferences.completed.toLocaleString()}</dd>
            </div>
            <div>
              <dt className={styles.copperText}>Pending</dt>
              <dd>{inferences.pending.toLocaleString()}</dd>
            </div>
            <div>
              <dt className={styles.redText}>Failed</dt>
              <dd>{inferences.failed.toLocaleString()}</dd>
            </div>
          </dl>
        </div>
      </DashboardPanel>

      <DashboardPanel
        className={styles.activityPanel}
        title="Recent activity"
        action={
          <Button size="small" variant="quiet" disabled>
            View history
          </Button>
        }
      >
        <ol className={styles.activityList}>
          {activity.map((entry) => (
            <li key={`${entry.time}-${entry.event}`}>
              <time>{entry.time}</time>
              <Clock3 size={13} aria-hidden="true" />
              <span>{entry.event}</span>
            </li>
          ))}
        </ol>
      </DashboardPanel>

      <DashboardPanel
        className={styles.logsPanel}
        title="System logs"
        action={
          <IconButton
            size="small"
            label="System log preview options"
            icon={<MoreHorizontal size={16} />}
            disabled
          />
        }
      >
        <div className={styles.logList} role="log" aria-label="Preview logs">
          {logs.map((line) => (
            <code key={line}>{line}</code>
          ))}
        </div>
      </DashboardPanel>
    </>
  );
}
