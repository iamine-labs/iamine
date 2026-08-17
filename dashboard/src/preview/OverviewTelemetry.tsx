import { Clock3, MoreHorizontal } from 'lucide-react';

import { Button, IconButton } from '../components';
import { DashboardPanel } from './DashboardPanel';
import { activityEntries, systemLogs, trafficSeries } from './fixtures';
import { DonutChart, Sparkline } from './PreviewCharts';
import styles from './OverviewPanels.module.css';

export function OverviewTelemetry() {
  return (
    <>
      <DashboardPanel
        title="Node status"
        action={
          <Button size="small" variant="quiet">
            View all
          </Button>
        }
      >
        <div className={styles.nodeStatusGrid}>
          <div>
            <strong className={styles.greenText}>21</strong>
            <span>Online</span>
          </div>
          <div>
            <strong className={styles.copperText}>2</strong>
            <span>Degraded</span>
          </div>
          <div>
            <strong className={styles.blueText}>1</strong>
            <span>Maintenance</span>
          </div>
          <div>
            <strong className={styles.redText}>0</strong>
            <span>Offline</span>
          </div>
        </div>
      </DashboardPanel>

      <DashboardPanel
        title="Network traffic"
        action={
          <Button size="small" variant="quiet">
            24h
          </Button>
        }
      >
        <div className={styles.trafficSummary}>
          <span>
            Inbound <strong>2.34 TB</strong>
          </span>
          <span>
            Outbound <strong>1.78 TB</strong>
          </span>
        </div>
        <div className={styles.trafficChart}>
          <Sparkline
            ariaLabel="Inbound traffic sample trend"
            color="green"
            points={trafficSeries.inbound}
          />
          <Sparkline
            ariaLabel="Outbound traffic sample trend"
            color="copper"
            points={trafficSeries.outbound}
          />
        </div>
      </DashboardPanel>

      <DashboardPanel title="Inferences (24h)">
        <div className={styles.inferenceContent}>
          <DonutChart total={1264} />
          <dl>
            <div>
              <dt className={styles.greenText}>Completed</dt>
              <dd>1,048</dd>
            </div>
            <div>
              <dt className={styles.copperText}>Pending</dt>
              <dd>164</dd>
            </div>
            <div>
              <dt className={styles.redText}>Failed</dt>
              <dd>52</dd>
            </div>
          </dl>
        </div>
      </DashboardPanel>

      <DashboardPanel
        className={styles.activityPanel}
        title="Recent activity"
        action={
          <Button size="small" variant="quiet">
            View history
          </Button>
        }
      >
        <ol className={styles.activityList}>
          {activityEntries.map((entry) => (
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
          />
        }
      >
        <div className={styles.logList} role="log" aria-label="Preview logs">
          {systemLogs.map((line) => (
            <code key={line}>{line}</code>
          ))}
        </div>
      </DashboardPanel>
    </>
  );
}
