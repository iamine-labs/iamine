import { Bot, ChevronRight, Gauge, MoreHorizontal, Server } from 'lucide-react';

import { Button, IconButton, StatusBadge } from '../components';
import { DashboardPanel } from './DashboardPanel';
import { type DashboardView, queueSeries, resourceMetrics } from './fixtures';
import { Sparkline } from './PreviewCharts';
import styles from './OverviewPanels.module.css';

interface OverviewSummaryProps {
  onNavigate: (view: DashboardView) => void;
}

export function OverviewSummary({ onNavigate }: OverviewSummaryProps) {
  return (
    <>
      <section className={styles.operationalPanel}>
        <img
          className={styles.operationalBackdrop}
          src="/assets/iamine-network-wallpaper.png"
          alt=""
        />
        <div className={styles.operationalContent}>
          <div>
            <div className={styles.operationalTitle}>
              <h2>System operational</h2>
              <span role="img" aria-label="Online" />
            </div>
            <p>24 nodes online · 18 active agents · 98.6% availability</p>
          </div>
          <Button
            size="small"
            variant="secondary"
            onClick={() => onNavigate('nodes')}
          >
            View global
          </Button>
        </div>
      </section>

      <DashboardPanel
        className={styles.localNodePanel}
        title="Local node"
        action={<StatusBadge tone="success">Online</StatusBadge>}
      >
        <div className={styles.localNodeContent}>
          <div className={styles.nodeIdentity}>
            <strong>NODE-LOCAL-01</strong>
            <dl>
              <div>
                <dt>Uptime</dt>
                <dd>12d 6h 24m</dd>
              </div>
              <div>
                <dt>Version</dt>
                <dd>1.2.0-beta</dd>
              </div>
              <div>
                <dt>Role</dt>
                <dd>Compute · Inference</dd>
              </div>
            </dl>
          </div>
          <div className={styles.serverStack} aria-hidden="true">
            <Server size={58} />
            <Server size={58} />
          </div>
        </div>
      </DashboardPanel>

      <DashboardPanel
        title="Local resources"
        action={
          <IconButton
            size="small"
            label="Resource details preview"
            icon={<MoreHorizontal size={16} />}
          />
        }
      >
        <div className={styles.resourceList}>
          {resourceMetrics.map((metric) => (
            <div className={styles.resourceRow} key={metric.label}>
              <span>{metric.label}</span>
              <div className={styles.resourceTrack}>
                <span
                  className={styles[metric.tone]}
                  style={{ width: `${metric.value}%` }}
                />
              </div>
              <strong>{metric.amount}</strong>
              <small>{metric.detail}</small>
            </div>
          ))}
        </div>
        <div className={styles.resourceFooter}>
          <span>Network allocation 50%</span>
          <Button size="small" variant="secondary">
            Manage
          </Button>
        </div>
      </DashboardPanel>

      <DashboardPanel
        className={styles.agentPanel}
        title="Active agent"
        action={<ChevronRight size={18} aria-hidden="true" />}
      >
        <div className={styles.agentIdentity}>
          <span className={styles.agentIcon} aria-hidden="true">
            <Bot size={25} />
          </span>
          <div>
            <strong>Coder Agent</strong>
            <StatusBadge tone="warning">Inference</StatusBadge>
            <p>Inference #A492</p>
          </div>
        </div>
        <dl className={styles.agentDetails}>
          <div>
            <dt>Model</dt>
            <dd>StarCoder</dd>
          </div>
          <div>
            <dt>Elapsed</dt>
            <dd>00:00:51.8</dd>
          </div>
          <div>
            <dt>Origin node</dt>
            <dd>NODE-DELTA-09</dd>
          </div>
        </dl>
        <div className={styles.agentProgress}>
          <span style={{ width: '68%' }} />
        </div>
        <div className={styles.agentFooter}>
          <span>68%</span>
          <Button size="small" variant="secondary">
            View details
          </Button>
        </div>
      </DashboardPanel>

      <DashboardPanel
        title="Inference queue"
        action={
          <Button size="small" variant="quiet">
            View all
          </Button>
        }
      >
        <div className={styles.queueList}>
          <div>
            <strong className={styles.copperText}>3</strong>
            <span>Running</span>
            <Sparkline
              ariaLabel="Running sample trend"
              color="copper"
              points={queueSeries.running}
            />
          </div>
          <div>
            <strong className={styles.copperText}>12</strong>
            <span>Pending</span>
            <Sparkline
              ariaLabel="Pending sample trend"
              color="blue"
              points={queueSeries.pending}
            />
          </div>
          <div>
            <strong className={styles.greenText}>148</strong>
            <span>Completed</span>
            <Sparkline
              ariaLabel="Completed sample trend"
              color="green"
              points={queueSeries.completed}
            />
          </div>
          <div>
            <strong className={styles.redText}>2</strong>
            <span>Failed</span>
            <Sparkline
              ariaLabel="Failed sample trend"
              color="red"
              points={queueSeries.failed}
            />
          </div>
        </div>
        <div className={styles.queueCapacity}>
          <Gauge size={15} />
          <span>Capacity within preview threshold</span>
        </div>
      </DashboardPanel>
    </>
  );
}
