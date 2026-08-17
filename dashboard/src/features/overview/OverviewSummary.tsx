import { Bot, ChevronRight, Gauge, MoreHorizontal, Server } from 'lucide-react';

import { Button, IconButton, StatusBadge } from '../../components';
import type { OverviewViewModel } from '../../contracts/view-models/overview';
import { DashboardPanel } from './DashboardPanel';
import { Sparkline } from './OverviewCharts';
import styles from './OverviewPanels.module.css';

interface OverviewSummaryProps {
  onOpenNodes: () => void;
  viewModel: OverviewViewModel;
}

export function OverviewSummary({
  onOpenNodes,
  viewModel,
}: OverviewSummaryProps) {
  const { activeAgent, localNode, operational, queue, resources } = viewModel;

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
              <h2>{operational.title}</h2>
              <span
                role="img"
                aria-label={operational.online ? 'Online' : 'Unavailable'}
              />
            </div>
            <p>{operational.summary}</p>
          </div>
          <Button size="small" variant="secondary" onClick={onOpenNodes}>
            View global
          </Button>
        </div>
      </section>

      <DashboardPanel
        className={styles.localNodePanel}
        title="Local node"
        action={
          <StatusBadge tone="success">{localNode.statusLabel}</StatusBadge>
        }
      >
        <div className={styles.localNodeContent}>
          <div className={styles.nodeIdentity}>
            <strong>{localNode.name}</strong>
            <dl>
              {localNode.facts.map((fact) => (
                <div key={fact.label}>
                  <dt>{fact.label}</dt>
                  <dd>{fact.value}</dd>
                </div>
              ))}
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
            disabled
          />
        }
      >
        <div className={styles.resourceList}>
          {resources.metrics.map((metric) => (
            <div className={styles.resourceRow} key={metric.label}>
              <span>{metric.label}</span>
              <div className={styles.resourceTrack}>
                <span
                  className={styles[metric.tone]}
                  style={{ width: `${metric.percent}%` }}
                />
              </div>
              <strong>{metric.value}</strong>
              <small>{metric.detail}</small>
            </div>
          ))}
        </div>
        <div className={styles.resourceFooter}>
          <span>{resources.allocationLabel}</span>
          <Button size="small" variant="secondary" disabled>
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
            <strong>{activeAgent.name}</strong>
            <StatusBadge tone="warning">{activeAgent.stateLabel}</StatusBadge>
            <p>{activeAgent.operationLabel}</p>
          </div>
        </div>
        <dl className={styles.agentDetails}>
          {activeAgent.facts.map((fact) => (
            <div key={fact.label}>
              <dt>{fact.label}</dt>
              <dd>{fact.value}</dd>
            </div>
          ))}
        </dl>
        <div className={styles.agentProgress}>
          <span style={{ width: `${activeAgent.progressPercent}%` }} />
        </div>
        <div className={styles.agentFooter}>
          <span>{activeAgent.progressPercent}%</span>
          <Button size="small" variant="secondary" disabled>
            View details
          </Button>
        </div>
      </DashboardPanel>

      <DashboardPanel
        title="Inference queue"
        action={
          <Button size="small" variant="quiet" disabled>
            View all
          </Button>
        }
      >
        <div className={styles.queueList}>
          {queue.items.map((item) => (
            <div key={item.label}>
              <strong className={styles[`${item.tone}Text`]}>
                {item.count}
              </strong>
              <span>{item.label}</span>
              <Sparkline
                ariaLabel={`${item.label} sample trend`}
                color={item.tone}
                points={item.points}
              />
            </div>
          ))}
        </div>
        <div className={styles.queueCapacity}>
          <Gauge size={15} />
          <span>{queue.capacityLabel}</span>
        </div>
      </DashboardPanel>
    </>
  );
}
