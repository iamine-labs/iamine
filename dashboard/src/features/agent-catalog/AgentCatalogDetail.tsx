import { Bot, CheckCircle2, ShieldX } from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  AgentCatalogEntry,
  AgentCatalogStage,
} from '../../contracts/view-models/agentCatalog';
import styles from './AgentCatalogDetail.module.css';

const stageTones: Record<AgentCatalogStage, StatusTone> = {
  reference: 'success',
  next: 'warning',
  planned: 'neutral',
};

export function AgentCatalogDetail({ agent }: { agent: AgentCatalogEntry }) {
  return (
    <aside className={styles.panel} aria-labelledby="selected-agent-name">
      <header className={styles.header}>
        <span className={styles.icon} aria-hidden="true">
          <Bot size={22} />
        </span>
        <div>
          <span>Selected preview</span>
          <h3 id="selected-agent-name">{agent.name}</h3>
        </div>
        <StatusBadge tone={stageTones[agent.stage]}>
          {agent.stageLabel}
        </StatusBadge>
      </header>

      <p className={styles.description}>{agent.description}</p>

      <dl className={styles.facts}>
        <div>
          <dt>Role</dt>
          <dd>{agent.roleLabel}</dd>
        </div>
        <div>
          <dt>Operating mode</dt>
          <dd>{agent.operatingMode}</dd>
        </div>
        <div>
          <dt>Platform</dt>
          <dd>{agent.platformLabel}</dd>
        </div>
        <div>
          <dt>Package stage</dt>
          <dd>{agent.packageStage}</dd>
        </div>
      </dl>

      <section className={styles.detailSection}>
        <h4>Declared capabilities</h4>
        <ul>
          {agent.capabilities.map((capability) => (
            <li key={capability}>
              <CheckCircle2 size={14} aria-hidden="true" />
              <span>{capability}</span>
            </li>
          ))}
        </ul>
      </section>

      <section className={styles.detailSection}>
        <h4>Boundary preview</h4>
        <ul>
          {agent.boundaries.map((boundary) => (
            <li key={boundary}>
              <ShieldX size={14} aria-hidden="true" />
              <span>{boundary}</span>
            </li>
          ))}
        </ul>
      </section>

      <footer className={styles.footer}>
        Preview metadata only. No local package or runtime state is queried.
      </footer>
    </aside>
  );
}
