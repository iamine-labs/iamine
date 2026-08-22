import { Bot, ChevronRight } from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  AgentCatalogEntry,
  AgentCatalogStage,
} from '../../contracts/view-models/agentCatalog';
import styles from './AgentCatalogList.module.css';

const stageTones: Record<AgentCatalogStage, StatusTone> = {
  reference: 'success',
  next: 'warning',
  planned: 'neutral',
};

interface AgentCatalogListProps {
  agents: readonly AgentCatalogEntry[];
  onSelect: (id: string) => void;
  selectedId: string;
}

export function AgentCatalogList({
  agents,
  onSelect,
  selectedId,
}: AgentCatalogListProps) {
  return (
    <section className={styles.panel} aria-labelledby="agent-catalog-results">
      <header className={styles.header}>
        <div>
          <h3 id="agent-catalog-results">Official agent roles</h3>
          <p>Presentation metadata from the current P0 preview sequence.</p>
        </div>
        <span>{agents.length} shown</span>
      </header>
      <div
        className={styles.tableFrame}
        role="region"
        aria-label="Agent catalog results scrollable table"
        tabIndex={0}
      >
        <table className={styles.table}>
          <thead>
            <tr>
              <th scope="col">Agent</th>
              <th scope="col">Role</th>
              <th scope="col">Mode</th>
              <th scope="col">Stage</th>
              <th scope="col">
                <span className="sr-only">Preview detail</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {agents.map((agent) => {
              const selected = agent.id === selectedId;

              return (
                <tr key={agent.id} data-selected={selected}>
                  <td>
                    <button
                      className={styles.agentButton}
                      type="button"
                      aria-label={`Select ${agent.name} preview`}
                      aria-pressed={selected}
                      onClick={() => onSelect(agent.id)}
                    >
                      <span className={styles.agentIcon} aria-hidden="true">
                        <Bot size={17} />
                      </span>
                      <span>
                        <strong>{agent.name}</strong>
                        <small>{agent.description}</small>
                      </span>
                    </button>
                  </td>
                  <td>{agent.roleLabel}</td>
                  <td>{agent.operatingMode}</td>
                  <td>
                    <StatusBadge tone={stageTones[agent.stage]}>
                      {agent.stageLabel}
                    </StatusBadge>
                  </td>
                  <td>
                    <ChevronRight size={16} aria-hidden="true" />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
