import { Boxes, ChevronRight } from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  NodePreviewNode,
  NodePreviewStatus,
} from '../../contracts/view-models/nodes';
import styles from './NodesList.module.css';

const statusTones: Record<NodePreviewStatus, StatusTone> = {
  available: 'success',
  limited: 'warning',
  offline: 'neutral',
};

interface NodesListProps {
  nodes: readonly NodePreviewNode[];
  onSelect: (id: string) => void;
  selectedId: string;
}

export function NodesList({ nodes, onSelect, selectedId }: NodesListProps) {
  return (
    <section className={styles.panel} aria-labelledby="nodes-results">
      <header className={styles.header}>
        <div>
          <h3 id="nodes-results">Synthetic node inventory</h3>
          <p>Presentation labels only, disconnected from every device.</p>
        </div>
        <span>{nodes.length} shown</span>
      </header>
      <div
        className={styles.tableFrame}
        role="region"
        aria-label="Nodes preview results scrollable table"
        tabIndex={0}
      >
        <table className={styles.table}>
          <thead>
            <tr>
              <th scope="col">Node</th>
              <th scope="col">Role</th>
              <th scope="col">Environment</th>
              <th scope="col">Status</th>
              <th scope="col">
                <span className="sr-only">Preview detail</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {nodes.map((node) => {
              const selected = node.id === selectedId;

              return (
                <tr key={node.id} data-selected={selected}>
                  <td>
                    <button
                      className={styles.nodeButton}
                      type="button"
                      aria-label={`Select ${node.name}`}
                      aria-pressed={selected}
                      onClick={() => onSelect(node.id)}
                    >
                      <span className={styles.nodeIcon} aria-hidden="true">
                        <Boxes size={17} />
                      </span>
                      <span>
                        <strong>{node.name}</strong>
                        <small>{node.summary}</small>
                      </span>
                    </button>
                  </td>
                  <td>{node.roleLabel}</td>
                  <td>{node.environmentLabel}</td>
                  <td>
                    <StatusBadge tone={statusTones[node.status]}>
                      {node.statusLabel}
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
