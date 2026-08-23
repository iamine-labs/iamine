import { ChevronRight, Stethoscope } from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  DiagnosticsCheck,
  DiagnosticsStatus,
} from '../../contracts/view-models/diagnostics';
import styles from './DiagnosticsList.module.css';

const statusTones: Record<DiagnosticsStatus, StatusTone> = {
  healthy: 'success',
  attention: 'warning',
  unavailable: 'neutral',
};

interface DiagnosticsListProps {
  checks: readonly DiagnosticsCheck[];
  onSelect: (id: string) => void;
  selectedId: string;
}

export function DiagnosticsList({
  checks,
  onSelect,
  selectedId,
}: DiagnosticsListProps) {
  return (
    <section className={styles.panel} aria-labelledby="diagnostics-results">
      <header className={styles.header}>
        <div>
          <h3 id="diagnostics-results">Synthetic checks</h3>
          <p>Bounded presentation data, disconnected from every device.</p>
        </div>
        <span>{checks.length} shown</span>
      </header>
      <div
        className={styles.tableFrame}
        role="region"
        aria-label="Diagnostics preview results scrollable table"
        tabIndex={0}
      >
        <table className={styles.table}>
          <thead>
            <tr>
              <th scope="col">Check</th>
              <th scope="col">Category</th>
              <th scope="col">Scope</th>
              <th scope="col">Status</th>
              <th scope="col">
                <span className="sr-only">Preview detail</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {checks.map((check) => {
              const selected = check.id === selectedId;

              return (
                <tr key={check.id} data-selected={selected}>
                  <td>
                    <button
                      className={styles.checkButton}
                      type="button"
                      aria-label={`Select ${check.title} preview`}
                      aria-pressed={selected}
                      onClick={() => onSelect(check.id)}
                    >
                      <span className={styles.checkIcon} aria-hidden="true">
                        <Stethoscope size={17} />
                      </span>
                      <span>
                        <strong>{check.title}</strong>
                        <small>{check.summary}</small>
                      </span>
                    </button>
                  </td>
                  <td>{check.categoryLabel}</td>
                  <td>{check.scopeLabel}</td>
                  <td>
                    <StatusBadge tone={statusTones[check.status]}>
                      {check.statusLabel}
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
