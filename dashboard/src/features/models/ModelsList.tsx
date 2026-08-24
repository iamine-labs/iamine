import { Box, ChevronRight } from 'lucide-react';

import { StatusBadge, type StatusTone } from '../../components';
import type {
  ModelPreviewItem,
  ModelPreviewState,
} from '../../contracts/view-models/models';
import styles from './ModelsList.module.css';

const stateTones: Record<ModelPreviewState, StatusTone> = {
  shown: 'success',
  attention: 'warning',
  unavailable: 'neutral',
};

interface ModelsListProps {
  models: readonly ModelPreviewItem[];
  onSelect: (id: string) => void;
  selectedId: string;
}

export function ModelsList({ models, onSelect, selectedId }: ModelsListProps) {
  return (
    <section className={styles.panel} aria-labelledby="models-results">
      <header className={styles.header}>
        <div>
          <h3 id="models-results">Synthetic model library</h3>
          <p>Presentation labels only, disconnected from every registry.</p>
        </div>
        <span>{models.length} shown</span>
      </header>
      <div
        className={styles.tableFrame}
        role="region"
        aria-label="Models preview results scrollable table"
        tabIndex={0}
      >
        <table className={styles.table}>
          <thead>
            <tr>
              <th scope="col">Model</th>
              <th scope="col">Category</th>
              <th scope="col">Display class</th>
              <th scope="col">Preview state</th>
              <th scope="col">
                <span className="sr-only">Preview detail</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {models.map((model) => {
              const selected = model.id === selectedId;

              return (
                <tr key={model.id} data-selected={selected}>
                  <td>
                    <button
                      className={styles.modelButton}
                      type="button"
                      aria-label={`Select ${model.name}`}
                      aria-pressed={selected}
                      onClick={() => onSelect(model.id)}
                    >
                      <span className={styles.modelIcon} aria-hidden="true">
                        <Box size={17} />
                      </span>
                      <span>
                        <strong>{model.name}</strong>
                        <small>{model.summary}</small>
                      </span>
                    </button>
                  </td>
                  <td>{model.categoryLabel}</td>
                  <td>{model.displayClassLabel}</td>
                  <td>
                    <StatusBadge tone={stateTones[model.previewState]}>
                      {model.previewStateLabel}
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
