import type { ReactNode } from 'react';

import styles from './DataTable.module.css';

export interface DataColumn<Row> {
  key: string;
  header: string;
  render: (row: Row) => ReactNode;
  align?: 'start' | 'end';
}

interface DataTableProps<Row> {
  caption: string;
  columns: readonly DataColumn<Row>[];
  getRowKey: (row: Row) => string;
  rows: readonly Row[];
}

export function DataTable<Row>({
  caption,
  columns,
  getRowKey,
  rows,
}: DataTableProps<Row>) {
  return (
    <div
      className={styles.frame}
      data-overflow-allowed="true"
      role="region"
      aria-label={`${caption} scrollable table`}
      tabIndex={0}
    >
      <table className={styles.table}>
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr>
            {columns.map((column) => (
              <th
                key={column.key}
                className={column.align === 'end' ? styles.end : undefined}
                scope="col"
              >
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={getRowKey(row)}>
              {columns.map((column) => (
                <td
                  key={column.key}
                  className={column.align === 'end' ? styles.end : undefined}
                >
                  {column.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
