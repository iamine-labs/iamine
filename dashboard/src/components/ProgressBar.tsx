import styles from './ProgressBar.module.css';

interface ProgressBarProps {
  label: string;
  value: number;
  valueLabel?: string;
  tone?: 'brand' | 'info' | 'warning';
}

export function ProgressBar({
  label,
  tone = 'brand',
  value,
  valueLabel,
}: ProgressBarProps) {
  const boundedValue = Math.max(0, Math.min(100, value));

  return (
    <div className={styles.root}>
      <div className={styles.header}>
        <span>{label}</span>
        <span className={styles.value}>{valueLabel ?? `${boundedValue}%`}</span>
      </div>
      <div
        className={styles.track}
        role="progressbar"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={boundedValue}
      >
        <span
          className={`${styles.fill} ${styles[tone]}`}
          style={{ width: `${boundedValue}%` }}
        />
      </div>
    </div>
  );
}
