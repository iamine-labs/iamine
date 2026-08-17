import styles from './OverviewCharts.module.css';

interface SparklineProps {
  ariaLabel: string;
  color: 'blue' | 'copper' | 'green' | 'red';
  points: readonly number[];
}

export function Sparkline({ ariaLabel, color, points }: SparklineProps) {
  const visiblePoints = points.length > 0 ? points : [0];
  const maximum = Math.max(...visiblePoints);
  const minimum = Math.min(...visiblePoints);
  const range = Math.max(maximum - minimum, 1);
  const coordinates = visiblePoints
    .map((point, index) => {
      const x = (index / Math.max(visiblePoints.length - 1, 1)) * 100;
      const y = 32 - ((point - minimum) / range) * 26;
      return `${x},${y}`;
    })
    .join(' ');

  return (
    <svg
      className={`${styles.sparkline} ${styles[color]}`}
      viewBox="0 0 100 36"
      role="img"
      aria-label={ariaLabel}
      preserveAspectRatio="none"
    >
      <polyline points={coordinates} />
    </svg>
  );
}

interface DonutChartProps {
  total: number;
  completed: number;
  pending: number;
  failed: number;
}

export function DonutChart({
  total,
  completed,
  pending,
  failed,
}: DonutChartProps) {
  const radius = 34;
  const circumference = 2 * Math.PI * radius;
  const unit = total > 0 ? circumference / total : 0;
  const completeArc = completed * unit;
  const pendingArc = pending * unit;
  const failedArc = failed * unit;

  return (
    <div className={styles.donutWrap}>
      <svg
        className={styles.donut}
        viewBox="0 0 84 84"
        role="img"
        aria-label={`${total} sample inferences: ${completed} completed, ${pending} pending, ${failed} failed`}
      >
        <circle className={styles.track} cx="42" cy="42" r={radius} />
        <circle
          className={styles.complete}
          cx="42"
          cy="42"
          r={radius}
          strokeDasharray={`${completeArc} ${circumference - completeArc}`}
        />
        <circle
          className={styles.pending}
          cx="42"
          cy="42"
          r={radius}
          strokeDasharray={`${pendingArc} ${circumference - pendingArc}`}
          strokeDashoffset={-completeArc}
        />
        <circle
          className={styles.failed}
          cx="42"
          cy="42"
          r={radius}
          strokeDasharray={`${failedArc} ${circumference - failedArc}`}
          strokeDashoffset={-(completeArc + pendingArc)}
        />
      </svg>
      <div className={styles.donutValue}>
        <strong>{total.toLocaleString()}</strong>
        <span>Total</span>
      </div>
    </div>
  );
}
