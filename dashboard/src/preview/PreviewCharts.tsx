import styles from './PreviewCharts.module.css';

interface SparklineProps {
  ariaLabel: string;
  color: 'blue' | 'copper' | 'green' | 'red';
  points: readonly number[];
}

export function Sparkline({ ariaLabel, color, points }: SparklineProps) {
  const maximum = Math.max(...points);
  const minimum = Math.min(...points);
  const range = Math.max(maximum - minimum, 1);
  const coordinates = points
    .map((point, index) => {
      const x = (index / Math.max(points.length - 1, 1)) * 100;
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
}

export function DonutChart({ total }: DonutChartProps) {
  const radius = 34;
  const circumference = 2 * Math.PI * radius;
  const complete = circumference * 0.72;
  const pending = circumference * 0.16;
  const failed = circumference * 0.04;

  return (
    <div className={styles.donutWrap}>
      <svg
        className={styles.donut}
        viewBox="0 0 84 84"
        role="img"
        aria-label={`${total} sample inferences: 72 percent completed, 16 percent pending, 4 percent failed`}
      >
        <circle className={styles.track} cx="42" cy="42" r={radius} />
        <circle
          className={styles.complete}
          cx="42"
          cy="42"
          r={radius}
          strokeDasharray={`${complete} ${circumference - complete}`}
        />
        <circle
          className={styles.pending}
          cx="42"
          cy="42"
          r={radius}
          strokeDasharray={`${pending} ${circumference - pending}`}
          strokeDashoffset={-complete}
        />
        <circle
          className={styles.failed}
          cx="42"
          cy="42"
          r={radius}
          strokeDasharray={`${failed} ${circumference - failed}`}
          strokeDashoffset={-(complete + pending)}
        />
      </svg>
      <div className={styles.donutValue}>
        <strong>{total.toLocaleString()}</strong>
        <span>Total</span>
      </div>
    </div>
  );
}
