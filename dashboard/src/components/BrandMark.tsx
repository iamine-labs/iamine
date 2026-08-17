import styles from './BrandMark.module.css';

interface BrandMarkProps {
  compact?: boolean;
}

export function BrandMark({ compact = false }: BrandMarkProps) {
  return (
    <div className={styles.mark} data-brand="iamine">
      <span className={styles.symbol} aria-hidden="true">
        <img src="/assets/iamine-mark.png" alt="" />
      </span>
      {!compact && <span className={styles.name}>IAMINE</span>}
    </div>
  );
}
