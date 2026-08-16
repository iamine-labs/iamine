import { Network } from 'lucide-react';

import styles from './BrandMark.module.css';

interface BrandMarkProps {
  compact?: boolean;
}

export function BrandMark({ compact = false }: BrandMarkProps) {
  return (
    <div className={styles.mark} aria-label="IAMINE">
      <span className={styles.symbol} aria-hidden="true">
        <Network size={20} strokeWidth={2} />
      </span>
      {!compact && <span className={styles.name}>IAMINE</span>}
    </div>
  );
}
