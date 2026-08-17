import styles from './Toggle.module.css';

interface ToggleProps {
  checked: boolean;
  label: string;
  description?: string;
  onChange: (checked: boolean) => void;
}

export function Toggle({ checked, description, label, onChange }: ToggleProps) {
  return (
    <div className={styles.row}>
      <span className={styles.copy}>
        <span className={styles.label}>{label}</span>
        {description && (
          <span className={styles.description}>{description}</span>
        )}
      </span>
      <button
        className={styles.control}
        type="button"
        role="switch"
        aria-checked={checked}
        aria-label={label}
        onClick={() => onChange(!checked)}
      >
        <span className={styles.thumb} />
      </button>
    </div>
  );
}
