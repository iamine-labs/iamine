import { LoaderCircle } from 'lucide-react';
import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from 'react';

import styles from './Button.module.css';

type ButtonVariant = 'primary' | 'secondary' | 'danger' | 'quiet';
type ButtonSize = 'small' | 'medium';

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  leadingIcon?: ReactNode;
  loading?: boolean;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  function Button(
    {
      children,
      className = '',
      disabled,
      leadingIcon,
      loading = false,
      size = 'medium',
      type = 'button',
      variant = 'secondary',
      ...props
    },
    ref,
  ) {
    const classes = [styles.button, styles[variant], styles[size], className]
      .filter(Boolean)
      .join(' ');

    return (
      <button
        ref={ref}
        className={classes}
        type={type}
        disabled={disabled || loading}
        aria-busy={loading || undefined}
        {...props}
      >
        <span className={styles.icon} aria-hidden="true">
          {loading ? (
            <LoaderCircle className={styles.spinner} size={16} />
          ) : (
            leadingIcon
          )}
        </span>
        <span className={styles.label}>{children}</span>
      </button>
    );
  },
);
