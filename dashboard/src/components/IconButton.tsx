import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from 'react';

import styles from './IconButton.module.css';

interface IconButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  label: string;
  icon: ReactNode;
  size?: 'small' | 'medium';
}

export const IconButton = forwardRef<HTMLButtonElement, IconButtonProps>(
  function IconButton(
    { className = '', icon, label, size = 'medium', type = 'button', ...props },
    ref,
  ) {
    return (
      <button
        ref={ref}
        className={`${styles.button} ${styles[size]} ${className}`}
        type={type}
        aria-label={label}
        title={label}
        {...props}
      >
        <span aria-hidden="true">{icon}</span>
      </button>
    );
  },
);
