import { forwardRef, type InputHTMLAttributes } from 'react';

import styles from './TextField.module.css';

interface TextFieldProps extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  description?: string;
  error?: string;
}

export const TextField = forwardRef<HTMLInputElement, TextFieldProps>(
  function TextField(
    { className = '', description, error, id, label, ...props },
    ref,
  ) {
    const fieldId = id ?? `field-${label.toLowerCase().replace(/\s+/g, '-')}`;
    const messageId = `${fieldId}-message`;

    return (
      <div className={styles.field}>
        <label className={styles.label} htmlFor={fieldId}>
          {label}
        </label>
        <input
          ref={ref}
          id={fieldId}
          className={`${styles.input} ${error ? styles.invalid : ''} ${className}`}
          aria-invalid={Boolean(error)}
          aria-describedby={description || error ? messageId : undefined}
          {...props}
        />
        {(description || error) && (
          <span
            id={messageId}
            className={error ? styles.error : styles.description}
          >
            {error ?? description}
          </span>
        )}
      </div>
    );
  },
);
