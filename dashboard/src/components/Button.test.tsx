import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { Button } from './Button';

describe('Button', () => {
  it('forwards a command and preserves its accessible name', () => {
    const onClick = vi.fn();
    render(<Button onClick={onClick}>Confirm preview</Button>);

    fireEvent.click(screen.getByRole('button', { name: 'Confirm preview' }));

    expect(onClick).toHaveBeenCalledOnce();
  });

  it('blocks interaction while loading', () => {
    render(<Button loading>Refresh sample</Button>);

    const button = screen.getByRole('button', { name: 'Refresh sample' });
    expect(button).toBeDisabled();
    expect(button).toHaveAttribute('aria-busy', 'true');
  });
});
