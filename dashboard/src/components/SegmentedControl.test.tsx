import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { SegmentedControl } from './SegmentedControl';

describe('SegmentedControl', () => {
  it('announces and changes the selected mode', () => {
    const onChange = vi.fn();
    render(
      <SegmentedControl
        label="Density"
        options={[
          { value: 'compact', label: 'Compact' },
          { value: 'comfortable', label: 'Comfortable' },
        ]}
        value="compact"
        onChange={onChange}
      />,
    );

    expect(screen.getByRole('button', { name: 'Compact' })).toHaveAttribute(
      'aria-pressed',
      'true',
    );
    fireEvent.click(screen.getByRole('button', { name: 'Comfortable' }));
    expect(onChange).toHaveBeenCalledWith('comfortable');
  });
});
