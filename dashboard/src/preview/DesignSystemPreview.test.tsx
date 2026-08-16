import axe from 'axe-core';
import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { DesignSystemPreview } from './DesignSystemPreview';

describe('DesignSystemPreview', () => {
  it('keeps mock provenance visible and filters sample records', () => {
    render(<DesignSystemPreview />);

    expect(screen.getByText('Preview data')).toBeVisible();
    expect(screen.getAllByRole('row')).toHaveLength(4);

    fireEvent.change(screen.getByRole('textbox', { name: 'Filter checks' }), {
      target: { value: 'model' },
    });

    expect(screen.getAllByRole('row')).toHaveLength(2);
    expect(screen.getByText('Model inventory')).toBeVisible();
  });

  it('has no detectable accessibility violations', async () => {
    const { container } = render(<DesignSystemPreview />);
    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });

    expect(results.violations).toEqual([]);
  });
});
