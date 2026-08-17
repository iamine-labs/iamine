import axe from 'axe-core';
import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { DesignSystemPreview } from './DesignSystemPreview';

describe('DesignSystemPreview', () => {
  it('keeps mock provenance visible and navigates locally', () => {
    render(<DesignSystemPreview />);

    expect(screen.getByText('Preview data')).toBeVisible();
    expect(
      screen.getByRole('heading', { name: 'System operational' }),
    ).toBeVisible();
    expect(screen.getAllByText('NODE-LOCAL-01')[0]).toBeVisible();

    fireEvent.click(screen.getByRole('button', { name: 'Agents' }));

    expect(screen.getByRole('heading', { name: 'Agents' })).toBeVisible();
    expect(screen.getByText('Preview boundary')).toBeVisible();

    fireEvent.click(screen.getByRole('button', { name: 'Return to Overview' }));
    expect(
      screen.getByRole('heading', { name: 'System operational' }),
    ).toBeVisible();
  });

  it('has no detectable structural accessibility violations', async () => {
    const { container } = render(<DesignSystemPreview />);
    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });

    expect(results.violations).toEqual([]);
  });
});
