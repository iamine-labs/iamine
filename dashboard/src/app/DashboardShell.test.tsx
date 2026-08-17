import axe from 'axe-core';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { describe, expect, it } from 'vitest';

import { DashboardShell } from './DashboardShell';

function renderShell(route = '/overview') {
  return render(
    <MemoryRouter initialEntries={[route]}>
      <DashboardShell />
    </MemoryRouter>,
  );
}

describe('DashboardShell', () => {
  it('keeps mock provenance visible and navigates through stable routes', async () => {
    renderShell();

    expect(screen.getByText('Preview data')).toBeVisible();
    expect(
      await screen.findByRole('heading', { name: 'System operational' }),
    ).toBeVisible();
    expect(screen.getAllByText('NODE-LOCAL-01')[0]).toBeVisible();

    fireEvent.click(screen.getByRole('link', { name: 'Agents' }));

    expect(screen.getByRole('heading', { name: 'Agents' })).toBeVisible();
    expect(screen.getByText('Preview boundary')).toBeVisible();

    fireEvent.click(screen.getByRole('button', { name: 'Return to Overview' }));
    expect(
      await screen.findByRole('heading', { name: 'System operational' }),
    ).toBeVisible();
  });

  it('renders a bounded fallback for unknown routes', () => {
    renderShell('/outside-approved-shell');

    expect(
      screen.getByRole('heading', { name: 'Page not found' }),
    ).toBeVisible();
    expect(screen.getByText('Unknown route')).toBeVisible();
  });

  it('marks unavailable shell actions as disabled', () => {
    renderShell();

    expect(
      screen.getByRole('button', { name: 'Search unavailable in preview' }),
    ).toBeDisabled();
    expect(screen.getByText('Core connection:')).toBeVisible();
  });

  it('has no detectable structural accessibility violations', async () => {
    const { container } = renderShell();
    await screen.findByRole('heading', { name: 'System operational' });
    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });

    expect(results.violations).toEqual([]);
  });
});
