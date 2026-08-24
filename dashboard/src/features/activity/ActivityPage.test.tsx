import axe from 'axe-core';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import type {
  ActivityDataSource,
  ActivityViewModel,
} from '../../contracts/view-models/activity';
import { activityMockViewModel } from '../../mocks/activityFixtures';
import { ActivityPage } from './ActivityPage';

function createDataSource(
  load: () => Promise<ActivityViewModel | null>,
): ActivityDataSource {
  return { kind: 'mock', load };
}

describe('ActivityPage', () => {
  it('renders loading and then the deterministic activity preview', async () => {
    let resolveLoad: (value: ActivityViewModel) => void = () => undefined;
    const load = vi.fn(
      () =>
        new Promise<ActivityViewModel>((resolve) => {
          resolveLoad = resolve;
        }),
    );

    render(<ActivityPage dataSource={createDataSource(load)} />);

    expect(
      screen.getByRole('heading', { name: 'Loading activity preview' }),
    ).toBeVisible();

    act(() => resolveLoad(activityMockViewModel));

    expect(
      await screen.findByRole('heading', { name: 'Activity' }),
    ).toBeVisible();
    expect(
      screen.getByText('Preview fixture; no event source was read'),
    ).toBeVisible();
    expect(screen.getByText('6 shown')).toBeVisible();
    expect(load).toHaveBeenCalledOnce();
  });

  it('renders a bounded empty state', async () => {
    render(
      <ActivityPage
        dataSource={createDataSource(() => Promise.resolve(null))}
      />,
    );

    expect(
      await screen.findByRole('heading', { name: 'No activity preview data' }),
    ).toBeVisible();
  });

  it('hides source failures and retries the mock load', async () => {
    const load = vi
      .fn<ActivityDataSource['load']>()
      .mockRejectedValueOnce(new Error('private audit record'))
      .mockResolvedValueOnce(activityMockViewModel);

    render(<ActivityPage dataSource={createDataSource(load)} />);

    expect(
      await screen.findByRole('heading', {
        name: 'Activity preview unavailable',
      }),
    ).toBeVisible();
    expect(screen.queryByText(/private audit record/i)).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Retry preview' }));

    expect(
      await screen.findByRole('heading', { name: 'Activity' }),
    ).toBeVisible();
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('filters, resets, and selects bounded presentation metadata', async () => {
    render(<ActivityPage />);
    await screen.findByRole('heading', { name: 'Activity' });

    fireEvent.change(
      screen.getByRole('searchbox', { name: 'Search activity' }),
      { target: { value: 'Preview Event B' } },
    );
    expect(screen.getByText('1 of 6')).toBeVisible();
    expect(
      screen.getByRole('heading', { name: 'Preview Event B' }),
    ).toBeVisible();

    fireEvent.click(
      screen.getByRole('button', { name: 'Clear activity filters' }),
    );
    fireEvent.click(screen.getByRole('button', { name: 'Attention' }));
    expect(screen.getByText('2 of 6')).toBeVisible();
    expect(
      screen.getByRole('heading', { name: 'Preview Event C' }),
    ).toBeVisible();

    fireEvent.click(
      screen.getByRole('button', { name: 'Clear activity filters' }),
    );
    fireEvent.change(
      screen.getByRole('combobox', {
        name: 'Filter activity by category label',
      }),
      { target: { value: 'agent' } },
    );
    expect(screen.getByText('2 of 6')).toBeVisible();
    fireEvent.click(
      screen.getByRole('button', { name: 'Select Preview Event E' }),
    );
    expect(
      screen.getByRole('heading', { name: 'Preview Event E' }),
    ).toBeVisible();
  });

  it('renders no matches without real event actions', async () => {
    render(<ActivityPage />);
    await screen.findByRole('heading', { name: 'Activity' });

    fireEvent.change(
      screen.getByRole('searchbox', { name: 'Search activity' }),
      { target: { value: 'real production trace' } },
    );

    expect(
      screen.getByRole('heading', { name: 'No matching activity' }),
    ).toBeVisible();
    for (const action of [
      'acknowledge',
      'approve',
      'deny',
      'retry task',
      'replay',
      'export',
      'delete',
      'open log',
    ]) {
      expect(
        screen.queryByRole('button', { name: new RegExp(action, 'i') }),
      ).not.toBeInTheDocument();
    }
  });

  it('has no detectable structural accessibility violations', async () => {
    const { container } = render(<ActivityPage />);
    await screen.findByRole('heading', { name: 'Activity' });

    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });

    expect(results.violations).toEqual([]);
  });
});
