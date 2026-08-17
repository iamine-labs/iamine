import axe from 'axe-core';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import type {
  OverviewDataSource,
  OverviewViewModel,
} from '../../contracts/view-models/overview';
import { overviewMockViewModel } from '../../mocks/overviewFixtures';
import { OverviewPage } from './OverviewPage';

function createDataSource(
  load: () => Promise<OverviewViewModel | null>,
): OverviewDataSource {
  return { kind: 'mock', load };
}

describe('OverviewPage', () => {
  it('renders loading and then the deterministic mock view model', async () => {
    let resolveLoad: (value: OverviewViewModel) => void = () => undefined;
    const load = vi.fn(
      () =>
        new Promise<OverviewViewModel>((resolve) => {
          resolveLoad = resolve;
        }),
    );

    render(
      <OverviewPage
        dataSource={createDataSource(load)}
        onOpenNodes={vi.fn()}
      />,
    );

    expect(
      screen.getByRole('heading', { name: 'Loading overview preview' }),
    ).toBeVisible();

    act(() => resolveLoad(overviewMockViewModel));

    expect(
      await screen.findByRole('heading', { name: 'System operational' }),
    ).toBeVisible();
    expect(
      screen.getByText('Mock Overview; not authoritative'),
    ).toBeInTheDocument();
    expect(load).toHaveBeenCalledOnce();
  });

  it('renders a bounded empty state', async () => {
    const dataSource = createDataSource(() => Promise.resolve(null));

    render(<OverviewPage dataSource={dataSource} onOpenNodes={vi.fn()} />);

    expect(
      await screen.findByRole('heading', { name: 'No overview preview data' }),
    ).toBeVisible();
  });

  it('does not expose rejected source details in the error state', async () => {
    const dataSource = createDataSource(() =>
      Promise.reject(new Error('private transport detail')),
    );

    render(<OverviewPage dataSource={dataSource} onOpenNodes={vi.fn()} />);

    expect(
      await screen.findByRole('heading', {
        name: 'Overview preview unavailable',
      }),
    ).toBeVisible();
    expect(
      screen.queryByText(/private transport detail/i),
    ).not.toBeInTheDocument();
  });

  it('retries a failed source without rebuilding the shell', async () => {
    const load = vi
      .fn<OverviewDataSource['load']>()
      .mockRejectedValueOnce(new Error('first attempt'))
      .mockResolvedValueOnce(overviewMockViewModel);

    render(
      <OverviewPage
        dataSource={createDataSource(load)}
        onOpenNodes={vi.fn()}
      />,
    );

    fireEvent.click(
      await screen.findByRole('button', { name: 'Retry preview' }),
    );

    expect(
      await screen.findByRole('heading', { name: 'System operational' }),
    ).toBeVisible();
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('keeps mock-only actions disabled and exposes the approved navigation', async () => {
    const onOpenNodes = vi.fn();

    render(<OverviewPage onOpenNodes={onOpenNodes} />);
    await screen.findByRole('heading', { name: 'System operational' });

    expect(screen.getByRole('button', { name: 'Manage' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'View details' })).toBeDisabled();
    expect(
      screen.getByRole('button', { name: 'Resource details preview' }),
    ).toBeDisabled();
    expect(screen.getByRole('button', { name: 'View history' })).toBeDisabled();
    for (const button of screen.getAllByRole('button', { name: 'View all' })) {
      expect(button).toBeDisabled();
    }
    expect(
      screen.getByRole('button', { name: 'System log preview options' }),
    ).toBeDisabled();

    fireEvent.click(screen.getByRole('button', { name: 'View global' }));
    expect(onOpenNodes).toHaveBeenCalledOnce();
  });

  it('has no detectable structural accessibility violations', async () => {
    const { container } = render(<OverviewPage onOpenNodes={vi.fn()} />);
    await screen.findByRole('heading', { name: 'System operational' });

    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });

    expect(results.violations).toEqual([]);
  });
});
