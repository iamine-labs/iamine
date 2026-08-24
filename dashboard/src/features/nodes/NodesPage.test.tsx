import axe from 'axe-core';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import type {
  NodesDataSource,
  NodesViewModel,
} from '../../contracts/view-models/nodes';
import { nodesMockViewModel } from '../../mocks/nodesFixtures';
import { NodesPage } from './NodesPage';

function createDataSource(
  load: () => Promise<NodesViewModel | null>,
): NodesDataSource {
  return { kind: 'mock', load };
}

describe('NodesPage', () => {
  it('renders loading and then the deterministic nodes preview', async () => {
    let resolveLoad: (value: NodesViewModel) => void = () => undefined;
    const load = vi.fn(
      () =>
        new Promise<NodesViewModel>((resolve) => {
          resolveLoad = resolve;
        }),
    );

    render(<NodesPage dataSource={createDataSource(load)} />);

    expect(
      screen.getByRole('heading', { name: 'Loading nodes preview' }),
    ).toBeVisible();

    act(() => resolveLoad(nodesMockViewModel));

    expect(await screen.findByRole('heading', { name: 'Nodes' })).toBeVisible();
    expect(
      screen.getByText('Preview fixture; no node discovery was performed'),
    ).toBeVisible();
    expect(screen.getByText('5 shown')).toBeVisible();
    expect(load).toHaveBeenCalledOnce();
  });

  it('renders a bounded empty state', async () => {
    render(
      <NodesPage dataSource={createDataSource(() => Promise.resolve(null))} />,
    );

    expect(
      await screen.findByRole('heading', { name: 'No nodes preview data' }),
    ).toBeVisible();
  });

  it('hides source failures and retries the mock load', async () => {
    const load = vi
      .fn<NodesDataSource['load']>()
      .mockRejectedValueOnce(new Error('private network inventory'))
      .mockResolvedValueOnce(nodesMockViewModel);

    render(<NodesPage dataSource={createDataSource(load)} />);

    expect(
      await screen.findByRole('heading', {
        name: 'Nodes preview unavailable',
      }),
    ).toBeVisible();
    expect(
      screen.queryByText(/private network inventory/i),
    ).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Retry preview' }));

    expect(await screen.findByRole('heading', { name: 'Nodes' })).toBeVisible();
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('filters, resets, and selects bounded presentation metadata', async () => {
    render(<NodesPage />);
    await screen.findByRole('heading', { name: 'Nodes' });

    fireEvent.change(screen.getByRole('searchbox', { name: 'Search nodes' }), {
      target: { value: 'Preview Node B' },
    });
    expect(screen.getByText('1 of 5')).toBeVisible();
    expect(
      screen.getByRole('heading', { name: 'Preview Node B' }),
    ).toBeVisible();

    fireEvent.click(
      screen.getByRole('button', { name: 'Clear nodes filters' }),
    );
    fireEvent.click(screen.getByRole('button', { name: 'Limited' }));
    expect(screen.getByText('1 of 5')).toBeVisible();
    expect(
      screen.getByRole('heading', { name: 'Preview Node C' }),
    ).toBeVisible();

    fireEvent.click(
      screen.getByRole('button', { name: 'Clear nodes filters' }),
    );
    fireEvent.change(
      screen.getByRole('combobox', {
        name: 'Filter nodes by capability label',
      }),
      { target: { value: 'acceleration' } },
    );
    expect(screen.getByText('2 of 5')).toBeVisible();
    fireEvent.click(
      screen.getByRole('button', { name: 'Select Preview Node E' }),
    );
    expect(
      screen.getByRole('heading', { name: 'Preview Node E' }),
    ).toBeVisible();
  });

  it('renders no matches without real node actions', async () => {
    render(<NodesPage />);
    await screen.findByRole('heading', { name: 'Nodes' });

    fireEvent.change(screen.getByRole('searchbox', { name: 'Search nodes' }), {
      target: { value: 'real production host' },
    });

    expect(
      screen.getByRole('heading', { name: 'No matching nodes' }),
    ).toBeVisible();
    expect(
      screen.queryByRole('button', { name: /discover/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /connect/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /configure/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /start/i }),
    ).not.toBeInTheDocument();
  });

  it('has no detectable structural accessibility violations', async () => {
    const { container } = render(<NodesPage />);
    await screen.findByRole('heading', { name: 'Nodes' });

    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });

    expect(results.violations).toEqual([]);
  });
});
