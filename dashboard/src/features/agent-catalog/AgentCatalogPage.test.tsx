import axe from 'axe-core';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import type {
  AgentCatalogDataSource,
  AgentCatalogViewModel,
} from '../../contracts/view-models/agentCatalog';
import { agentCatalogMockViewModel } from '../../mocks/agentCatalogFixtures';
import { AgentCatalogPage } from './AgentCatalogPage';

function createDataSource(
  load: () => Promise<AgentCatalogViewModel | null>,
): AgentCatalogDataSource {
  return { kind: 'mock', load };
}

describe('AgentCatalogPage', () => {
  it('renders loading and then the deterministic catalog preview', async () => {
    let resolveLoad: (value: AgentCatalogViewModel) => void = () => undefined;
    const load = vi.fn(
      () =>
        new Promise<AgentCatalogViewModel>((resolve) => {
          resolveLoad = resolve;
        }),
    );

    render(<AgentCatalogPage dataSource={createDataSource(load)} />);

    expect(
      screen.getByRole('heading', {
        name: 'Loading agent catalog preview',
      }),
    ).toBeVisible();

    act(() => resolveLoad(agentCatalogMockViewModel));

    expect(
      await screen.findByRole('heading', { name: 'Agent catalog' }),
    ).toBeVisible();
    expect(
      screen.getByText('Preview catalog; not local node state'),
    ).toBeVisible();
    expect(screen.getByText('6 shown')).toBeVisible();
    expect(load).toHaveBeenCalledOnce();
  });

  it('renders a bounded empty state', async () => {
    render(
      <AgentCatalogPage
        dataSource={createDataSource(() => Promise.resolve(null))}
      />,
    );

    expect(
      await screen.findByRole('heading', {
        name: 'No agent catalog preview data',
      }),
    ).toBeVisible();
  });

  it('hides source failures and retries the mock load', async () => {
    const load = vi
      .fn<AgentCatalogDataSource['load']>()
      .mockRejectedValueOnce(new Error('private registry detail'))
      .mockResolvedValueOnce(agentCatalogMockViewModel);

    render(<AgentCatalogPage dataSource={createDataSource(load)} />);

    expect(
      await screen.findByRole('heading', {
        name: 'Agent catalog preview unavailable',
      }),
    ).toBeVisible();
    expect(
      screen.queryByText(/private registry detail/i),
    ).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Retry preview' }));

    expect(
      await screen.findByRole('heading', { name: 'Agent catalog' }),
    ).toBeVisible();
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('filters, resets, and selects bounded presentation metadata', async () => {
    render(<AgentCatalogPage />);
    await screen.findByRole('heading', { name: 'Agent catalog' });

    fireEvent.change(screen.getByRole('searchbox', { name: 'Search agents' }), {
      target: { value: 'Windows Optimizer' },
    });
    expect(screen.getByText('1 of 6')).toBeVisible();
    expect(
      screen.getByRole('heading', { name: 'Windows Optimizer Assistant' }),
    ).toBeVisible();
    expect(screen.queryByText('Node Doctor')).not.toBeInTheDocument();

    fireEvent.click(
      screen.getByRole('button', { name: 'Clear agent catalog filters' }),
    );
    fireEvent.click(screen.getByRole('button', { name: 'Planned' }));
    expect(screen.getByText('4 of 6')).toBeVisible();

    fireEvent.click(
      screen.getByRole('button', {
        name: 'Select Home Network Assistant preview',
      }),
    );
    expect(
      screen.getByRole('heading', { name: 'Home Network Assistant' }),
    ).toBeVisible();
    expect(screen.getByText('No network discovery')).toBeVisible();
  });

  it('renders a controlled no-match state without real agent actions', async () => {
    render(<AgentCatalogPage />);
    await screen.findByRole('heading', { name: 'Agent catalog' });

    fireEvent.change(screen.getByRole('searchbox', { name: 'Search agents' }), {
      target: { value: 'printer deployment' },
    });

    expect(
      screen.getByRole('heading', { name: 'No matching agents' }),
    ).toBeVisible();
    expect(
      screen.queryByRole('button', { name: /install/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /execute/i }),
    ).not.toBeInTheDocument();
  });

  it('opens the permission preview for the exact selected agent', async () => {
    const onReviewPermissions = vi.fn();
    render(<AgentCatalogPage onReviewPermissions={onReviewPermissions} />);
    await screen.findByRole('heading', { name: 'Agent catalog' });

    fireEvent.click(
      screen.getByRole('button', {
        name: 'Select Home Network Assistant preview',
      }),
    );
    fireEvent.click(
      screen.getByRole('button', { name: 'Review permission preview' }),
    );

    expect(onReviewPermissions).toHaveBeenCalledOnce();
    expect(onReviewPermissions).toHaveBeenCalledWith('home-network-assistant');
  });

  it('has no detectable structural accessibility violations', async () => {
    const { container } = render(<AgentCatalogPage />);
    await screen.findByRole('heading', { name: 'Agent catalog' });

    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });

    expect(results.violations).toEqual([]);
  });
});
