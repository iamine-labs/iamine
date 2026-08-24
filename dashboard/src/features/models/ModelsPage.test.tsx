import axe from 'axe-core';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import type {
  ModelsDataSource,
  ModelsViewModel,
} from '../../contracts/view-models/models';
import { modelsMockViewModel } from '../../mocks/modelsFixtures';
import { ModelsPage } from './ModelsPage';

function createDataSource(
  load: () => Promise<ModelsViewModel | null>,
): ModelsDataSource {
  return { kind: 'mock', load };
}

describe('ModelsPage', () => {
  it('renders loading and then the deterministic models preview', async () => {
    let resolveLoad: (value: ModelsViewModel) => void = () => undefined;
    const load = vi.fn(
      () =>
        new Promise<ModelsViewModel>((resolve) => {
          resolveLoad = resolve;
        }),
    );

    render(<ModelsPage dataSource={createDataSource(load)} />);

    expect(
      screen.getByRole('heading', { name: 'Loading models preview' }),
    ).toBeVisible();

    act(() => resolveLoad(modelsMockViewModel));

    expect(
      await screen.findByRole('heading', { name: 'Models' }),
    ).toBeVisible();
    expect(
      screen.getByText('Preview fixture; no model registry was read'),
    ).toBeVisible();
    expect(screen.getByText('5 shown')).toBeVisible();
    expect(load).toHaveBeenCalledOnce();
  });

  it('renders a bounded empty state', async () => {
    render(
      <ModelsPage dataSource={createDataSource(() => Promise.resolve(null))} />,
    );

    expect(
      await screen.findByRole('heading', { name: 'No models preview data' }),
    ).toBeVisible();
  });

  it('hides source failures and retries the mock load', async () => {
    const load = vi
      .fn<ModelsDataSource['load']>()
      .mockRejectedValueOnce(new Error('private artifact path'))
      .mockResolvedValueOnce(modelsMockViewModel);

    render(<ModelsPage dataSource={createDataSource(load)} />);

    expect(
      await screen.findByRole('heading', {
        name: 'Models preview unavailable',
      }),
    ).toBeVisible();
    expect(
      screen.queryByText(/private artifact path/i),
    ).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Retry preview' }));

    expect(
      await screen.findByRole('heading', { name: 'Models' }),
    ).toBeVisible();
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('filters, resets, and selects bounded presentation metadata', async () => {
    render(<ModelsPage />);
    await screen.findByRole('heading', { name: 'Models' });

    fireEvent.change(screen.getByRole('searchbox', { name: 'Search models' }), {
      target: { value: 'Preview Model B' },
    });
    expect(screen.getByText('1 of 5')).toBeVisible();
    expect(
      screen.getByRole('heading', { name: 'Preview Model B' }),
    ).toBeVisible();

    fireEvent.click(
      screen.getByRole('button', { name: 'Clear models filters' }),
    );
    fireEvent.click(screen.getByRole('button', { name: 'Review' }));
    expect(screen.getByText('1 of 5')).toBeVisible();
    expect(
      screen.getByRole('heading', { name: 'Preview Model C' }),
    ).toBeVisible();

    fireEvent.click(
      screen.getByRole('button', { name: 'Clear models filters' }),
    );
    fireEvent.change(
      screen.getByRole('combobox', {
        name: 'Filter models by category label',
      }),
      { target: { value: 'general' } },
    );
    expect(screen.getByText('2 of 5')).toBeVisible();
    fireEvent.click(
      screen.getByRole('button', { name: 'Select Preview Model E' }),
    );
    expect(
      screen.getByRole('heading', { name: 'Preview Model E' }),
    ).toBeVisible();
  });

  it('renders no matches without real model actions', async () => {
    render(<ModelsPage />);
    await screen.findByRole('heading', { name: 'Models' });

    fireEvent.change(screen.getByRole('searchbox', { name: 'Search models' }), {
      target: { value: 'real production artifact' },
    });

    expect(
      screen.getByRole('heading', { name: 'No matching models' }),
    ).toBeVisible();
    for (const action of [
      'download',
      'install',
      'activate',
      'select model',
      'run model',
      'remove',
    ]) {
      expect(
        screen.queryByRole('button', { name: new RegExp(action, 'i') }),
      ).not.toBeInTheDocument();
    }
  });

  it('has no detectable structural accessibility violations', async () => {
    const { container } = render(<ModelsPage />);
    await screen.findByRole('heading', { name: 'Models' });

    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });

    expect(results.violations).toEqual([]);
  });
});
