import axe from 'axe-core';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import type {
  DiagnosticsDataSource,
  DiagnosticsViewModel,
} from '../../contracts/view-models/diagnostics';
import { diagnosticsMockViewModel } from '../../mocks/diagnosticsFixtures';
import { DiagnosticsPage } from './DiagnosticsPage';

function createDataSource(
  load: () => Promise<DiagnosticsViewModel | null>,
): DiagnosticsDataSource {
  return { kind: 'mock', load };
}

describe('DiagnosticsPage', () => {
  it('renders loading and then the deterministic diagnostics preview', async () => {
    let resolveLoad: (value: DiagnosticsViewModel) => void = () => undefined;
    const load = vi.fn(
      () =>
        new Promise<DiagnosticsViewModel>((resolve) => {
          resolveLoad = resolve;
        }),
    );

    render(<DiagnosticsPage dataSource={createDataSource(load)} />);

    expect(
      screen.getByRole('heading', { name: 'Loading diagnostics preview' }),
    ).toBeVisible();

    act(() => resolveLoad(diagnosticsMockViewModel));

    expect(
      await screen.findByRole('heading', { name: 'Diagnostics' }),
    ).toBeVisible();
    expect(
      screen.getByText('Preview fixture; no device was inspected'),
    ).toBeVisible();
    expect(screen.getByText('6 shown')).toBeVisible();
    expect(load).toHaveBeenCalledOnce();
  });

  it('renders a bounded empty state', async () => {
    render(
      <DiagnosticsPage
        dataSource={createDataSource(() => Promise.resolve(null))}
      />,
    );

    expect(
      await screen.findByRole('heading', {
        name: 'No diagnostics preview data',
      }),
    ).toBeVisible();
  });

  it('hides source failures and retries the mock load', async () => {
    const load = vi
      .fn<DiagnosticsDataSource['load']>()
      .mockRejectedValueOnce(new Error('private device evidence'))
      .mockResolvedValueOnce(diagnosticsMockViewModel);

    render(<DiagnosticsPage dataSource={createDataSource(load)} />);

    expect(
      await screen.findByRole('heading', {
        name: 'Diagnostics preview unavailable',
      }),
    ).toBeVisible();
    expect(
      screen.queryByText(/private device evidence/i),
    ).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Retry preview' }));

    expect(
      await screen.findByRole('heading', { name: 'Diagnostics' }),
    ).toBeVisible();
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('filters, resets, and selects bounded presentation metadata', async () => {
    render(<DiagnosticsPage />);
    await screen.findByRole('heading', { name: 'Diagnostics' });

    fireEvent.change(screen.getByRole('searchbox', { name: 'Search checks' }), {
      target: { value: 'Local Control API' },
    });
    expect(screen.getByText('1 of 6')).toBeVisible();
    expect(
      screen.getByRole('heading', { name: 'Local Control API' }),
    ).toBeVisible();
    expect(screen.queryByText('Runtime policy')).not.toBeInTheDocument();

    fireEvent.click(
      screen.getByRole('button', { name: 'Clear diagnostics filters' }),
    );
    fireEvent.click(screen.getByRole('button', { name: 'Attention' }));
    expect(screen.getByText('2 of 6')).toBeVisible();

    fireEvent.click(
      screen.getByRole('button', { name: 'Select Model readiness preview' }),
    );
    expect(
      screen.getByRole('heading', { name: 'Model readiness' }),
    ).toBeVisible();
    expect(screen.getByText('models.mock.unobserved')).toBeVisible();
  });

  it('renders no matches without real diagnostic actions', async () => {
    render(<DiagnosticsPage />);
    await screen.findByRole('heading', { name: 'Diagnostics' });

    fireEvent.change(screen.getByRole('searchbox', { name: 'Search checks' }), {
      target: { value: 'physical sensor calibration' },
    });

    expect(
      screen.getByRole('heading', { name: 'No matching checks' }),
    ).toBeVisible();
    expect(
      screen.queryByRole('button', { name: /run diagnostics/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /export/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /repair/i }),
    ).not.toBeInTheDocument();
  });

  it('has no detectable structural accessibility violations', async () => {
    const { container } = render(<DiagnosticsPage />);
    await screen.findByRole('heading', { name: 'Diagnostics' });

    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });

    expect(results.violations).toEqual([]);
  });
});
