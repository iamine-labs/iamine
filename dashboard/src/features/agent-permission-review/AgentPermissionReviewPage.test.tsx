import axe from 'axe-core';
import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import type {
  AgentPermissionReviewDataSource,
  AgentPermissionReviewViewModel,
} from '../../contracts/view-models/agentPermissionReview';
import { agentPermissionReviewFixtures } from '../../mocks/agentPermissionReviewFixtures';
import { AgentPermissionReviewPage } from './AgentPermissionReviewPage';

const nodeDoctorReview = agentPermissionReviewFixtures['node-doctor'];

function createDataSource(
  load: (agentId: string) => Promise<AgentPermissionReviewViewModel | null>,
): AgentPermissionReviewDataSource {
  return { kind: 'mock', load };
}

function renderReview(
  dataSource?: AgentPermissionReviewDataSource,
  agentId = 'node-doctor',
) {
  return render(
    <AgentPermissionReviewPage
      agentId={agentId}
      dataSource={dataSource}
      onBack={vi.fn()}
    />,
  );
}

describe('AgentPermissionReviewPage', () => {
  it('renders loading and then the exact local permission fixture', async () => {
    let resolveLoad: (value: AgentPermissionReviewViewModel) => void = () =>
      undefined;
    const load = vi.fn(
      () =>
        new Promise<AgentPermissionReviewViewModel>((resolve) => {
          resolveLoad = resolve;
        }),
    );

    renderReview(createDataSource(load));

    expect(
      screen.getByRole('heading', {
        name: 'Loading permission review preview',
      }),
    ).toBeVisible();

    act(() => resolveLoad(nodeDoctorReview));

    expect(
      await screen.findByRole('heading', { name: 'Permission review' }),
    ).toBeVisible();
    expect(screen.getByRole('heading', { name: 'Node Doctor' })).toBeVisible();
    expect(
      screen.getByText('Preview decision; no authorization issued'),
    ).toBeVisible();
    expect(screen.getByText('No network or worker startup')).toBeVisible();
    expect(load).toHaveBeenCalledWith('node-doctor');
  });

  it('renders a controlled empty state for an unknown agent', async () => {
    renderReview(undefined, 'unknown-agent');

    expect(
      await screen.findByRole('heading', {
        name: 'Permission preview unavailable for this agent',
      }),
    ).toBeVisible();
    expect(
      screen.getByRole('button', { name: 'Return to Agent catalog' }),
    ).toBeVisible();
  });

  it('hides local source failures and retries without leaking details', async () => {
    const load = vi
      .fn<AgentPermissionReviewDataSource['load']>()
      .mockRejectedValueOnce(new Error('private local fixture detail'))
      .mockResolvedValueOnce(nodeDoctorReview);

    renderReview(createDataSource(load));

    expect(
      await screen.findByRole('heading', {
        name: 'Permission review preview unavailable',
      }),
    ).toBeVisible();
    expect(
      screen.queryByText(/private local fixture detail/i),
    ).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Retry preview' }));

    expect(
      await screen.findByRole('heading', { name: 'Permission review' }),
    ).toBeVisible();
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('requires acknowledgement before recording a local confirmation preview', async () => {
    renderReview();
    await screen.findByRole('heading', { name: 'Permission review' });

    const confirm = screen.getByRole('button', { name: 'Confirm preview' });
    expect(confirm).toBeDisabled();

    fireEvent.click(
      screen.getByRole('checkbox', {
        name: /I reviewed this preview request/i,
      }),
    );
    expect(confirm).toBeEnabled();
    fireEvent.click(confirm);

    expect(
      screen.getByText('No permission or runtime authority was created.'),
    ).toBeVisible();
    expect(screen.getByText('Preview confirmation recorded')).toBeVisible();
    expect(
      screen.getByText('Not persisted · not emitted · no authority'),
    ).toBeVisible();
    const decisionPanel = screen
      .getByRole('heading', { name: 'Decision' })
      .closest('section');
    expect(decisionPanel).not.toBeNull();
    expect(within(decisionPanel!).getAllByText('None')).toHaveLength(3);
  });

  it('records and resets a denial preview without acknowledgement', async () => {
    renderReview();
    await screen.findByRole('heading', { name: 'Permission review' });

    fireEvent.click(screen.getByRole('button', { name: 'Deny preview' }));

    expect(screen.getByText('Preview denial recorded')).toBeVisible();
    expect(
      screen.getByText('No permission or runtime authority was created.'),
    ).toBeVisible();

    fireEvent.click(screen.getByRole('button', { name: 'Reset preview' }));

    expect(screen.getByText('Pending review')).toBeVisible();
    expect(
      screen.getByRole('button', { name: 'Confirm preview' }),
    ).toBeDisabled();
    expect(
      screen.queryByText('Preview denial recorded'),
    ).not.toBeInTheDocument();
  });

  it('has no detectable structural accessibility violations', async () => {
    const { container } = renderReview();
    await screen.findByRole('heading', { name: 'Permission review' });

    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });

    expect(results.violations).toEqual([]);
  });
});
