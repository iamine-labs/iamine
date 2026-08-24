export type DashboardRouteId =
  | 'overview'
  | 'agents'
  | 'nodes'
  | 'models'
  | 'activity'
  | 'diagnostics'
  | 'marketplace';

export interface DashboardRoute {
  id: DashboardRouteId;
  label: string;
  path: `/${string}`;
  availability: 'preview' | 'reserved';
}

export const dashboardRoutes: readonly DashboardRoute[] = [
  {
    id: 'overview',
    label: 'Overview',
    path: '/overview',
    availability: 'preview',
  },
  { id: 'agents', label: 'Agents', path: '/agents', availability: 'preview' },
  { id: 'nodes', label: 'Nodes', path: '/nodes', availability: 'preview' },
  { id: 'models', label: 'Models', path: '/models', availability: 'reserved' },
  {
    id: 'activity',
    label: 'Activity',
    path: '/activity',
    availability: 'reserved',
  },
  {
    id: 'diagnostics',
    label: 'Diagnostics',
    path: '/diagnostics',
    availability: 'preview',
  },
  {
    id: 'marketplace',
    label: 'Marketplace',
    path: '/marketplace',
    availability: 'reserved',
  },
];

export const overviewRoute = dashboardRoutes[0];
export const agentPermissionRoutePattern = '/agents/:agentId/permissions';

export function getDashboardRoute(id: DashboardRouteId): DashboardRoute {
  const route = dashboardRoutes.find((candidate) => candidate.id === id);

  if (!route) throw new Error(`Unknown dashboard route: ${id}`);
  return route;
}

export function getAgentPermissionRoute(agentId: string): string {
  return `/agents/${encodeURIComponent(agentId)}/permissions`;
}
