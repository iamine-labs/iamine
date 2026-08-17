export type DashboardRouteId =
  'overview' | 'agents' | 'nodes' | 'models' | 'activity' | 'marketplace';

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
  { id: 'agents', label: 'Agents', path: '/agents', availability: 'reserved' },
  { id: 'nodes', label: 'Nodes', path: '/nodes', availability: 'reserved' },
  { id: 'models', label: 'Models', path: '/models', availability: 'reserved' },
  {
    id: 'activity',
    label: 'Activity',
    path: '/activity',
    availability: 'reserved',
  },
  {
    id: 'marketplace',
    label: 'Marketplace',
    path: '/marketplace',
    availability: 'reserved',
  },
];

export const overviewRoute = dashboardRoutes[0];

export function getDashboardRoute(id: DashboardRouteId): DashboardRoute {
  const route = dashboardRoutes.find((candidate) => candidate.id === id);

  if (!route) throw new Error(`Unknown dashboard route: ${id}`);
  return route;
}
