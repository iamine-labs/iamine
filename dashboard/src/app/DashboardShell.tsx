import { ArrowLeft, Construction, MapPinOff } from 'lucide-react';
import { Suspense, useCallback, useState } from 'react';
import {
  Navigate,
  Route,
  Routes,
  useLocation,
  useNavigate,
  useParams,
} from 'react-router';

import { Button, StatePanel, StatusBadge } from '../components';
import { AgentCatalogPage } from '../features/agent-catalog/AgentCatalogPage';
import { AgentPermissionReviewPage } from '../features/agent-permission-review/AgentPermissionReviewPage';
import { DiagnosticsPage } from '../features/diagnostics/DiagnosticsPage';
import { NodesPage } from '../features/nodes/NodesPage';
import { OverviewPage } from '../features/overview/OverviewPage';
import { DashboardChrome } from './DashboardChrome';
import { DashboardStatusBar } from './DashboardStatusBar';
import {
  agentPermissionRoutePattern,
  dashboardRoutes,
  getAgentPermissionRoute,
  getDashboardRoute,
  overviewRoute,
  type DashboardRoute,
} from './routes';
import styles from './DashboardShell.module.css';

function OverviewRoute() {
  const navigate = useNavigate();

  return (
    <OverviewPage
      onOpenNodes={() => void navigate(getDashboardRoute('nodes').path)}
    />
  );
}

function AgentsRoute() {
  const navigate = useNavigate();

  return (
    <AgentCatalogPage
      onReviewPermissions={(agentId) =>
        void navigate(getAgentPermissionRoute(agentId))
      }
    />
  );
}

function AgentPermissionRoute() {
  const navigate = useNavigate();
  const { agentId = '' } = useParams();

  return (
    <AgentPermissionReviewPage
      key={agentId}
      agentId={agentId}
      onBack={() => void navigate(getDashboardRoute('agents').path)}
    />
  );
}

function ReservedRoute({ route }: { route: DashboardRoute }) {
  const navigate = useNavigate();

  return (
    <section className={styles.placeholder}>
      <span className={styles.placeholderIcon} aria-hidden="true">
        <Construction size={34} />
      </span>
      <StatusBadge tone="info">Preview boundary</StatusBadge>
      <h2>{route.label}</h2>
      <p>
        This destination is reserved for its own feature. No node request,
        mutation, or fictitious endpoint is available from this shell.
      </p>
      <Button
        leadingIcon={<ArrowLeft size={16} />}
        onClick={() => void navigate(overviewRoute.path)}
      >
        Return to Overview
      </Button>
    </section>
  );
}

function UnknownRoute() {
  const navigate = useNavigate();

  return (
    <section className={styles.placeholder}>
      <span className={styles.placeholderIcon} aria-hidden="true">
        <MapPinOff size={34} />
      </span>
      <StatusBadge tone="warning">Unknown route</StatusBadge>
      <h2>Page not found</h2>
      <p>The requested dashboard route is not part of the approved shell.</p>
      <Button onClick={() => void navigate(overviewRoute.path)}>
        Return to Overview
      </Button>
    </section>
  );
}

function RouteLoadingState() {
  return (
    <div className={styles.routeState}>
      <StatePanel
        state="loading"
        title="Loading dashboard view"
        detail="Preparing the local presentation surface."
      />
    </div>
  );
}

export function DashboardShell() {
  const [drawerOpen, setDrawerOpen] = useState(false);
  const location = useLocation();

  const closeDrawer = useCallback(() => setDrawerOpen(false), []);
  const toggleDrawer = useCallback(() => setDrawerOpen((open) => !open), []);

  const activeRoute = dashboardRoutes.find(
    (route) =>
      route.path === location.pathname ||
      (route.id === 'agents' && location.pathname.startsWith(`${route.path}/`)),
  );
  const activeLabel = activeRoute?.label ?? 'Unknown route';
  const activeView = activeRoute?.id;

  return (
    <div className={styles.shell}>
      <DashboardChrome
        activeView={activeView}
        drawerOpen={drawerOpen}
        onDrawerToggle={toggleDrawer}
        onNavigate={closeDrawer}
      />

      <main className={styles.main} id="dashboard-content" tabIndex={-1}>
        <h1 className="sr-only">IAMINE {activeLabel} dashboard</h1>
        <Suspense fallback={<RouteLoadingState />}>
          <Routes>
            <Route
              index
              element={<Navigate replace to={overviewRoute.path} />}
            />
            <Route path={overviewRoute.path} element={<OverviewRoute />} />
            <Route
              path={getDashboardRoute('agents').path}
              element={<AgentsRoute />}
            />
            <Route
              path={agentPermissionRoutePattern}
              element={<AgentPermissionRoute />}
            />
            <Route
              path={getDashboardRoute('diagnostics').path}
              element={<DiagnosticsPage />}
            />
            <Route
              path={getDashboardRoute('nodes').path}
              element={<NodesPage />}
            />
            {dashboardRoutes
              .filter(
                (route) =>
                  route.id !== overviewRoute.id &&
                  route.id !== 'agents' &&
                  route.id !== 'diagnostics' &&
                  route.id !== 'nodes',
              )
              .map((route) => (
                <Route
                  key={route.id}
                  path={route.path}
                  element={<ReservedRoute route={route} />}
                />
              ))}
            <Route path="*" element={<UnknownRoute />} />
          </Routes>
        </Suspense>
      </main>

      <DashboardStatusBar />
    </div>
  );
}
