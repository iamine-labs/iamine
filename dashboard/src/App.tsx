import { HashRouter } from 'react-router';

import { DashboardErrorBoundary } from './app/DashboardErrorBoundary';
import { DashboardShell } from './app/DashboardShell';

export function App() {
  return (
    <DashboardErrorBoundary>
      <HashRouter>
        <DashboardShell />
      </HashRouter>
    </DashboardErrorBoundary>
  );
}
