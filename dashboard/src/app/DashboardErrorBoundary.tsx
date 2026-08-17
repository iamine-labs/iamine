import { AlertTriangle, RotateCcw } from 'lucide-react';
import { Component, type ErrorInfo, type ReactNode } from 'react';

import { Button } from '../components';
import styles from './DashboardErrorBoundary.module.css';

interface DashboardErrorBoundaryProps {
  children: ReactNode;
}

interface DashboardErrorBoundaryState {
  failed: boolean;
}

export class DashboardErrorBoundary extends Component<
  DashboardErrorBoundaryProps,
  DashboardErrorBoundaryState
> {
  state: DashboardErrorBoundaryState = { failed: false };

  static getDerivedStateFromError(): DashboardErrorBoundaryState {
    return { failed: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Dashboard shell render failed', error, info.componentStack);
  }

  render() {
    if (!this.state.failed) return this.props.children;

    return (
      <main className={styles.fallback} role="alert">
        <span className={styles.icon} aria-hidden="true">
          <AlertTriangle size={30} />
        </span>
        <h1>Dashboard unavailable</h1>
        <p>
          The local interface could not render. No IAMINE node action was
          attempted.
        </p>
        <Button
          leadingIcon={<RotateCcw size={16} />}
          onClick={() => window.location.reload()}
        >
          Reload dashboard
        </Button>
      </main>
    );
  }
}
