import {
  Activity,
  Bell,
  Bot,
  Boxes,
  CircleHelp,
  CircleUserRound,
  Cpu,
  LayoutDashboard,
  Menu,
  Search,
  ShieldCheck,
  Store,
  X,
} from 'lucide-react';
import { useEffect } from 'react';
import { NavLink } from 'react-router';
import type { ComponentType } from 'react';

import { BrandMark, IconButton, StatusBadge } from '../components';
import { dashboardRoutes, type DashboardRouteId } from './routes';
import styles from './DashboardChrome.module.css';

interface DashboardChromeProps {
  activeView?: DashboardRouteId;
  drawerOpen: boolean;
  onNavigate: () => void;
  onDrawerToggle: () => void;
}

const viewIcons: Record<DashboardRouteId, ComponentType<{ size?: number }>> = {
  overview: LayoutDashboard,
  agents: Bot,
  nodes: Boxes,
  models: Cpu,
  activity: Activity,
  marketplace: Store,
};

export function DashboardChrome({
  activeView,
  drawerOpen,
  onDrawerToggle,
  onNavigate,
}: DashboardChromeProps) {
  useEffect(() => {
    if (!drawerOpen) return;

    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onDrawerToggle();
    };

    window.addEventListener('keydown', closeOnEscape);
    return () => window.removeEventListener('keydown', closeOnEscape);
  }, [drawerOpen, onDrawerToggle]);

  return (
    <>
      <a className={styles.skipLink} href="#dashboard-content">
        Skip to dashboard content
      </a>
      <aside
        id="primary-navigation"
        className={`${styles.sidebar} ${drawerOpen ? styles.drawerOpen : ''}`}
        aria-label="Primary navigation"
      >
        <div className={styles.sidebarBrand}>
          <BrandMark />
          <IconButton
            className={styles.drawerClose}
            label="Close navigation"
            icon={<X size={18} />}
            onClick={onDrawerToggle}
          />
        </div>
        <nav className={styles.sideNav}>
          {dashboardRoutes.map((item) => {
            const Icon = viewIcons[item.id];
            const selected = item.id === activeView;
            return (
              <NavLink
                className={styles.sideNavItem}
                data-selected={selected}
                key={item.id}
                aria-label={`Open ${item.label} from sidebar`}
                title={item.label}
                onClick={onNavigate}
                to={item.path}
              >
                <Icon size={19} />
                <span>{item.label}</span>
              </NavLink>
            );
          })}
        </nav>
        <div
          className={styles.securityButton}
          aria-label="Security controls unavailable in preview"
          title="Security controls unavailable in preview"
        >
          <ShieldCheck size={19} />
          <span>Security pending</span>
        </div>
      </aside>

      {drawerOpen && (
        <button
          className={styles.backdrop}
          type="button"
          aria-label="Close navigation backdrop"
          onClick={onDrawerToggle}
        />
      )}

      <header className={styles.topbar}>
        <div className={styles.mobileBrand}>
          <IconButton
            label="Open navigation"
            icon={<Menu size={19} />}
            aria-controls="primary-navigation"
            aria-expanded={drawerOpen}
            onClick={onDrawerToggle}
          />
          <BrandMark />
        </div>
        <nav className={styles.topNav} aria-label="Dashboard sections">
          {dashboardRoutes.map((item) => (
            <NavLink
              className={styles.topNavItem}
              data-selected={item.id === activeView}
              key={item.id}
              onClick={onNavigate}
              to={item.path}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
        <div className={styles.topActions}>
          <StatusBadge tone="info">Preview data</StatusBadge>
          <IconButton
            disabled
            label="Search unavailable in preview"
            icon={<Search size={17} />}
          />
          <IconButton
            disabled
            label="Notifications unavailable in preview"
            icon={<Bell size={17} />}
          />
          <IconButton
            disabled
            label="Help unavailable in preview"
            icon={<CircleHelp size={17} />}
          />
          <span className={styles.focusStatus}>
            <span aria-hidden="true" />
            Focus
          </span>
          <span
            className={styles.avatar}
            role="img"
            aria-label="Local operator preview"
          >
            <CircleUserRound size={24} />
          </span>
        </div>
      </header>
    </>
  );
}
