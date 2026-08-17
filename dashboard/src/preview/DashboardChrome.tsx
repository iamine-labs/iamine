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
import type { ComponentType } from 'react';

import { BrandMark, IconButton, StatusBadge } from '../components';
import { type DashboardView, navigationItems } from './fixtures';
import styles from './DashboardChrome.module.css';

interface DashboardChromeProps {
  activeView: DashboardView;
  drawerOpen: boolean;
  onDrawerToggle: () => void;
  onNavigate: (view: DashboardView) => void;
}

const viewIcons: Record<DashboardView, ComponentType<{ size?: number }>> = {
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
  return (
    <>
      <aside
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
          {navigationItems.map((item) => {
            const Icon = viewIcons[item.id];
            const selected = item.id === activeView;
            return (
              <button
                className={styles.sideNavItem}
                data-selected={selected}
                type="button"
                key={item.id}
                aria-current={selected ? 'page' : undefined}
                aria-label={`Open ${item.label} from sidebar`}
                title={item.label}
                onClick={() => onNavigate(item.id)}
              >
                <Icon size={19} />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>
        <button
          className={styles.securityButton}
          type="button"
          aria-label="Security status preview"
          title="Security status"
        >
          <ShieldCheck size={19} />
          <span>Secure</span>
        </button>
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
            onClick={onDrawerToggle}
          />
          <BrandMark />
        </div>
        <nav className={styles.topNav} aria-label="Dashboard sections">
          {navigationItems.map((item) => (
            <button
              className={styles.topNavItem}
              data-selected={item.id === activeView}
              type="button"
              key={item.id}
              aria-current={item.id === activeView ? 'page' : undefined}
              onClick={() => onNavigate(item.id)}
            >
              {item.label}
            </button>
          ))}
        </nav>
        <div className={styles.topActions}>
          <StatusBadge tone="info">Preview data</StatusBadge>
          <IconButton label="Search preview" icon={<Search size={17} />} />
          <IconButton label="Preview notifications" icon={<Bell size={17} />} />
          <IconButton label="Preview help" icon={<CircleHelp size={17} />} />
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
