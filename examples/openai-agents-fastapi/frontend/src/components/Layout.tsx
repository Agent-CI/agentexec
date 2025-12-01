import { Outlet, NavLink } from 'react-router-dom';
import { ActiveAgentsBadge } from 'agentexec-ui';
import { useActiveCount } from '../api/queries';

export function Layout() {
  const { data, isLoading } = useActiveCount();

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="sidebar__header">
          <h1 className="sidebar__title">AgentExec</h1>
        </div>
        <nav className="sidebar__nav">
          <NavLink
            to="/agents"
            className={({ isActive }) =>
              `sidebar__nav-item ${isActive ? 'sidebar__nav-item--active' : ''}`
            }
          >
            <svg className="sidebar__nav-icon" viewBox="0 0 16 16" fill="currentColor">
              <path d="M8 0a8 8 0 1 1 0 16A8 8 0 0 1 8 0ZM1.5 8a6.5 6.5 0 1 0 13 0 6.5 6.5 0 0 0-13 0Zm4.879-2.773 4.264 2.559a.25.25 0 0 1 0 .428l-4.264 2.559A.25.25 0 0 1 6 10.559V5.442a.25.25 0 0 1 .379-.215Z" />
            </svg>
            <span className="sidebar__nav-text">Background Agents</span>
            <ActiveAgentsBadge count={data?.count ?? 0} loading={isLoading} />
          </NavLink>
        </nav>
        <div className="sidebar__footer">
          <p className="sidebar__footer-text">Updates every 15s</p>
        </div>
      </aside>

      <main className="main">
        <Outlet />
      </main>
    </div>
  );
}
