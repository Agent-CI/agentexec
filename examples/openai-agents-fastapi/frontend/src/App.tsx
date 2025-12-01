import { useState } from 'react';
import {
  useActivityList,
  useActivityDetail,
  useActiveCount,
  TaskList,
  TaskDetail,
  Pagination,
  ActiveAgentsBadge,
} from 'agentexec-ui';

// Import the GitHub dark theme styles
import 'agentexec-ui/styles';

const POLL_INTERVAL = 15000; // 15 seconds
const PAGE_SIZE = 20;

function App() {
  const [page, setPage] = useState(1);
  const [selectedAgentId, setSelectedAgentId] = useState<string | null>(null);

  // Fetch active agent count (updates every 15 seconds)
  const { count: activeCount, loading: countLoading } = useActiveCount({
    pollInterval: POLL_INTERVAL,
  });

  // Fetch paginated activity list (updates every 15 seconds)
  const { data: activityList, loading: listLoading } = useActivityList({
    page,
    pageSize: PAGE_SIZE,
    pollInterval: POLL_INTERVAL,
  });

  // Fetch selected task detail (updates every 5 seconds when selected)
  const { data: taskDetail, loading: detailLoading, error: detailError } = useActivityDetail({
    agentId: selectedAgentId,
    pollInterval: 5000,
  });

  const handleTaskClick = (agentId: string) => {
    setSelectedAgentId(agentId);
  };

  const handleCloseDetail = () => {
    setSelectedAgentId(null);
  };

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="sidebar__header">
          <h1 className="sidebar__title">AgentExec</h1>
        </div>
        <nav className="sidebar__nav">
          <a href="#" className="sidebar__nav-item sidebar__nav-item--active">
            <svg className="sidebar__nav-icon" viewBox="0 0 16 16" fill="currentColor">
              <path d="M8 0a8 8 0 1 1 0 16A8 8 0 0 1 8 0ZM1.5 8a6.5 6.5 0 1 0 13 0 6.5 6.5 0 0 0-13 0Zm4.879-2.773 4.264 2.559a.25.25 0 0 1 0 .428l-4.264 2.559A.25.25 0 0 1 6 10.559V5.442a.25.25 0 0 1 .379-.215Z" />
            </svg>
            <span className="sidebar__nav-text">Background Agents</span>
            <ActiveAgentsBadge count={activeCount} loading={countLoading} />
          </a>
        </nav>
        <div className="sidebar__footer">
          <p className="sidebar__footer-text">
            Updates every 15s
          </p>
        </div>
      </aside>

      <main className="main">
        <header className="main__header">
          <h2 className="main__title">Background Agents</h2>
          <p className="main__subtitle">
            {activityList ? `${activityList.total} total tasks` : 'Loading...'}
          </p>
        </header>

        <div className="main__content">
          <div className={`task-panel ${selectedAgentId ? 'task-panel--with-detail' : ''}`}>
            <div className="task-panel__list">
              <TaskList
                items={activityList?.items || []}
                loading={listLoading}
                onTaskClick={handleTaskClick}
                selectedAgentId={selectedAgentId}
              />
              {activityList && activityList.total_pages > 1 && (
                <Pagination
                  page={page}
                  totalPages={activityList.total_pages}
                  onPageChange={setPage}
                />
              )}
            </div>

            {selectedAgentId && (
              <div className="task-panel__detail">
                <TaskDetail
                  activity={taskDetail}
                  loading={detailLoading}
                  error={detailError}
                  onClose={handleCloseDetail}
                />
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
