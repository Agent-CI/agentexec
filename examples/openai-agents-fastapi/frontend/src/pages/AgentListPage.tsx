import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useActivityList, TaskList, Pagination } from 'agentexec-ui';

const POLL_INTERVAL = 15000; // 15 seconds
const PAGE_SIZE = 20;

export function AgentListPage() {
  const navigate = useNavigate();
  const [page, setPage] = useState(1);

  const { data: activityList, loading: listLoading } = useActivityList({
    page,
    pageSize: PAGE_SIZE,
    pollInterval: POLL_INTERVAL,
  });

  const handleTaskClick = (agentId: string) => {
    navigate(`/agents/${agentId}`);
  };

  return (
    <>
      <header className="main__header">
        <h2 className="main__title">Background Agents</h2>
        <p className="main__subtitle">
          {activityList ? `${activityList.total} total tasks` : 'Loading...'}
        </p>
      </header>

      <div className="main__content">
        <div className="task-panel">
          <div className="task-panel__list">
            <TaskList
              items={activityList?.items || []}
              loading={listLoading}
              onTaskClick={handleTaskClick}
            />
            {activityList && activityList.total_pages > 1 && (
              <Pagination
                page={page}
                totalPages={activityList.total_pages}
                onPageChange={setPage}
              />
            )}
          </div>
        </div>
      </div>
    </>
  );
}
