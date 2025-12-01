import { useParams, useNavigate } from 'react-router-dom';
import { TaskDetail } from 'agentexec-ui';
import { useActivityDetail } from '../api/queries';

export function AgentDetailPage() {
  const { agentId } = useParams<{ agentId: string }>();
  const navigate = useNavigate();

  const { data: taskDetail, isLoading, error } = useActivityDetail(agentId ?? null);

  const handleClose = () => {
    navigate('/agents');
  };

  return (
    <>
      <header className="main__header">
        <div className="main__header-row">
          <button className="main__back-btn" onClick={handleClose} aria-label="Back to list">
            <svg viewBox="0 0 16 16" fill="currentColor" width="16" height="16">
              <path d="M7.78 12.53a.75.75 0 0 1-1.06 0L2.47 8.28a.75.75 0 0 1 0-1.06l4.25-4.25a.751.751 0 0 1 1.042.018.751.751 0 0 1 .018 1.042L4.81 7h7.44a.75.75 0 0 1 0 1.5H4.81l2.97 2.97a.75.75 0 0 1 0 1.06Z" />
            </svg>
            Back to agents
          </button>
        </div>
        <h2 className="main__title">
          {taskDetail?.agent_type || 'Agent Details'}
        </h2>
        <p className="main__subtitle">
          {agentId ? `ID: ${agentId}` : ''}
        </p>
      </header>

      <div className="main__content">
        <div className="task-panel task-panel--detail-view">
          <TaskDetail
            activity={taskDetail ?? null}
            loading={isLoading}
            error={error instanceof Error ? error : null}
          />
        </div>
      </div>
    </>
  );
}
