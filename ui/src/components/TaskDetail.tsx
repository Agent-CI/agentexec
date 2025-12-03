import type { ActivityDetail, ActivityLog } from '../types';
import { StatusBadge } from './StatusBadge';
import { ProgressBar } from './ProgressBar';

export interface TaskDetailProps {
  activity: ActivityDetail | null;
  loading?: boolean;
  error?: Error | null;
  onClose?: () => void;
  className?: string;
}

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  return date.toLocaleString();
}

function LogEntry({ log, isLatest }: { log: ActivityLog; isLatest: boolean }) {
  return (
    <div className={`ax-task-detail__log-entry ${isLatest ? 'ax-task-detail__log-entry--latest' : ''}`}>
      <div className="ax-task-detail__log-header">
        <StatusBadge status={log.status} />
        <span className="ax-task-detail__log-time">{formatTimestamp(log.created_at)}</span>
        {log.percentage !== null && (
          <span className="ax-task-detail__log-percentage">{log.percentage}%</span>
        )}
      </div>
      <p className="ax-task-detail__log-message">{log.message}</p>
    </div>
  );
}

/**
 * TaskDetail displays detailed information about a specific agent task
 */
export function TaskDetail({
  activity,
  loading = false,
  error = null,
  onClose,
  className = '',
}: TaskDetailProps) {
  if (loading) {
    return (
      <div className={`ax-task-detail ax-task-detail--loading ${className}`.trim()}>
        <div className="ax-task-detail__skeleton">
          <div className="ax-task-detail__skeleton-header" />
          <div className="ax-task-detail__skeleton-content" />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`ax-task-detail ax-task-detail--error ${className}`.trim()}>
        <div className="ax-task-detail__error">
          <h3>Error loading task</h3>
          <p>{error.message}</p>
          {onClose && (
            <button className="ax-task-detail__close-btn" onClick={onClose}>
              Close
            </button>
          )}
        </div>
      </div>
    );
  }

  if (!activity) {
    return (
      <div className={`ax-task-detail ax-task-detail--empty ${className}`.trim()}>
        <div className="ax-task-detail__empty-state">
          <p>Select a task to view details</p>
        </div>
      </div>
    );
  }

  const latestLog = activity.logs[activity.logs.length - 1];
  const currentStatus = latestLog?.status || 'queued';
  const currentPercentage = latestLog?.percentage || 0;

  return (
    <div className={`ax-task-detail ${className}`.trim()}>
      <div className="ax-task-detail__header">
        <div className="ax-task-detail__title-row">
          <h2 className="ax-task-detail__title">{activity.agent_type}</h2>
          {onClose && (
            <button className="ax-task-detail__close-btn" onClick={onClose} aria-label="Close">
              &times;
            </button>
          )}
        </div>
        <div className="ax-task-detail__meta">
          <span className="ax-task-detail__agent-id">
            Agent ID: <code>{activity.agent_id}</code>
          </span>
          <StatusBadge status={currentStatus} />
        </div>
        <div className="ax-task-detail__progress">
          <ProgressBar percentage={currentPercentage} status={currentStatus} />
          <span className="ax-task-detail__progress-text">{currentPercentage}% complete</span>
        </div>
        <div className="ax-task-detail__timestamps">
          <span>Created: {formatTimestamp(activity.created_at)}</span>
          <span>Updated: {formatTimestamp(activity.updated_at)}</span>
        </div>
      </div>

      <div className="ax-task-detail__logs">
        <h3 className="ax-task-detail__logs-title">Activity Log ({activity.logs.length} entries)</h3>
        <div className="ax-task-detail__logs-list">
          {[...activity.logs].reverse().map((log, index) => (
            <LogEntry key={log.id} log={log} isLatest={index === 0} />
          ))}
        </div>
      </div>
    </div>
  );
}
