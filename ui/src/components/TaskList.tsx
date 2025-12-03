import type { ActivityListItem } from '../types';
import { StatusBadge } from './StatusBadge';
import { ProgressBar } from './ProgressBar';

export interface TaskListProps {
  items: ActivityListItem[];
  loading?: boolean;
  onTaskClick?: (agentId: string) => void;
  selectedAgentId?: string | null;
  className?: string;
}

function formatElapsedTime(seconds: number): string {
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  if (minutes < 60) {
    return `${minutes}m ${remainingSeconds}s`;
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `${hours}h ${remainingMinutes}m`;
}

function formatTimestamp(timestamp: string | null): string {
  if (!timestamp) return '-';
  const date = new Date(timestamp);
  return date.toLocaleString();
}

/**
 * TaskList displays a list of agent tasks with their status and progress
 */
export function TaskList({
  items,
  loading = false,
  onTaskClick,
  selectedAgentId,
  className = '',
}: TaskListProps) {
  if (loading && items.length === 0) {
    return (
      <div className={`ax-task-list ax-task-list--loading ${className}`.trim()}>
        <div className="ax-task-list__skeleton">
          {[1, 2, 3].map((i) => (
            <div key={i} className="ax-task-list__skeleton-item" />
          ))}
        </div>
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className={`ax-task-list ax-task-list--empty ${className}`.trim()}>
        <div className="ax-task-list__empty-state">
          <p>No tasks found</p>
        </div>
      </div>
    );
  }

  return (
    <div className={`ax-task-list ${className}`.trim()}>
      <table className="ax-task-list__table">
        <thead>
          <tr>
            <th>Task Type</th>
            <th>Status</th>
            <th>Progress</th>
            <th>Latest Update</th>
            <th>Duration</th>
          </tr>
        </thead>
        <tbody>
          {items.map((item) => {
            const isSelected = selectedAgentId === item.agent_id;
            return (
              <tr
                key={item.agent_id}
                className={`ax-task-list__row ${isSelected ? 'ax-task-list__row--selected' : ''} ${onTaskClick ? 'ax-task-list__row--clickable' : ''}`}
                onClick={() => onTaskClick?.(item.agent_id)}
              >
                <td className="ax-task-list__cell ax-task-list__cell--type">
                  <span className="ax-task-list__agent-type">{item.agent_type}</span>
                  <span className="ax-task-list__agent-id">{item.agent_id.slice(0, 8)}...</span>
                </td>
                <td className="ax-task-list__cell ax-task-list__cell--status">
                  <StatusBadge status={item.status} />
                </td>
                <td className="ax-task-list__cell ax-task-list__cell--progress">
                  <div className="ax-task-list__progress-container">
                    <ProgressBar percentage={item.percentage} status={item.status} />
                    <span className="ax-task-list__progress-text">
                      {item.percentage}%
                    </span>
                  </div>
                </td>
                <td className="ax-task-list__cell ax-task-list__cell--message">
                  <span className="ax-task-list__message" title={item.latest_log_message || ''}>
                    {item.latest_log_message || '-'}
                  </span>
                  <span className="ax-task-list__timestamp">
                    {formatTimestamp(item.latest_log_timestamp)}
                  </span>
                </td>
                <td className="ax-task-list__cell ax-task-list__cell--duration">
                  {formatElapsedTime(item.elapsed_time_seconds)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
