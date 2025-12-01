import type { Status } from '../types';

export interface ProgressBarProps {
  percentage: number;
  status: Status;
  className?: string;
}

/**
 * ProgressBar displays completion progress for an agent
 */
export function ProgressBar({ percentage, status, className = '' }: ProgressBarProps) {
  const isActive = status === 'running' || status === 'queued';

  return (
    <div className={`ax-progress-bar ${className}`.trim()}>
      <div
        className={`ax-progress-bar__fill ${isActive ? 'ax-progress-bar__fill--animated' : ''}`}
        style={{ width: `${Math.min(100, Math.max(0, percentage))}%` }}
        data-status={status}
      />
    </div>
  );
}
