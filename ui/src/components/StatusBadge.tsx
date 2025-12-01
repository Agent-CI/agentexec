import type { Status } from '../types';

export interface StatusBadgeProps {
  status: Status;
  className?: string;
}

const statusConfig: Record<Status, { label: string; className: string }> = {
  queued: {
    label: 'Queued',
    className: 'ax-status-badge--queued',
  },
  running: {
    label: 'Running',
    className: 'ax-status-badge--running',
  },
  complete: {
    label: 'Complete',
    className: 'ax-status-badge--complete',
  },
  error: {
    label: 'Error',
    className: 'ax-status-badge--error',
  },
  canceled: {
    label: 'Canceled',
    className: 'ax-status-badge--canceled',
  },
};

/**
 * StatusBadge displays the current status of an agent
 */
export function StatusBadge({ status, className = '' }: StatusBadgeProps) {
  const config = statusConfig[status] || statusConfig.queued;

  return (
    <span className={`ax-status-badge ${config.className} ${className}`.trim()}>
      {config.label}
    </span>
  );
}
