export interface ActiveAgentsBadgeProps {
  count: number;
  loading?: boolean;
  className?: string;
}

/**
 * ActiveAgentsBadge displays the count of active agents
 */
export function ActiveAgentsBadge({ count, loading = false, className = '' }: ActiveAgentsBadgeProps) {
  return (
    <span className={`ax-active-badge ${loading ? 'ax-active-badge--loading' : ''} ${className}`.trim()}>
      {loading ? (
        <span className="ax-active-badge__loading">...</span>
      ) : (
        <span className="ax-active-badge__count">{count}</span>
      )}
    </span>
  );
}
