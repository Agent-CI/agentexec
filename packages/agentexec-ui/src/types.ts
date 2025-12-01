/**
 * Agent execution status enum matching backend Status enum
 */
export type Status = 'queued' | 'running' | 'complete' | 'error' | 'canceled';

/**
 * Activity log entry matching ActivityLogSchema
 */
export interface ActivityLog {
  id: string;
  message: string;
  status: Status;
  completion_percentage: number;
  created_at: string;
}

/**
 * Detailed activity record matching ActivityDetailSchema
 */
export interface ActivityDetail {
  id: string;
  agent_id: string;
  agent_type: string;
  created_at: string;
  updated_at: string;
  logs: ActivityLog[];
}

/**
 * Activity list item matching ActivityListItemSchema
 */
export interface ActivityListItem {
  agent_id: string;
  agent_type: string;
  status: Status;
  latest_log_message: string | null;
  latest_log_timestamp: string | null;
  completion_percentage: number;
  started_at: string | null;
  elapsed_time_seconds: number;
}

/**
 * Paginated activity list matching ActivityListSchema
 */
export interface ActivityList {
  items: ActivityListItem[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

/**
 * Active count response
 */
export interface ActiveCountResponse {
  count: number;
}

/**
 * API client configuration
 */
export interface ApiConfig {
  baseUrl: string;
}
