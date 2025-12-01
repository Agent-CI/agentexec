import { useState, useEffect, useCallback } from 'react';
import type { ActiveCountResponse } from '../types';

interface UseActiveCountOptions {
  baseUrl?: string;
  pollInterval?: number;
}

interface UseActiveCountResult {
  count: number;
  loading: boolean;
  error: Error | null;
  refetch: () => void;
}

/**
 * Hook to fetch count of active (queued or running) agents
 */
export function useActiveCount(options: UseActiveCountOptions = {}): UseActiveCountResult {
  const { baseUrl = '', pollInterval } = options;

  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchData = useCallback(async () => {
    try {
      const url = `${baseUrl}/api/agents/active/count`;
      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const result: ActiveCountResponse = await response.json();
      setCount(result.count);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e : new Error('Unknown error'));
    } finally {
      setLoading(false);
    }
  }, [baseUrl]);

  useEffect(() => {
    fetchData();

    if (pollInterval && pollInterval > 0) {
      const interval = setInterval(fetchData, pollInterval);
      return () => clearInterval(interval);
    }
  }, [fetchData, pollInterval]);

  return { count, loading, error, refetch: fetchData };
}
