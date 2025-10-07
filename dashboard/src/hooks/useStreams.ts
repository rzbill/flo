import { useApiData, useApiMutation } from './useApiData';
import { apiUrl } from '../lib/api';

// Types
export interface Stream {
  name: string;
  partitions: number;
  lastActivity?: Date;
  namespace: string;
}

export interface StreamStats {
  total_count: number;
  total_bytes: number;
  active_subscribers?: number;
  groups_count?: number;
  last_publish_ms?: number;
  last_delivered_ms?: number;
  partitions?: Array<{
    partition: number;
    first_seq: number;
    last_seq: number;
    count: number;
    bytes: number;
    last_publish_ms?: number;
  }>;
}

export interface Message {
  id: string;
  rawIdB64?: string;
  partition: number;
  timestamp: Date;
  payload: string;
  headers: Record<string, string>;
  attempts: number;
}

// Retry/DLQ Types
export interface RetryMessage {
  id: string;
  payload: string;
  headers: Record<string, string>;
  partition: number;
  seq: number;
  retry_count: number;
  max_retries: number;
  next_retry_at_ms: number;
  created_at_ms: number;
  last_error: string;
  group: string;
}

export interface DLQMessage {
  id: string;
  payload: string;
  headers: Record<string, string>;
  partition: number;
  seq: number;
  retry_count: number;
  max_retries: number;
  failed_at_ms: number;
  last_error: string;
  group: string;
}

// Retry/DLQ stats (new concise schema)
export interface RetryDLQGroupStat {
  name: string;
  retry_count: number;
  dlq_count: number;
  total_retries: number;
  success_rate: number;
}

export interface RetryDLQStatsSummary {
  total_groups: number;
  total_retry: number;
  total_dlq: number;
  total_retries: number;
  success_rate: number;
}

export interface RetryDLQResponse {
  items: RetryMessage[] | DLQMessage[] | null;
  next_token?: string;
}

export interface RetryDLQStatsResponse {
  namespace: string;
  stream: string;
  groups: RetryDLQGroupStat[];
  summary: RetryDLQStatsSummary;
}

// Metrics (time-series)
export interface MetricsSeries {
  label: string;
  points: [number, number][]; // [t_ms, value]
}

export interface MetricsResponse {
  namespace: string;
  stream: string;
  metric: 'publish_count' | 'ack_count' | 'publish_rate' | 'bytes_rate';
  unit: string;
  start_ms: number;
  end_ms: number;
  step_ms: number;
  series: MetricsSeries[];
}

// Stream-specific hooks
export function useStreams(namespace: string) {
  return useApiData<{ streams: string[] }>(
    ['streams', namespace],
    apiUrl(`/v1/streams?namespace=${encodeURIComponent(namespace)}`),
    {
      staleTime: 30 * 1000, // 30 seconds
    }
  );
}

export function useNamespaces() {
  return useApiData<{ namespaces: string[] }>(
    ['namespaces'],
    apiUrl('/v1/namespaces'),
    { staleTime: 5 * 60 * 1000 }
  );
}

export function useStreamStats(namespace: string, stream: string) {
  return useApiData<StreamStats>(
    ['stream-stats', namespace, stream],
    apiUrl(`/v1/streams/stats?namespace=${encodeURIComponent(namespace)}&stream=${encodeURIComponent(stream)}`),
    {
      staleTime: 10 * 1000, // 10 seconds
      refetchInterval: 30 * 1000, // Refetch every 30 seconds
    }
  );
}

export function useStreamMessages(
  namespace: string,
  stream: string,
  options?: {
    limit?: number;
    reverse?: boolean;
    filter?: string;
  }
) {
  const params = new URLSearchParams({
    namespace,
    stream,
    limit: String(options?.limit || 200),
    reverse: String(options?.reverse ? 1 : 0),
  });
  
  if (options?.filter) {
    params.set('filter', options.filter);
  }

  return useApiData<{ items: any[] }>(
    ['stream-messages', namespace, stream, options?.filter || ''],
    apiUrl(`/v1/streams/search?${params.toString()}`),
    {
      staleTime: 5 * 1000, // 5 seconds
    }
  );
}

export function useCreateStream() {
  return useApiMutation<
    { success: boolean },
    { namespace: string; stream: string; partitions: number }
  >(
    async ({ namespace, stream, partitions }) => {
      const response = await fetch(apiUrl('/v1/streams/create'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, stream, partitions }),
      });
      
      if (!response.ok) {
        throw new Error(`Failed to create stream: ${response.statusText}`);
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['streams']],
    }
  );
}

export function useFlushStream() {
  return useApiMutation<
    { deleted_count: number },
    { namespace: string; stream: string }
  >(
    async ({ namespace, stream }) => {
      const response = await fetch(
        apiUrl(`/v1/streams/flush?namespace=${encodeURIComponent(namespace)}&stream=${encodeURIComponent(stream)}`),
        { method: 'DELETE' }
      );
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || 'Flush failed');
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['stream-stats'], ['stream-messages']],
    }
  );
}

export function useDeleteMessage() {
  return useApiMutation<
    { success: boolean },
    { namespace: string; stream: string; id: string }
  >(
    async ({ namespace, stream, id }) => {
      const response = await fetch(
        apiUrl(`/v1/streams/delete?namespace=${encodeURIComponent(namespace)}&stream=${encodeURIComponent(stream)}&id=${encodeURIComponent(id)}`),
        { method: 'DELETE' }
      );
      
      if (!response.ok) {
        throw new Error(`Failed to delete message: ${response.statusText}`);
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['stream-messages'], ['stream-stats']],
    }
  );
}

export function usePublishMessage() {
  return useApiMutation<
    { success: boolean; eventId?: string },
    {
      namespace: string;
      stream: string;
      payload: string;
      headers?: Record<string, string>;
      key?: string;
    }
  >(
    async ({ namespace, stream, payload, headers = {}, key }) => {
      const response = await fetch(apiUrl('/v1/streams/publish'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace,
          stream,
          payload: new TextEncoder().encode(payload),
          headers,
          key,
        }),
      });
      
      if (!response.ok) {
        throw new Error('Failed to publish message');
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['stream-messages'], ['stream-stats']],
    }
  );
}

// Retry/DLQ API functions
export function useRetryMessages(
  namespace: string,
  stream: string,
  group?: string,
  options?: {
    limit?: number;
    reverse?: boolean;
    startToken?: string;
    enabled?: boolean;
  }
) {
  const params = new URLSearchParams({
    namespace,
    stream,
    ...(group && { group }),
    ...(options?.limit && { limit: options.limit.toString() }),
    ...(options?.reverse && { reverse: options.reverse.toString() }),
    ...(options?.startToken && { start_token: options.startToken }),
  });

  return useApiData<RetryDLQResponse>(
    ['retry-messages', namespace, stream, group || 'all', options?.startToken || ''],
    apiUrl(`/v1/streams/retry?${params}`),
    {
      enabled: options?.enabled ?? true,
      staleTime: 10 * 1000, // 10 seconds
    }
  );
}

export function useDLQMessages(
  namespace: string,
  stream: string,
  group?: string,
  options?: {
    limit?: number;
    reverse?: boolean;
    startToken?: string;
    enabled?: boolean;
  }
) {
  const params = new URLSearchParams({
    namespace,
    stream,
    ...(group && { group }),
    ...(options?.limit && { limit: options.limit.toString() }),
    ...(options?.reverse && { reverse: options.reverse.toString() }),
    ...(options?.startToken && { start_token: options.startToken }),
  });

  return useApiData<RetryDLQResponse>(
    ['dlq-messages', namespace, stream, group || 'all', options?.startToken || ''],
    apiUrl(`/v1/streams/dlq?${params}`),
    {
      enabled: options?.enabled ?? true,
      staleTime: 10 * 1000, // 10 seconds
    }
  );
}

export function useRetryDLQStats(
  namespace: string,
  stream: string,
  group?: string,
  options?: {
    enabled?: boolean;
  }
) {
  const params = new URLSearchParams({
    namespace,
    stream,
    ...(group && { group }),
  });

  return useApiData<RetryDLQStatsResponse>(
    ['retry-dlq-stats', namespace, stream, group || 'all'],
    apiUrl(`/v1/streams/retry-dlq-stats?${params}`),
    {
      enabled: options?.enabled ?? true,
      staleTime: 30 * 1000, // 30 seconds
    }
  );
}

// Fetch stream metrics (time-series). Provide either range or start/end.
export function useStreamMetrics(
  namespace: string,
  stream: string,
  type: 'publish_count' | 'ack_count' | 'publish_rate' | 'bytes_rate',
  options?: {
    range?: '7d' | '30d' | '90d';
    startMs?: number;
    endMs?: number;
    stepMs?: number;
    byPartition?: boolean;
    enabled?: boolean;
  }
) {
  const params = new URLSearchParams({
    namespace,
    stream,
    type,
  });
  if (options?.range) params.set('range', options.range);
  if (options?.startMs) params.set('start_ms', String(options.startMs));
  if (options?.endMs) params.set('end_ms', String(options.endMs));
  if (options?.stepMs) params.set('step_ms', String(options.stepMs));
  if (options?.byPartition) params.set('by_partition', '1');

  return useApiData<MetricsResponse>(
    ['stream-metrics', namespace, stream, type, options?.range || `${options?.startMs}-${options?.endMs}`, String(options?.stepMs || '')],
    apiUrl(`/v1/streams/metrics?${params.toString()}`),
    {
      enabled: options?.enabled ?? true,
      staleTime: 10 * 1000,
      refetchInterval: 30 * 1000,
    }
  );
}

// Stream Groups Types
export interface StreamGroupInfo {
  name: string;
  has_cursor?: boolean;
  has_retry_data?: boolean;
  active_subscribers?: number;
  last_delivered_ms?: number;
  total_lag?: number;
  partitions?: Array<{
    partition: number;
    cursor_seq: number;
    end_seq: number;
    lag: number;
  }>;
}

export interface StreamGroupsResponse {
  namespace: string;
  stream: string;
  groups: StreamGroupInfo[];
  summary?: {
    total?: number;
    with_cursor?: number;
    with_retry_data?: number;
  };
}

export const useStreamGroups = (
  namespace: string,
  stream: string,
  options?: { enabled?: boolean }
) => {
  const params = new URLSearchParams({
    namespace,
    stream,
  });

  return useApiData<StreamGroupsResponse>(
    ['stream-groups', namespace, stream],
    apiUrl(`/v1/streams/groups?${params}`),
    {
      enabled: options?.enabled ?? true,
      staleTime: 30 * 1000, // 30 seconds
    }
  );
}
