import { useQuery } from '@tanstack/react-query';
import { useApiData, useApiMutation } from './useApiData';
import { apiUrl } from '../lib/api';

// Types
export interface WorkQueue {
  name: string;
  partitions: number;
  namespace: string;
  groups?: string[];
}

export interface WorkQueueStats {
  total_enqueued: number;
  total_completed: number;
  total_failed: number;
  pending_count: number;
  in_flight_count: number;
  retry_count: number;
  dlq_count: number;
  groups?: GroupStats[];
}

export interface GroupStats {
  group: string;
  pending_count: number;
  in_flight_count: number;
  retry_count: number;
  dlq_count: number;
  consumer_count: number;
}

export interface Consumer {
  consumer_id: string;
  capacity: number;
  in_flight: number;
  labels: Record<string, string>;
  last_seen_ms: number;
  expires_at_ms: number;
  is_alive: boolean;
}

export interface PendingEntry {
  id: string;
  id_b64?: string;
  consumer_id: string;
  delivery_count: number;
  last_delivery_ms: number;
  lease_expires_at_ms: number;
  idle_ms: number;
}

export interface WorkQueueMessage {
  id: string;
  id_b64?: string;
  payload: string;
  headers: Record<string, string>;
  partition: number;
  enqueued_at_ms: number;
  priority?: number;
}

export interface DLQMessage {
  id: string;
  id_b64?: string;
  payload: string;
  headers: Record<string, string>;
  partition: number;
  seq: number;
  delivery_count: number;
  failed_at_ms: number;
  last_error: string;
  group: string;
}

// API Response types
export interface WorkQueuesListResponse {
  namespace: string;
  workqueues: string[];
}

export interface ConsumersResponse {
  consumers: Consumer[];
}

export interface PendingResponse {
  entries: PendingEntry[];
  next_token?: string;
}

export interface DLQResponse {
  messages: DLQMessage[];
  next_token?: string;
}

export interface GroupsResponse {
  namespace: string;
  name: string;
  groups: string[];
}

export interface ReadyMessage {
  id: string;
  id_b64?: string;
  seq: number;
  partition: number;
  priority: number;
  enqueued_at_ms: number;
  delay_until_ms: number;
  headers: Record<string, string>;
  payload?: string;
  payload_size: number;
}

export interface ReadyMessagesResponse {
  messages: ReadyMessage[];
}

export interface CompletedEntry {
  id: string;
  id_b64?: string;
  seq: number;
  partition: number;
  group: string;
  consumer_id: string;
  enqueued_at_ms: number;
  dequeued_at_ms: number;
  completed_at_ms: number;
  duration_ms: number;
  delivery_count: number;
  payload_size: number;
  headers?: Record<string, string>;
}

export interface CompletedResponse {
  entries: CompletedEntry[];
}

// Hooks

// List all workqueues in a namespace
export function useWorkQueues(namespace: string) {
  return useApiData<WorkQueuesListResponse>(
    ['workqueues', namespace],
    apiUrl(`/v1/workqueues?namespace=${encodeURIComponent(namespace)}`),
    { enabled: !!namespace }
  );
}

// Get workqueue stats
export function useWorkQueueStats(namespace: string, name: string, group?: string) {
  const params = new URLSearchParams({
    namespace,
    name,
    ...(group && { group })
  });
  
  return useQuery<WorkQueueStats>({
    queryKey: ['workqueue-stats', namespace, name, group],
    queryFn: async () => {
      const res = await fetch(apiUrl(`/v1/workqueues/stats?${params}`));
      if (!res.ok) throw new Error('Failed to fetch stats');
      const data = await res.json();
      
      // Parse int64 string values to numbers
      return {
        total_enqueued: parseInt(data.total_enqueued || '0'),
        total_completed: parseInt(data.total_completed || '0'),
        total_failed: parseInt(data.total_failed || '0'),
        pending_count: parseInt(data.pending_count || '0'),
        in_flight_count: parseInt(data.in_flight_count || '0'),
        retry_count: parseInt(data.retry_count || '0'),
        dlq_count: parseInt(data.dlq_count || '0'),
        groups: data.groups || []
      };
    },
    enabled: !!namespace && !!name
  });
}

// List consumers in a group
export function useConsumers(namespace: string, name: string, group: string) {
  const params = new URLSearchParams({ namespace, name, group });
  
  return useApiData<ConsumersResponse>(
    ['consumers', namespace, name, group],
    apiUrl(`/v1/workqueues/consumers/list?${params}`),
    { enabled: !!namespace && !!name && !!group }
  );
}

// List consumer groups for a workqueue
export function useWorkQueueGroups(namespace: string, name: string) {
  const params = new URLSearchParams({ namespace, name });
  
  return useApiData<GroupsResponse>(
    ['workqueue-groups', namespace, name],
    apiUrl(`/v1/workqueues/groups?${params}`),
    { enabled: !!namespace && !!name }
  );
}

// List pending (in-flight) messages
export function usePending(namespace: string, name: string, group: string, limit: number = 100) {
  const params = new URLSearchParams({
    namespace,
    name,
    group,
    limit: limit.toString()
  });
  
  return useApiData<PendingResponse>(
    ['pending', namespace, name, group, String(limit)],
    apiUrl(`/v1/workqueues/pending?${params}`),
    { enabled: !!namespace && !!name && !!group }
  );
}

// List ready/queued messages
export function useReadyMessages(namespace: string, name: string, partition: number = 0, limit: number = 100, includePayload: boolean = false) {
  const params = new URLSearchParams({
    namespace,
    name,
    partition: partition.toString(),
    limit: limit.toString(),
    include_payload: includePayload.toString()
  });
  
  return useApiData<ReadyMessagesResponse>(
    ['ready-messages', namespace, name, String(partition), String(limit), String(includePayload)],
    apiUrl(`/v1/workqueues/ready?${params}`),
    { enabled: !!namespace && !!name }
  );
}

// List completed messages
export function useCompletedMessages(namespace: string, name: string, group: string, partition: number = 0, limit: number = 100) {
  const params = new URLSearchParams({
    namespace,
    name,
    group,
    partition: partition.toString(),
    limit: limit.toString()
  });
  
  return useApiData<CompletedResponse>(
    ['completed-messages', namespace, name, group, String(partition), String(limit)],
    apiUrl(`/v1/workqueues/completed?${params}`),
    { enabled: !!namespace && !!name && !!group }
  );
}

// List DLQ messages
export function useDLQMessages(namespace: string, name: string, group: string, limit: number = 100) {
  const params = new URLSearchParams({
    namespace,
    name,
    group,
    limit: limit.toString()
  });
  
  return useApiData<DLQResponse>(
    ['dlq-messages', namespace, name, group, String(limit)],
    apiUrl(`/v1/workqueues/dlq?${params}`),
    { enabled: !!namespace && !!name && !!group }
  );
}

// Mutations

// Create workqueue
export function useCreateWorkQueue() {
  return useApiMutation<
    void,
    { namespace: string; name: string; partitions?: number }
  >(
    async ({ namespace, name, partitions }) => {
      const response = await fetch(apiUrl('/v1/workqueues/create'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, name, partitions }),
      });
      
      if (!response.ok) {
        throw new Error(`Failed to create workqueue: ${response.statusText}`);
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['workqueues']],
    }
  );
}

// Enqueue message
export function useEnqueueMessage() {
  return useApiMutation<
    { id: string },
    {
      namespace: string;
      name: string;
      payload: string;
      headers?: Record<string, string>;
      priority?: number;
      delay_ms?: number;
      key?: string;
    }
  >(
    async ({ namespace, name, payload, headers, priority, delay_ms, key }) => {
      const response = await fetch(apiUrl('/v1/workqueues/enqueue'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, name, payload, headers, priority, delay_ms, key }),
      });
      
      if (!response.ok) {
        throw new Error(`Failed to enqueue message: ${response.statusText}`);
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['workqueue-stats'], ['pending']],
    }
  );
}

// Complete message
export function useCompleteMessage() {
  return useApiMutation<
    void,
    {
      namespace: string;
      name: string;
      group: string;
      id: string;
    }
  >(
    async ({ namespace, name, group, id }) => {
      const response = await fetch(apiUrl('/v1/workqueues/complete'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, name, group, id }),
      });
      
      if (!response.ok) {
        throw new Error(`Failed to complete message: ${response.statusText}`);
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['pending'], ['workqueue-stats']],
    }
  );
}

// Fail message
export function useFailMessage() {
  return useApiMutation<
    void,
    {
      namespace: string;
      name: string;
      group: string;
      id: string;
      error?: string;
      retry_after_ms?: number;
    }
  >(
    async ({ namespace, name, group, id, error, retry_after_ms }) => {
      const response = await fetch(apiUrl('/v1/workqueues/fail'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, name, group, id, error, retry_after_ms }),
      });
      
      if (!response.ok) {
        throw new Error(`Failed to fail message: ${response.statusText}`);
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['pending'], ['workqueue-stats'], ['dlq-messages']],
    }
  );
}

// Claim messages
export function useClaimMessages() {
  return useApiMutation<
    { claimed_count: number },
    {
      namespace: string;
      name: string;
      group: string;
      new_consumer_id: string;
      ids: string[];
      lease_ms?: number;
    }
  >(
    async ({ namespace, name, group, new_consumer_id, ids, lease_ms }) => {
      const response = await fetch(apiUrl('/v1/workqueues/claim'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, name, group, new_consumer_id, ids, lease_ms }),
      });
      
      if (!response.ok) {
        throw new Error(`Failed to claim messages: ${response.statusText}`);
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['pending'], ['consumers']],
    }
  );
}

// Flush workqueue
export function useFlushWorkQueue() {
  return useApiMutation<
    void,
    {
      namespace: string;
      name: string;
      group?: string;
    }
  >(
    async ({ namespace, name, group }) => {
      const response = await fetch(apiUrl('/v1/workqueues/flush'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, name, group }),
      });
      
      if (!response.ok) {
        throw new Error(`Failed to flush workqueue: ${response.statusText}`);
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['workqueue-stats'], ['pending'], ['dlq-messages']],
    }
  );
}

// Register consumer
export function useRegisterConsumer() {
  return useApiMutation<
    void,
    {
      namespace: string;
      name: string;
      group: string;
      consumer_id: string;
      capacity?: number;
      labels?: Record<string, string>;
    }
  >(
    async ({ namespace, name, group, consumer_id, capacity, labels }) => {
      const response = await fetch(apiUrl('/v1/workqueues/consumers/register'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, name, group, consumer_id, capacity, labels }),
      });
      
      if (!response.ok) {
        throw new Error(`Failed to register consumer: ${response.statusText}`);
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['consumers']],
    }
  );
}

// Unregister consumer
export function useUnregisterConsumer() {
  return useApiMutation<
    void,
    {
      namespace: string;
      name: string;
      group: string;
      consumer_id: string;
    }
  >(
    async ({ namespace, name, group, consumer_id }) => {
      const response = await fetch(apiUrl('/v1/workqueues/consumers/unregister'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, name, group, consumer_id }),
      });
      
      if (!response.ok) {
        throw new Error(`Failed to unregister consumer: ${response.statusText}`);
      }
      
      return response.json();
    },
    {
      invalidateQueries: [['consumers']],
    }
  );
}

// WorkQueue Metrics
export interface WorkQueueMetricsResponse {
  series: Array<{
    label: string;
    points: [number, number][]; // [timestamp_ms, value]
  }>;
  start_ms: number;
  end_ms: number;
  step_ms: number;
  metric_type: string;
}

export function useWorkQueueMetrics(
  namespace: string,
  name: string,
  type: 'enqueue_count' | 'dequeue_count' | 'complete_count' | 'fail_count' | 'enqueue_rate' | 'dequeue_rate' | 'complete_rate' | 'queue_depth',
  options?: {
    range?: '7d' | '30d' | '90d';
    startMs?: number;
    endMs?: number;
    stepMs?: number;
    enabled?: boolean;
  }
) {
  const { range = '90d', startMs, endMs, stepMs, enabled = true } = options || {};

  const params = new URLSearchParams({
    namespace,
    name,
    type,
    ...(range && { range }),
    ...(startMs && { start_ms: startMs.toString() }),
    ...(endMs && { end_ms: endMs.toString() }),
    ...(stepMs && { step_ms: stepMs.toString() }),
  });

  return useQuery<WorkQueueMetricsResponse>({
    queryKey: ['workqueue-metrics', namespace, name, type, range, startMs, endMs, stepMs],
    queryFn: async () => {
      const res = await fetch(apiUrl(`/v1/workqueues/metrics?${params}`));
      if (!res.ok) {
        throw new Error(`Failed to fetch metrics: ${res.statusText}`);
      }
      return res.json();
    },
    enabled: enabled && !!namespace && !!name,
    refetchInterval: 30000, // Refresh every 30s
    staleTime: 25000,
  });
}
