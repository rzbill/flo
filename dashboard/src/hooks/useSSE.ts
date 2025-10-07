import { useEffect, useRef, useState } from 'react';

export interface SSEEvent {
  id: string;
  partition: number;
  sequence: number;
  timestamp: Date;
  payload: string;
  headers: Record<string, string>;
  attempts: number;
}

export function useSSE(
  url: string,
  options?: {
    enabled?: boolean;
    onMessage?: (event: SSEEvent) => void;
    onError?: (error: Event) => void;
    onOpen?: () => void;
    maxReconnectDelayMs?: number;
  }
) {
  const [isConnected, setIsConnected] = useState(false);
  const [events, setEvents] = useState<SSEEvent[]>([]);
  const sseRef = useRef<EventSource | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimerRef = useRef<number | null>(null);

  useEffect(() => {
    if (!options?.enabled || !url) return;

    const connect = () => {
      const es = new EventSource(url);
      sseRef.current = es;

      es.onopen = () => {
        setIsConnected(true);
        reconnectAttemptsRef.current = 0;
        options?.onOpen?.();
      };

      es.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          const sseEvent: SSEEvent = {
            id: data.id || '',
            partition: data.partition || 0,
            sequence: data.sequence || Date.now(),
            timestamp: new Date(),
            payload: data.payload ? atob(data.payload) : '',
            headers: data.headers || {},
            attempts: data.attempts || 1,
          };
          setEvents(prev => [...prev, sseEvent]);
          options?.onMessage?.(sseEvent);
        } catch (error) {
          // ignore parse errors
        }
      };

      es.onerror = (error) => {
        setIsConnected(false);
        options?.onError?.(error);
        try { es.close(); } catch {}
        scheduleReconnect();
      };
    };

    const scheduleReconnect = () => {
      const attempt = reconnectAttemptsRef.current + 1;
      reconnectAttemptsRef.current = attempt;
      const maxDelay = options?.maxReconnectDelayMs ?? 5000;
      const base = Math.min(maxDelay, 500 * Math.pow(2, attempt));
      const jitter = Math.random() * 200;
      const delay = Math.min(maxDelay, base + jitter);
      if (reconnectTimerRef.current) window.clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = window.setTimeout(connect, delay);
    };

    connect();

    return () => {
      if (reconnectTimerRef.current) window.clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
      const es = sseRef.current;
      if (es) {
        try { es.close(); } catch {}
      }
      sseRef.current = null;
      setIsConnected(false);
    };
  }, [url, options?.enabled, options?.onMessage, options?.onError, options?.onOpen, options?.maxReconnectDelayMs]);

  const disconnect = () => {
    if (reconnectTimerRef.current) window.clearTimeout(reconnectTimerRef.current);
    reconnectTimerRef.current = null;
    if (sseRef.current) {
      try { sseRef.current.close(); } catch {}
      sseRef.current = null;
      setIsConnected(false);
    }
  };

  const clearEvents = () => {
    setEvents([]);
  };

  return {
    isConnected,
    events,
    disconnect,
    clearEvents,
  };
}
