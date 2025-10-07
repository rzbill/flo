import React, { useState, useEffect, useRef } from 'react';
import { Button } from '../../ui/button';
import { Card, CardContent } from '../../ui/card';
import { Badge } from '../../ui/badge';
import { ScrollArea } from '../../ui/scroll-area';
import LiveTailTab from '../../custom/LiveTailTab';
import DeleteConfirmDialog from '../../custom/DeleteConfirmDialog';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../ui/table';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '../../ui/tabs';
import {
  Play,
  Circle,
  Hash,
  Trash
} from 'lucide-react';
import { toast } from 'sonner';
import { decodeMessageId, seqToBase64Token, bytesToHex, beUint64ToNumber } from '../../../utils/messageUtils';
import { Pagination, PaginationContent, PaginationItem, PaginationPrevious, PaginationNext, PaginationLink } from '../../ui/pagination';
import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from '../../ui/resizable';

interface Event {
  id: string; // decoded numeric string for display
  rawIdB64?: string; // original base64 id for API deletes
  partition: number;
  timestamp: Date;
  payload: string;
  headers: Record<string, string>;
  attempts: number;
}

interface DeliveryStatus {
  group: string;
  delivered_at_ms: number;
  acknowledged: boolean;
  nacked: boolean;
  retry_count: number;
}

interface MessageDeliveryInfo {
  message_id: string;
  namespace: string;
  stream: string;
  delivery_status: DeliveryStatus[];
}

interface LiveTailViewProps {
  namespace?: string;
  stream?: string;
}

export function StreamsMessages({ namespace = 'default', stream = 'events' }: LiveTailViewProps) {
  const formatJsonPretty = (text: string): string => {
    try {
      return JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      return text;
    }
  };
  const formatXmlPretty = (xml: string): string => {
    try {
      // Normalize and insert line breaks between tags
      const reg = /(>)(<)(\/*)/g;
      let formatted = xml.replace(/\r?\n|\r/g, '').replace(reg, '$1\n$2$3');
      const lines = formatted.split('\n');
      let indent = 0;
      const pad = (n: number) => '  '.repeat(n);
      const out: string[] = [];
      for (let line of lines) {
        line = line.trim();
        if (!line) continue;
        const isClosing = /^<\//.test(line);
        const isSelfClosing = /\/\s*>$/.test(line) || /^<.*\/>$/.test(line);
        if (isClosing && indent > 0) indent -= 1;
        out.push(pad(indent) + line);
        if (!isClosing && !isSelfClosing && /^<[^!?][^>]*>/.test(line)) indent += 1;
      }
      return out.join('\n');
    } catch {
      return xml;
    }
  };
  const oneLine = (text: string): string => {
    if (!text) return '';
    try {
      // If it is JSON, compact it
      const parsed = JSON.parse(text);
      return JSON.stringify(parsed);
    } catch {
      // Collapse whitespace and remove newlines
      return text.replace(/\s+/g, ' ').trim();
    }
  };
  // Decode header b64 into flat key/value map with a friendly timestamp line
  const decodeHeaderKV = (b64?: string): Record<string, string> => {
    const out: Record<string, string> = {};
    if (!b64) return out;
    try {
      const raw = atob(b64);
      const bytes = new Uint8Array(raw.length);
      for (let i = 0; i < raw.length; i++) bytes[i] = raw.charCodeAt(i);
      // First 8 bytes: ms timestamp (already shown in Metadata); skip adding to headers list
      // bytes.subarray(0, 8) contains the timestamp
      // Remainder: JSON headers if present
      if (bytes.length > 8) {
        const rest = bytes.subarray(8);
        try {
          const text = new TextDecoder('utf-8', { fatal: false }).decode(rest);
          const obj = JSON.parse(text);
          if (obj && typeof obj === 'object') {
            for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
              out[k] = String(v);
            }
          }
        } catch {
          // ignore non-JSON remainder
        }
      }
    } catch {
      // ignore
    }
    return out;
  };
  const [isConnected, setIsConnected] = useState(false);
  const [isPaused, setIsPaused] = useState(false);
  const [events, setEvents] = useState<Event[]>([]);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null);
  const [selectedNamespace, setSelectedNamespace] = useState(namespace);
  const [selectedStream, setSelectedStream] = useState(stream);
  const [autoScroll, setAutoScroll] = useState(true);
  const [filterOpen, setFilterOpen] = useState(false);
  const [filterExpr, setFilterExpr] = useState('');

  const eventIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const sseRef = useRef<EventSource | null>(null);
  const [namespaces, setNamespaces] = useState<string[]>([]);
  const [streams, setStreams] = useState<string[]>([]);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [deliveryInfo, setDeliveryInfo] = useState<MessageDeliveryInfo | null>(null);
  const [loadingDeliveryInfo, setLoadingDeliveryInfo] = useState(false);

  // Fetch delivery status for a message
  const fetchDeliveryStatus = async (messageId: string) => {
    if (!messageId) return;

    setLoadingDeliveryInfo(true);
    try {
      const response = await fetch(`/v1/streams/message-delivery?namespace=${encodeURIComponent(selectedNamespace)}&stream=${encodeURIComponent(selectedStream)}&message_id=${encodeURIComponent(messageId)}`);
      if (response.ok) {
        const data = await response.json();
        const normalized = {
          ...data,
          delivery_status: Array.isArray(data?.delivery_status) ? data.delivery_status : [],
        } as MessageDeliveryInfo;
        setDeliveryInfo(normalized);
      } else {
        setDeliveryInfo(null);
      }
    } catch (error) {
      console.error('Failed to fetch delivery status:', error);
      setDeliveryInfo(null);
    } finally {
      setLoadingDeliveryInfo(false);
    }
  };

  // Helper: refresh historical snapshot via search API
  const refreshHistory = async (
    ns: string = selectedNamespace,
    ch: string = selectedStream,
    expr: string = filterExpr,
  ) => {
    try {
      const params = new URLSearchParams();
      params.set('namespace', ns);
      params.set('stream', ch);
      params.set('limit', '200');
      params.set('reverse', '1');
      if (expr) params.set('filter', expr);
      const res = await fetch(`/v1/streams/search?${params.toString()}`);
      const d = await res.json().catch(() => ({} as any));
      const items: any[] = (d?.records || d?.events || d?.items || []);
      const mapped: Event[] = items.map((it: any, idx: number) => {
        const rawId: string = typeof it?.id === 'string' ? it.id : '';
        const id = rawId ? decodeMessageId(rawId) : String(it?.seq ?? it?.sequence ?? idx);
        const payloadB64: string = typeof it?.payload === 'string' ? it.payload : (it?.data || '');
        let text = payloadB64 || '';
        try { text = atob(payloadB64); } catch { }
        const tsMs = typeof it?.timestamp === 'number' ? it.timestamp : (typeof it?.at_ms === 'number' ? it.at_ms : Date.now());
        const headerB64: string = typeof it?.header === 'string' ? it.header : '';
        return {
          id,
          rawIdB64: rawId || seqToBase64Token(id),
          partition: Number(it?.partition ?? 0),
          timestamp: new Date(tsMs),
          payload: text,
          headers: decodeHeaderKV(headerB64),
          attempts: Number(it?.attempts ?? 1),
        } as Event;
      });
      setEvents(mapped);
    } catch { }
  };

  // Load historical messages immediately using search API when not connected
  useEffect(() => {
    (async () => {
      if (isConnected) return;
      try {
        const params = new URLSearchParams();
        params.set('namespace', selectedNamespace);
        params.set('stream', selectedStream);
        params.set('limit', '200');
        params.set('reverse', '1');
        if (filterExpr) params.set('filter', filterExpr);
        const res = await fetch(`/v1/streams/search?${params.toString()}`);
        const d = await res.json().catch(() => ({} as any));
        const items: any[] = (d?.records || d?.events || d?.items || []);
        const mapped: Event[] = items.map((it: any, idx: number) => {
          const rawId: string = typeof it?.id === 'string' ? it.id : '';
          const id = rawId ? decodeMessageId(rawId) : String(it?.seq ?? it?.sequence ?? idx);
          const payloadB64: string = typeof it?.payload === 'string' ? it.payload : (it?.data || '');
          let text = payloadB64 || '';
          try { text = atob(payloadB64); } catch { }
          const tsMs = typeof it?.timestamp === 'number' ? it.timestamp : (typeof it?.at_ms === 'number' ? it.at_ms : Date.now());
          const headerB64: string = typeof it?.header === 'string' ? it.header : '';
          return {
            id,
            rawIdB64: rawId || seqToBase64Token(id),
            partition: Number(it?.partition ?? 0),
            timestamp: new Date(tsMs),
            payload: text,
            headers: decodeHeaderKV(headerB64),
            attempts: Number(it?.attempts ?? 1),
          } as Event;
        });
        setEvents(mapped);
        setSelected({});
      } catch {
        setEvents([]);
        setSelected({});
      }
    })();
  }, [selectedNamespace, selectedStream, filterExpr, isConnected]);

  // Ensure history is loaded on mount and whenever inputs change, regardless of connection state
  useEffect(() => {
    refreshHistory(selectedNamespace, selectedStream, filterExpr);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedNamespace, selectedStream, filterExpr]);

  // Load namespaces and streams
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch('/v1/namespaces');
        const d = await res.json().catch(() => ({}));
        const list = Array.isArray(d?.namespaces) ? (d.namespaces as string[]) : [];
        if (list.length) {
          setNamespaces(list);
          if (!list.includes(selectedNamespace)) setSelectedNamespace(list[0]);
        }
      } catch { }
    })();
  }, []);

  useEffect(() => {
    (async () => {
      try {
        if (!selectedNamespace) return;
        const res = await fetch(`/v1/streams?namespace=${encodeURIComponent(selectedNamespace)}`);
        const d = await res.json().catch(() => ({}));
        const list = Array.isArray(d?.streams) ? (d.streams as string[]) : [];
        setStreams(list);
        if (list.length && !list.includes(selectedStream)) setSelectedStream(list[0]);
      } catch {
        setStreams([]);
      }
    })();
  }, [selectedNamespace]);

  // Fetch delivery status when a message is selected
  useEffect(() => {
    if (selectedEvent?.rawIdB64) {
      fetchDeliveryStatus(selectedEvent.rawIdB64);
    } else {
      setDeliveryInfo(null);
    }
  }, [selectedEvent, selectedNamespace, selectedStream]);

  // Connect to backend SSE when connected (raw tail endpoint, no group/acks)
  useEffect(() => {
    if (isConnected && !isPaused) {
      const url = `/v1/streams/tail?namespace=${encodeURIComponent(selectedNamespace)}&stream=${encodeURIComponent(selectedStream)}&from=latest${filterExpr ? `&filter=${encodeURIComponent(filterExpr)}` : ''}`;
      const es = new EventSource(url);
      sseRef.current = es;
      es.onmessage = (e) => {
        try {
          const d = JSON.parse(e.data);
          const id: string = typeof d.id === 'string' ? decodeMessageId(d.id) : '';
          const payloadB64: string = typeof d.payload === 'string' ? d.payload : '';
          let text = payloadB64;
          try { text = atob(payloadB64); } catch { }
          const headerB64: string = typeof d?.header === 'string' ? d.header : '';
          const ev: Event = {
            id,
            rawIdB64: typeof d.id === 'string' ? d.id : seqToBase64Token(id),
            partition: 0,
            timestamp: new Date(),
            payload: text,
            headers: decodeHeaderKV(headerB64),
            attempts: 1,
          };
          setEvents(prev => [...prev, ev]);
        } catch { }
      };
      es.onerror = () => {
        es.close();
        sseRef.current = null;
        setIsConnected(false);
      };
    }
    return () => {
      if (sseRef.current) {
        sseRef.current.close();
        sseRef.current = null;
      }
    };
  }, [isConnected, isPaused, selectedNamespace, selectedStream, filterExpr]);

  // Auto-scroll to bottom
  useEffect(() => {
    if (autoScroll) {
      // Use setTimeout to ensure DOM is updated
      setTimeout(() => {
        const scrollContainer = document.querySelector('[data-radix-scroll-area-viewport]');
        if (scrollContainer) {
          scrollContainer.scrollTop = scrollContainer.scrollHeight;
        }
      }, 0);
    }
  }, [events, autoScroll]);

  const handleConnect = () => {
    setIsConnected(true);
    setEvents([]);
    setSelectedEvent(null);
    toast.success(`Connected to ${selectedNamespace}/${selectedStream}`);
  };

  const anySelected = Object.keys(selected).length > 0;
  const handleDeleteSelected = async () => {
    setShowDeleteConfirm(true);
  };
  const confirmDelete = async () => {
    const ids = events.filter(e => selected[e.id]).map(e => e.rawIdB64 || seqToBase64Token(e.id));
    if (ids.length === 0) { setShowDeleteConfirm(false); return; }
    setIsDeleting(true);
    try {
      const results = await Promise.allSettled(ids.map(idB64 => fetch(`/v1/streams/delete?namespace=${encodeURIComponent(selectedNamespace)}&stream=${encodeURIComponent(selectedStream)}&message_id=${encodeURIComponent(idB64)}`, { method: 'DELETE' })));
      const success = results.filter(r => r.status === 'fulfilled' && (r as PromiseFulfilledResult<Response>).value.ok).length;
      const failed = ids.length - success;
      if (success > 0) {
        setEvents(prev => prev.filter(e => !selected[e.id]));
        setSelected({});
      }
      if (failed === 0) toast.success(`Deleted ${success} message(s)`); else toast.error(`Deleted ${success}, failed ${failed}`);
    } catch {
      toast.error('Delete failed');
    } finally {
      setIsDeleting(false);
      setShowDeleteConfirm(false);
    }
  };

  const handleDisconnect = () => {
    setIsConnected(false);
    setIsPaused(false);
    if (eventIntervalRef.current) { clearInterval(eventIntervalRef.current); }
    if (sseRef.current) { sseRef.current.close(); sseRef.current = null; }
    toast.info('Disconnected from stream');
  };

  const handleTogglePause = () => {
    setIsPaused(prev => {
      const next = !prev;
      if (!prev) {
        // We are pausing: keep stream but stop auto scroll
        setAutoScroll(false);
      } else {
        // We are resuming: refresh history snapshot so we fill any gap
        (async () => {
          try {
            const params = new URLSearchParams();
            params.set('namespace', selectedNamespace);
            params.set('stream', selectedStream);
            params.set('limit', '200');
            params.set('reverse', '1');
            if (filterExpr) params.set('filter', filterExpr);
            const res = await fetch(`/v1/streams/search?${params.toString()}`);
            const d = await res.json().catch(() => ({} as any));
            const items: any[] = (d?.records || d?.events || d?.items || []);
            const mapped: Event[] = items.map((it: any, idx: number) => {
              const rawId: string = typeof it?.id === 'string' ? it.id : '';
              const id = rawId ? decodeMessageId(rawId) : String(it?.seq ?? it?.sequence ?? idx);
              const payloadB64: string = typeof it?.payload === 'string' ? it.payload : (it?.data || '');
              let text = payloadB64 || '';
              try { text = atob(payloadB64); } catch { }
              const tsMs = typeof it?.timestamp === 'number' ? it.timestamp : (typeof it?.at_ms === 'number' ? it.at_ms : Date.now());
              return {
                id,
                rawIdB64: rawId || seqToBase64Token(id),
                partition: Number(it?.partition ?? 0),
                timestamp: new Date(tsMs),
                payload: text,
                headers: (it?.headers as Record<string, string>) || {},
                attempts: Number(it?.attempts ?? 1),
              } as Event;
            });
            setEvents(mapped);
          } catch { }
        })();
      }
      return next;
    });
  };

  const copyEventId = (eventId: string) => {
    navigator.clipboard.writeText(eventId);
    toast.success('Event ID copied to clipboard');
  };

  const formatRelativeTime = (date: Date) => {
    const seconds = Math.floor((Date.now() - date.getTime()) / 1000);
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    return `${Math.floor(seconds / 3600)}h ago`;
  };

  return (
    <div className="h-[calc(100vh-175px)] flex flex-col">
      {/* Compact Header Toolbar */}
      <div className="border-b bg-background px-3 py-2 flex-shrink-0">
        {anySelected ? (
          <div className="flex items-center gap-2 h-[42px]">
            <span className="text-xs text-muted-foreground">{Object.keys(selected).length} selected</span>
            <Button size="sm" variant="destructive" onClick={handleDeleteSelected} className="h-7 px-2 gap-1 text-xs">
              <Trash className="w-3 h-3 text-xs" /> Delete
            </Button>
            <Button size="sm" variant="outline" onClick={() => setSelected({})} className="h-7 px-2 text-xs">Cancel</Button>
          </div>
        ) : (
          <LiveTailTab
            defaultNamespace={selectedNamespace}
            defaultStream={selectedStream}
            onStart={(ns, ch) => {
              setSelectedNamespace(ns);
              setSelectedStream(ch);
              handleConnect();
            }}
            onToggle={(running, ns, ch) => {
              if (!running) {
                // paused: load history snapshot
                refreshHistory(ns, ch, filterExpr);
              }
            }}
            onFilterApply={(expr) => {
              setFilterExpr(expr);
              refreshHistory(selectedNamespace, selectedStream, expr);
            }}
          />
        )}
      </div>

      {/* Message Stream */}
      <div className="flex-1 min-h-0">
        <ResizablePanelGroup direction="horizontal" className="h-full w-full">
          <ResizablePanel defaultSize={70} minSize={40}>
            <div className="h-full min-h-0 flex flex-col">
              {events.length > 0 ? (
                <ScrollArea className="flex-1 min-h-0 max-h-[calc(100vh-150px)]">
                  <Table className="border-b-1 w-full [&_*]:align-top [&_td]:border-l [&_th]:border-l [&_td:first-child]:border-l-0 [&_th:first-child]:border-l-0 [&_tr]:border-b" style={{ tableLayout: 'fixed', width: '100%' }}>
                    <TableHeader>
                      <TableRow className="bg-muted/40 border-b]">
                        <TableHead className="w-[28px] h-[28px] p-2 text-xs">
                          <input
                            type="checkbox"
                            style={{ marginTop: '3px' }}
                            className="h-3 w-3 bg border accent-gray-900 border-gray-300 text-xs"
                            checked={events.length > 0 && Object.keys(selected).length === events.length}
                            onChange={(e) => {
                              if (e.target.checked) {
                                const all: Record<string, boolean> = {};
                                for (const ev of events) all[ev.id] = true;
                                setSelected(all);
                              } else {
                                setSelected({});
                              }
                            }}
                          />
                        </TableHead>
                        <TableHead className="w-[80px] p-2 text-xs uppercase font-light">ID</TableHead>
                        <TableHead className="p-2 text-xs uppercase font-light">Payload</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {events.map((event) => (
                        <TableRow
                          key={event.id}
                          className={`text-xs border-b ${selectedEvent?.id === event.id ? 'bg-muted' : ''}`}
                          onClick={() => setSelectedEvent(event)}
                        >
                          <TableCell className="p-2">
                            <input
                              type="checkbox"
                              style={{ marginTop: '1px' }}
                              className="h-3 w-3 border-gray-300 hover:accent-gray-800 accent-gray-900 text-xs"
                              checked={!!selected[event.id]}
                              onChange={(e) => {
                                setSelected(prev => {
                                  const copy = { ...prev };
                                  if (e.target.checked) copy[event.id] = true; else delete copy[event.id];
                                  return copy;
                                });
                              }}
                            />
                          </TableCell>
                          <TableCell className="p-2 font-mono text-[11px] truncate w-[80px]" title={event.id}>{event.id}</TableCell>
                          <TableCell className="p-2 font-mono truncate whitespace-nowrap overflow-hidden" title={event.payload}>{oneLine(event.payload)}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </ScrollArea>
              ) : (
                <div className="flex-1 flex items-center justify-center text-center">
                  <div>
                    {isConnected ? (
                      <>
                        <Circle className="w-8 h-8 text-muted-foreground mx-auto mb-2 animate-pulse" />
                        <p className="text-muted-foreground text-sm">Listening for events...</p>
                        <p className="text-xs text-muted-foreground mt-1">
                          Events will appear here as they arrive
                        </p>
                      </>
                    ) : (
                      <>
                        <Play className="w-8 h-8 text-muted-foreground mx-auto mb-2" />
                        <p className="text-muted-foreground">Connect to start streaming</p>
                        <p className="text-sm text-muted-foreground mt-1">
                          Configure your stream and click Start
                        </p>
                      </>
                    )}
                  </div>
                </div>
              )}
              {/* Pagination Row */}
              <div className="border-t bg-background px-3 py-2 flex items-center justify-between min-h-[42px]">
              <Pagination className="justify-end">
                <PaginationContent>
                  <PaginationItem>
                    <PaginationPrevious href="#" className="text-xs" />
                  </PaginationItem>
                  <PaginationItem>
                    <PaginationLink href="#" isActive size="sm" className="text-xs w-6 h-6">
                      1
                    </PaginationLink>
                  </PaginationItem>
                  <PaginationItem>
                    <PaginationNext href="#" className="text-xs" />
                  </PaginationItem>
                </PaginationContent>
              </Pagination>
              </div>
            </div>
          </ResizablePanel>
          <ResizableHandle withHandle />
          <ResizablePanel defaultSize={30} minSize={20}>
            <div className="h-full min-h-0 flex flex-col">
              <Card className="flex-1 min-h-0 rounded-none border-0 bg-muted/30 w-full flex flex-col">
                <CardContent className="p-0 flex-1 min-h-0 flex flex-col">
                  {selectedEvent ? (
                    <ScrollArea className="flex-1 min-h-0">
                      <div className="space-y-0">
                        <div className="p-4 hover:bg-muted/90">
                          <h4 className="font-medium mb-2 text-xs text-muted-foreground">Metadata</h4>
                          <div className="space-y-2 text-sm">
                            <div className="flex justify-between">
                              <span className="text-muted-foreground text-xs">ID:</span>
                              <span className="font-mono text-xs">{selectedEvent.id}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground text-xs">Partition:</span>
                              <span>{selectedEvent.partition}</span>
                            </div>
                            {/* Sequence omitted since ID already encodes it */}
                            <div className="flex justify-between">
                              <span className="text-muted-foreground text-xs">Timestamp:</span>
                              <span className="text-xs">{selectedEvent.timestamp.toLocaleString()}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground text-xs">Attempts:</span>
                              <span>{selectedEvent.attempts}</span>
                            </div>
                          </div>
                        </div>
                        <div className="border-t" />

                        <div className="p-4 hover:bg-muted/90">
                          <h4 className="font-medium mb-2 text-xs text-muted-foreground">Headers</h4>
                          {Object.keys(selectedEvent.headers || {}).length === 0 ? (
                            <div className="text-xs text-muted-foreground">No header information available</div>
                          ) : (
                            <div className="space-y-1">
                              {Object.entries(selectedEvent.headers).map(([key, value]) => (
                                <div key={key} className="text-xs">
                                  <span className="text-muted-foreground">{key}:</span>
                                  <span className="ml-2 font-mono">{value}</span>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>

                        <div className="border-t" />

                        <div className="p-4 hover:bg-muted/90">
                          <Tabs defaultValue="json" className="w-full">
                            <div className="flex items-center justify-between mb-2">
                              <h4 className="font-medium text-xs text-muted-foreground">Payload</h4>
                              <TabsList className="h-7">
                                <TabsTrigger value="json" className="text-xs px-2">JSON</TabsTrigger>
                                <TabsTrigger value="text" className="text-xs px-2">Text</TabsTrigger>
                                <TabsTrigger value="xml" className="text-xs px-2">XML</TabsTrigger>
                              </TabsList>
                            </div>
                            <TabsContent value="json">
                              <pre className="text-xs rounded overflow-auto font-mono max-h-[50vh] whitespace-pre-wrap break-words">
                                {formatJsonPretty(selectedEvent.payload)}
                              </pre>
                            </TabsContent>
                            <TabsContent value="text">
                              <pre className="text-xs rounded overflow-auto font-mono max-h-[50vh] whitespace-pre-wrap break-words">
                                {selectedEvent.payload}
                              </pre>
                            </TabsContent>
                            <TabsContent value="xml">
                              <pre className="text-xs rounded overflow-auto font-mono max-h-[50vh] whitespace-pre-wrap break-words">
                                {formatXmlPretty(selectedEvent.payload)}
                              </pre>
                            </TabsContent>
                          </Tabs>
                        </div>

                        <div className="border-t" />

                        <div className="p-4 hover:bg-muted/90">
                          <h4 className="font-medium mb-2 text-xs text-muted-foreground">Delivery Status</h4>
                          {loadingDeliveryInfo ? (
                            <div className="text-xs text-muted-foreground">Loading delivery status...</div>
                          ) : (Array.isArray(deliveryInfo?.delivery_status) && deliveryInfo!.delivery_status.length > 0) ? (
                            <div className="border rounded">
                              <Table>
                                <TableHeader>
                                  <TableRow className="bg-muted/40">
                                    <TableHead className="text-xs font-medium w-[150px]">Group</TableHead>
                                    <TableHead className="text-xs font-medium w-[180px]">Delivered</TableHead>
                                    <TableHead className="text-xs font-medium w-[80px]">Ack/Nack</TableHead>
                                    <TableHead className="text-xs font-medium w-[50px]">Retried</TableHead>
                                  </TableRow>
                                </TableHeader>
                                <TableBody>
                                  {(deliveryInfo?.delivery_status || []).map((status, index) => (
                                    <TableRow key={index} className="text-xs">
                                      <TableCell className="font-mono w-[150px]">{status.group}</TableCell>
                                      <TableCell className="w-[180px]">{new Date(status.delivered_at_ms).toLocaleString()}</TableCell>
                                      <TableCell className="w-[80px]">
                                        <Badge
                                          variant={status.nacked ? "destructive" : status.acknowledged ? "default" : "secondary"}
                                          className="text-[10px]"
                                        >
                                          {status.nacked ? "Nacked" : status.acknowledged ? "Acked" : "Pending"}
                                        </Badge>
                                      </TableCell>
                                      <TableCell className="w-[50px]">{status.retry_count}</TableCell>
                                    </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            </div>
                          ) : (
                            <div className="text-xs text-muted-foreground">No delivery information available</div>
                          )}
                        </div>
                      </div>
                    </ScrollArea>
                  ) : (
                    <div className="flex-1 min-h-0 flex items-center justify-center text-center">
                      <div>
                        <Hash className="w-8 h-8 text-muted-foreground mx-auto mb-2" />
                        <p className="text-muted-foreground text-sm">No message selected</p>
                        <p className="text-sm text-muted-foreground mt-1">
                          Click on a message to view details
                        </p>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          </ResizablePanel>
        </ResizablePanelGroup>

      </div>
      <DeleteConfirmDialog
        open={showDeleteConfirm}
        selectedCount={Object.keys(selected).length}
        onCancel={() => setShowDeleteConfirm(false)}
        onConfirm={confirmDelete}
        loading={isDeleting}
        title="Delete Selected Messages"
        description="This will permanently delete the selected messages."
      />
    </div>
  );
}


