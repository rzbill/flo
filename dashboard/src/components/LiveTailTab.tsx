import React, { useState, useEffect, useRef } from 'react';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card';
import { Badge } from './ui/badge';
import { ScrollArea } from './ui/scroll-area';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table';
import { 
  Play, 
  Pause, 
  Copy, 
  Check, 
  X, 
  ArrowDown, 
  Circle,
  Hash
} from 'lucide-react';
import { toast } from 'sonner';

interface Event {
  id: string;
  partition: number;
  sequence: number;
  timestamp: Date;
  payload: string;
  headers: Record<string, string>;
  attempts: number;
  acked?: boolean;
  nacked?: boolean;
}

interface LiveTailTabProps {
  namespace: string;
  stream: string;
}

export function LiveTailTab({ namespace, stream }: LiveTailTabProps) {
  const [isConnected, setIsConnected] = useState(false);
  const [isPaused, setIsPaused] = useState(false);
  const [events, setEvents] = useState<Event[]>([]);
  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null);
  const [consumerGroup, setConsumerGroup] = useState('ui');
  const [autoScroll, setAutoScroll] = useState(true);
  const [role, setRole] = useState<'service' | 'user'>('service');
  const [filterQuery, setFilterQuery] = useState('');
  
  const eventIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const sseRef = useRef<EventSource | null>(null);

  // Subscribe to SSE when connected
  useEffect(() => {
    if (isConnected && !isPaused) {
      const url = `/v1/streams/subscribe?namespace=${encodeURIComponent(namespace)}&stream=${encodeURIComponent(stream)}&group=${encodeURIComponent(consumerGroup)}`;
      const es = new EventSource(url);
      sseRef.current = es;
      es.onmessage = (e) => {
        try {
          const d = JSON.parse(e.data);
          const id: string = typeof d.id === 'string' ? d.id : '';
          const payloadB64: string = typeof d.payload === 'string' ? d.payload : '';
          const text = (() => { try { return atob(payloadB64); } catch { return payloadB64; } })();
          const ev: Event = {
            id,
            partition: 0,
            sequence: Date.now(),
            timestamp: new Date(),
            payload: text,
            headers: {},
            attempts: 1,
          };
          setEvents(prev => [...prev, ev]);
        } catch {}
      };
      es.onerror = () => {
        // Auto close on error; UI can restart
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
  }, [isConnected, isPaused, namespace, stream, consumerGroup]);

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
    toast.success(`Connected to ${namespace}/${stream}`);
  };

  const handleDisconnect = () => {
    setIsConnected(false);
    setIsPaused(false);
    if (sseRef.current) {
      sseRef.current.close();
      sseRef.current = null;
    }
    if (eventIntervalRef.current) {
      clearInterval(eventIntervalRef.current);
    }
    toast.info('Disconnected from stream');
  };

  const handleTogglePause = () => {
    setIsPaused(!isPaused);
    if (!isPaused) {
      setAutoScroll(false);
    }
  };

  const handleAck = async (event: Event) => {
    try {
      await fetch('/v1/streams/ack', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, stream, group: consumerGroup, id: event.id })
      });
      setEvents(prev => prev.map(e => e.id === event.id ? { ...e, acked: true } : e));
      toast.success('Event acknowledged');
    } catch {
      toast.error('Ack failed');
    }
  };

  const handleNack = async (event: Event) => {
    try {
      await fetch('/v1/streams/nack', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace, stream, group: consumerGroup, id: event.id })
      });
      setEvents(prev => prev.map(e => e.id === event.id ? { ...e, nacked: true } : e));
      toast.info('Event nacked - scheduled for retry');
    } catch {
      toast.error('Nack failed');
    }
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

  const connectionStatus = isConnected ? (isPaused ? 'paused' : 'live') : 'disconnected';
  const statusColors = {
    live: 'bg-green-500',
    paused: 'bg-yellow-500',
    disconnected: 'bg-gray-500'
  };

  // Toolbar styling
  return (
    <div className="space-y-4">
      {/* Toolbar */}
      <div className="border rounded-md p-3 bg-background">
        <div className="flex flex-wrap items-center gap-2">
          <div className="text-xs text-muted-foreground">Join a stream</div>
          <div className="text-xs bg-muted/50 rounded px-2 py-1">{namespace}/{stream}</div>
          <div className="w-px h-4 bg-border mx-1" />
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground">Role</span>
            <Button variant="outline" size="sm" className="h-7 px-2 text-xs" onClick={() => setRole(r => r === 'service' ? 'user' : 'service')}>
              {role} role
            </Button>
          </div>
          <div className="w-px h-4 bg-border mx-1" />
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground">Group</span>
            <Input value={consumerGroup} onChange={(e) => setConsumerGroup(e.target.value)} disabled={isConnected} className="h-7 text-xs w-[140px]" />
          </div>
          <div className="w-px h-4 bg-border mx-1" />
          <div className="flex items-center gap-2">
            <Input placeholder="Filter messages" value={filterQuery} onChange={(e) => setFilterQuery(e.target.value)} className="h-7 text-xs w-[200px]" />
          </div>
          <div className="ml-auto flex items-center gap-2">
            <div className="flex items-center gap-2 mr-2">
              <div className={`w-2 h-2 rounded-full ${statusColors[connectionStatus]}`} />
              <span className="text-xs capitalize text-muted-foreground">{connectionStatus}</span>
            </div>
            {!isConnected ? (
              <Button onClick={handleConnect} className="gap-2 h-7 text-xs">
                <Play className="w-3 h-3" />
                Start listening
              </Button>
            ) : (
              <>
                <Button variant="outline" onClick={handleTogglePause} className="gap-2 h-7 text-xs">
                  {isPaused ? (
                    <>
                      <Play className="w-3 h-3" />
                      Resume
                    </>
                  ) : (
                    <>
                      <Pause className="w-3 h-3" />
                      Pause
                    </>
                  )}
                </Button>
                <Button variant="outline" onClick={handleDisconnect} className="h-7 text-xs">Stop</Button>
              </>
            )}
          </div>
        </div>
      </div>

      {/* Events Stream */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 min-h-[500px]">
        {/* Events List */}
        <div className="lg:col-span-2">
          <Card className="h-full flex flex-col">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Event Stream</CardTitle>
            </CardHeader>
            <CardContent className="flex-1 min-h-0 p-0">
              {events.length > 0 ? (
                <ScrollArea className="h-full">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-[100px]">Time</TableHead>
                        <TableHead className="w-[80px]">Partition</TableHead>
                        <TableHead className="w-[100px]">Sequence</TableHead>
                        <TableHead>Payload</TableHead>
                        <TableHead className="w-[100px]">Status</TableHead>
                        <TableHead className="w-[120px]">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {events.filter(e => !filterQuery || e.payload.toLowerCase().includes(filterQuery.toLowerCase())).map((event) => (
                        <TableRow
                          key={event.id}
                          onClick={() => setSelectedEvent(event)}
                          className={`cursor-pointer hover:bg-accent/50 ${
                            selectedEvent?.id === event.id ? 'bg-accent' : ''
                          } ${
                            event.acked ? 'opacity-60' : ''
                          }`}
                        >
                          <TableCell className="font-medium">
                            <div className="flex items-center gap-2">
                              <Circle className="w-2 h-2 fill-current text-blue-500" />
                              <span className="text-xs">
                                {formatRelativeTime(event.timestamp)}
                              </span>
                            </div>
                          </TableCell>
                          <TableCell>
                            <Badge variant="outline" className="text-xs">
                              P{event.partition}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-xs text-muted-foreground">
                            #{event.sequence}
                          </TableCell>
                          <TableCell>
                            <div className="font-mono text-xs max-w-[300px] truncate">
                              {event.payload}
                            </div>
                          </TableCell>
                          <TableCell>
                            {(event.acked || event.nacked) && (
                              <Badge 
                                variant={event.acked ? "default" : "secondary"}
                                className="text-xs"
                              >
                                {event.acked ? 'ACK' : 'NACK'}
                              </Badge>
                            )}
                          </TableCell>
                          <TableCell>
                            <div className="flex gap-1">
                              <Button
                                size="sm"
                                variant="ghost"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleAck(event);
                                }}
                                disabled={event.acked || event.nacked}
                                className="h-6 w-6 p-0"
                                title="Acknowledge"
                              >
                                <Check className="w-3 h-3" />
                              </Button>
                              <Button
                                size="sm"
                                variant="ghost"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleNack(event);
                                }}
                                disabled={event.acked || event.nacked}
                                className="h-6 w-6 p-0"
                                title="Negative Acknowledge"
                              >
                                <X className="w-3 h-3" />
                              </Button>
                              <Button
                                size="sm"
                                variant="ghost"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  copyEventId(event.id);
                                }}
                                className="h-6 w-6 p-0"
                                title="Copy Event ID"
                              >
                                <Copy className="w-3 h-3" />
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </ScrollArea>
              ) : (
                <div className="h-full flex items-center justify-center text-center p-8">
                  <div>
                    {isConnected ? (
                      <>
                        <Circle className="w-8 h-8 text-muted-foreground mx-auto mb-2 animate-pulse" />
                        <p className="text-muted-foreground">Listening for events...</p>
                        <p className="text-sm text-muted-foreground mt-1">
                          Events will appear here as they arrive
                        </p>
                      </>
                    ) : (
                      <>
                        <Play className="w-8 h-8 text-muted-foreground mx-auto mb-2" />
                        <p className="text-muted-foreground">Connect to start streaming</p>
                        <p className="text-sm text-muted-foreground mt-1">
                          Click Start to begin receiving events
                        </p>
                      </>
                    )}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Event Inspector */}
        <div>
          <Card className="h-full">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg">Event Inspector</CardTitle>
              <CardDescription>
                {selectedEvent ? 'Event details and metadata' : 'Select an event to inspect'}
              </CardDescription>
            </CardHeader>
            <CardContent>
              {selectedEvent ? (
                <ScrollArea className="h-[400px]">
                  <div className="space-y-4">
                    <div>
                      <h4 className="font-medium mb-2">Metadata</h4>
                      <div className="space-y-2 text-sm">
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">Event ID:</span>
                          <span className="font-mono text-xs">{selectedEvent.id}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">Partition:</span>
                          <span>{selectedEvent.partition}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">Sequence:</span>
                          <span>{selectedEvent.sequence}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">Timestamp:</span>
                          <span>{selectedEvent.timestamp.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">Attempts:</span>
                          <span>{selectedEvent.attempts}</span>
                        </div>
                      </div>
                    </div>

                    <div>
                      <h4 className="font-medium mb-2">Headers</h4>
                      <div className="space-y-1">
                        {Object.entries(selectedEvent.headers).map(([key, value]) => (
                          <div key={key} className="text-xs">
                            <span className="text-muted-foreground">{key}:</span>
                            <span className="ml-2 font-mono">{value}</span>
                          </div>
                        ))}
                      </div>
                    </div>

                    <div>
                      <h4 className="font-medium mb-2">Payload</h4>
                      <pre className="text-xs bg-muted/50 rounded p-3 overflow-auto font-mono">
                        {JSON.stringify(JSON.parse(selectedEvent.payload), null, 2)}
                      </pre>
                    </div>
                  </div>
                </ScrollArea>
              ) : (
                <div className="h-[400px] flex items-center justify-center text-center">
                  <div>
                    <Hash className="w-8 h-8 text-muted-foreground mx-auto mb-2" />
                    <p className="text-muted-foreground">No event selected</p>
                    <p className="text-sm text-muted-foreground mt-1">
                      Click on an event to view details
                    </p>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}