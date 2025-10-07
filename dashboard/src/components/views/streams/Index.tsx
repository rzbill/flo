import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Button } from '../../ui/button';
import { Input } from '../../ui/input';
import { 
  Select, 
  SelectContent, 
  SelectItem, 
  SelectTrigger, 
  SelectValue 
} from '../../ui/select';
import { 
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
  SheetFooter
} from '../../ui/sheet';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../ui/table';
import { Search, Plus, ArrowUpDown } from 'lucide-react';
import { toast } from 'sonner';
import { useStreams, useCreateStream, useNamespaces, Stream } from '../../../hooks/useStreams';
import { useFormatting } from '../../../hooks/useFormatting';
import { useAppStore } from '../../../store/app';
import { fetchJSON } from '../../../lib/api';

interface StreamsViewProps {
  onNavigate: (view: import('../../../types/navigation').View, namespace?: string, stream?: string, tab?: string) => void;
}

export const StreamsView = React.memo(function StreamsView({ onNavigate }: StreamsViewProps) {
  const [searchParams] = useSearchParams();
  const urlNamespace = searchParams.get('namespace') || 'default';
  const { namespace, setNamespace } = useAppStore();
  
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [newStreamName, setNewStreamName] = useState('');
  const [newStreamPartitions, setNewStreamPartitions] = useState('4');
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedNamespace, setSelectedNamespace] = useState(urlNamespace || namespace);
  const [sortBy, setSortBy] = useState<'name' | 'messages' | 'groups' | 'last'>('last');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  const [namespaces, setNamespaces] = useState(['all', 'default'] as string[]);
  const { data: nsData } = useNamespaces();

  // Use custom hooks
  const { formatNumber, formatBytes, formatRelativeTime } = useFormatting();
  
  // TanStack Query hooks
  const { 
    data: streamsData, 
    isLoading: streamsLoading, 
    error: streamsError 
  } = useStreams(selectedNamespace);
  
  const createStreamMutation = useCreateStream();

  // Memoized values
  const streams = useMemo(() => {
    if (!streamsData?.streams) return [];
    if (!Array.isArray(streamsData.streams)) return [];
    
    // Convert string array to Stream objects
    return streamsData.streams.map((name: string): Stream => ({
      name,
      partitions: 4, // Default partitions, we'll need to get this from stats
      namespace: selectedNamespace
    }));
  }, [streamsData, selectedNamespace]);
  
  const streamKey = useCallback((ns: string, ch: string) => `${ns}:${ch}`, []);

  // Load namespaces via hook
  useEffect(() => {
    const list = Array.isArray(nsData?.namespaces) ? (nsData!.namespaces as string[]) : [];
    if (list.length) setNamespaces((['all', ...list] as string[]));
  }, [nsData]);

  // Sync selected namespace to global store
  useEffect(() => {
    setNamespace(selectedNamespace);
  }, [selectedNamespace, setNamespace]);

  const filteredStreams = useMemo(() => {
    return streams.filter(stream => {
      if (!stream?.name || typeof stream.name !== 'string') return false;
      if (!searchTerm) return true;
      return stream.name.toLowerCase().includes(searchTerm.toLowerCase());
    });
  }, [streams, searchTerm]);

  const [metrics, setMetrics] = React.useState({ totalMessages: 0, totalBytes: 0, streamsCount: 0, activeSubscribers: 0 });
  const [streamStats, setStreamStats] = React.useState<Record<string, { count: number; bytes: number; lastPublishMs?: number; lastDeliveredMs?: number; groupsCount?: number }>>({});

  const sortedStreams = React.useMemo(() => {
    const arr = [...filteredStreams];
    const getLastMs = (c: Stream) => streamStats[streamKey(c.namespace, c.name)]?.lastPublishMs || 0;
    const getCount = (c: Stream) => streamStats[streamKey(c.namespace, c.name)]?.count || 0;
    const getGroups = (c: Stream) => streamStats[streamKey(c.namespace, c.name)]?.groupsCount || 0;
    arr.sort((a, b) => {
      let av: number | string = 0;
      let bv: number | string = 0;
      if (sortBy === 'last') { av = getLastMs(a); bv = getLastMs(b); }
      else if (sortBy === 'messages') { av = getCount(a); bv = getCount(b); }
      else if (sortBy === 'groups') { av = getGroups(a); bv = getGroups(b); }
      else { av = a.name; bv = b.name; }
      let cmp = 0;
      if (typeof av === 'string' && typeof bv === 'string') cmp = av.localeCompare(bv);
      else cmp = (av as number) - (bv as number);
      return sortDir === 'asc' ? cmp : -cmp;
    });
    return arr;
  }, [filteredStreams, streamStats, sortBy, sortDir, streamKey]);

  const handleCreateStream = useCallback(async () => {
    if (!newStreamName.trim()) return;
    
    try {
      await createStreamMutation.mutateAsync({
        namespace: selectedNamespace,
        stream: newStreamName.trim(),
        partitions: parseInt(newStreamPartitions)
      });
      toast.success(`Stream "${newStreamName.trim()}" created`);
      setShowCreateModal(false);
      setNewStreamName('');
      setNewStreamPartitions('4');
    } catch (error) {
      toast.error('Failed to create stream');
    }
  }, [newStreamName, selectedNamespace, newStreamPartitions, createStreamMutation]);

  const handleRowOpen = useCallback((stream: Stream) => {
    onNavigate('stream-details', stream.namespace, stream.name, 'messages');
  }, [onNavigate]);

  const handleSort = useCallback((column: 'name' | 'messages' | 'groups' | 'last') => {
    if (sortBy === column) {
      setSortDir(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(column);
      setSortDir('desc');
    }
  }, [sortBy]);

  // Load stream stats
  useEffect(() => {
    if (streams.length === 0) return;
    
    const loadStats = async () => {
      const stats: Record<string, any> = {};
      let totalMessages = 0;
      let totalBytes = 0;
      let activeSubscribers = 0;

      for (const stream of streams) {
        try {
          const data = await fetchJSON<any>(`/v1/streams/stats?namespace=${encodeURIComponent(stream.namespace)}&stream=${encodeURIComponent(stream.name)}`);
          const key = streamKey(stream.namespace, stream.name);
          stats[key] = {
            count: data.total_count || 0,
            bytes: data.total_bytes || 0,
            lastPublishMs: data.last_publish_ms,
            lastDeliveredMs: data.last_delivered_ms,
            groupsCount: data.groups_count || 0,
          };
          totalMessages += data.total_count || 0;
          totalBytes += data.total_bytes || 0;
          activeSubscribers += data.active_subscribers || 0;
        } catch {}
      }

      setStreamStats(stats);
      setMetrics({ totalMessages, totalBytes, streamsCount: streams.length, activeSubscribers });
    };

    loadStats();
  }, [streams, streamKey]);

  if (streamsLoading) {
    return (
      <div className="p-6 space-y-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-muted-foreground">Loading streams...</div>
        </div>
      </div>
    );
  }

  if (streamsError) {
    return (
      <div className="p-6 space-y-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-red-600">
            Failed to load streams: {streamsError instanceof Error ? streamsError.message : 'Unknown error'}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-4 mx-auto lg:max-w-3xl xl:max-w-4xl">
      {/* Header */}
      <div className="flex items-end justify-between py-6 my-12 border-b-1">
        <h1 className="text-2xl md:text-3xl font-light">Streams</h1>
        <div className="flex gap-8 md:gap-10">
          <div>
            <div className="uppercase text-[11px] tracking-wide text-muted-foreground mb-1">Total Messages</div>
            <div className="text-2xl font-light leading-none">{formatNumber(metrics.totalMessages)}</div>
          </div>
          <div>
            <div className="uppercase text-[11px] tracking-wide text-muted-foreground mb-1">Total Bytes</div>
            <div className="text-2xl font-light leading-none">{formatBytes(metrics.totalBytes)}</div>
          </div>
          <div>
            <div className="uppercase text-[11px] tracking-wide text-muted-foreground mb-1">Streams</div>
            <div className="text-2xl font-light leading-none">{formatNumber(metrics.streamsCount)}</div>
          </div>
          <div>
            <div className="uppercase text-[11px] tracking-wide text-muted-foreground mb-1">Active Subscribers</div>
            <div className="text-2xl font-light leading-none">{formatNumber(metrics.activeSubscribers)}</div>
          </div>
        </div>
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-3">
        <div className="shrink-0">
          <Select value={selectedNamespace} onValueChange={setSelectedNamespace}>
            <SelectTrigger className="w-[180px] h-8 hover:bg-input-background active:bg-input data-[state=open]:bg-input">
              <span className="flex items-center gap-1 text-xs leading-none">
                <span className="text-muted-foreground">namespace</span>
                <span className="inline-block w-1" aria-hidden />
                <SelectValue />
              </span>
            </SelectTrigger>
            <SelectContent>
              {namespaces.map(ns => (
                <SelectItem key={ns} value={ns} className="text-xs">
                  {ns}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex-1 max-w-[260px]">
          <div className="relative ">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground w-4 h-4" />
            <Input
              placeholder="Search streams..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10 h-8 text-xs placeholder:text-xs"
            />
          </div>
        </div>
        <div className="ml-auto flex items-center gap-2">
        <Button
          onClick={() => setShowCreateModal(true)}
          className="h-8 px-3 text-xs gap-1.5"
        >
          <Plus className="w-3.5 h-3.5" />
          Create Stream
        </Button>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto rounded-lg border border-gray-200">
        <Table className="table-fixed w-full">
          <TableHeader>
            <TableRow className="bg-muted/40">
              <TableHead className="w-[200px] p-4 text-xs uppercase font-light">
                <button
                  onClick={() => handleSort('name')}
                  className="flex uppercase items-center gap-1 hover:text-foreground"
                >
                  Name
                  <ArrowUpDown className={`w-3 h-3 ${sortBy != 'name' ? 'hidden' : ''}`} />
                </button>
              </TableHead>
              <TableHead className="w-[100px] p-4 text-xs uppercase font-light">
                <button
                  onClick={() => handleSort('messages')}
                  className="flex uppercase  items-center gap-1 hover:text-foreground"
                >
                  Messages
                  <ArrowUpDown className={`w-3 h-3 ${sortBy != 'messages' ? 'hidden' : ''}`} />
                </button>
              </TableHead>
              <TableHead className="w-[100px] p-4 text-xs uppercase font-light">
                <button
                  onClick={() => handleSort('groups')}
                  className="flex uppercase items-center gap-1 hover:text-foreground"
                >
                  Groups
                  <ArrowUpDown className={`w-3 h-3 ${sortBy != 'groups' ? 'hidden' : ''}`} />
                </button>
              </TableHead>
              <TableHead className="w-[200px] p-4 text-xs uppercase font-light">
                <button
                  onClick={() => handleSort('last')}
                  className="flex uppercase items-center gap-1 hover:text-foreground"
                >
                  Last Activity
                  <ArrowUpDown className={`w-3 h-3 ${sortBy != 'last' ? 'hidden' : ''}`} />
                </button>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {sortedStreams.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="text-center py-8 text-muted-foreground">
                  No streams found
                </TableCell>
              </TableRow>
            ) : (
              sortedStreams.map((stream) => {
              const stats = streamStats[streamKey(stream.namespace, stream.name)] || {};
              return (
                <TableRow 
                  key={streamKey(stream.namespace, stream.name)} 
                  className="text-xs hover:bg-muted/20 cursor-pointer"
                  onClick={() => handleRowOpen(stream)}
                >
                  <TableCell className="p-4 font-medium">
                    <div className="flex items-center gap-2">
                      {stream.name}
                    </div>
                  </TableCell>
                  <TableCell className="p-4 text-muted-foreground">
                    {formatNumber((stats as any).count || 0)}
                  </TableCell>
                  <TableCell className="p-4 text-muted-foreground">
                    {formatNumber((stats as any).groupsCount || 0)}
                  </TableCell>
                  <TableCell className="p-4 text-muted-foreground">
                    {(() => {
                      const pub = (stats as any).lastPublishMs ? formatRelativeTime((stats as any).lastPublishMs) : '–';
                      const del = (stats as any).lastDeliveredMs ? formatRelativeTime((stats as any).lastDeliveredMs) : '–';
                      return (
                        <div className="flex flex-wrap items-center gap-4">
                          <div>
                            <span className="uppercase text-[10px] mr-1">Published</span>
                            <span className="text-foreground">{pub}</span>
                          </div>
                          <div>
                            <span className="uppercase text-[10px] mr-1">Delivered</span>
                            <span className="text-foreground">{del}</span>
                          </div>
                        </div>
                      );
                    })()}
                  </TableCell>
                </TableRow>
              );
            })
            )}
          </TableBody>
        </Table>
      </div>

      {/* Create Stream Sheet (Right Panel) */}
      <Sheet open={showCreateModal} onOpenChange={setShowCreateModal}>
        <SheetContent side="right" className="sm:max-w-md">
          <SheetHeader>
            <SheetTitle>Create Stream</SheetTitle>
            <SheetDescription>
              Create a new stream in the {selectedNamespace} namespace.
            </SheetDescription>
          </SheetHeader>
          <div className="space-y-4 py-4">
            <div>
              <label className="block text-sm font-medium mb-2">Stream Name</label>
              <Input
                placeholder="Enter stream name"
                value={newStreamName}
                onChange={(e) => setNewStreamName(e.target.value)}
                className="text-sm"
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-2">Partitions</label>
              <Select value={newStreamPartitions} onValueChange={setNewStreamPartitions}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1">1</SelectItem>
                  <SelectItem value="2">2</SelectItem>
                  <SelectItem value="4">4</SelectItem>
                  <SelectItem value="8">8</SelectItem>
                  <SelectItem value="16">16</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          <SheetFooter>
            <Button
              variant="outline"
              onClick={() => setShowCreateModal(false)}
              disabled={createStreamMutation.isPending}
            >
              Cancel
            </Button>
            <Button
              onClick={handleCreateStream}
              disabled={!newStreamName.trim() || createStreamMutation.isPending}
            >
              {createStreamMutation.isPending ? 'Creating...' : 'Create Stream'}
            </Button>
          </SheetFooter>
        </SheetContent>
      </Sheet>
    </div>
  );
});