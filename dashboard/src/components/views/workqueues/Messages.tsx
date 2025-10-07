import React, { useState, useMemo } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../ui/card';
import { Button } from '../../ui/button';
import { Input } from '../../ui/input';
import { Badge } from '../../ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../ui/tabs';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '../../ui/select';
import { PackageCheck, PackageOpen, PackageX, Search, FileText, Clock, Users } from 'lucide-react';
import {
  useReadyMessages,
  usePending,
  useCompletedMessages,
  useWorkQueueGroups
} from '../../../hooks/useWorkQueues';
import { useFormatting } from '../../../hooks/useFormatting';

interface MessagesProps {
  namespace: string;
  workqueue: string;
}

type MessageState = 'ready' | 'pending' | 'completed';

export default function Messages({ namespace, workqueue }: MessagesProps) {
  const [activeTab, setActiveTab] = useState<MessageState>('ready');
  const [selectedGroup, setSelectedGroup] = useState<string>('default');
  const [searchTerm, setSearchTerm] = useState('');
  const [limit] = useState(100);
  const { formatNumber, formatRelativeTime, formatBytes } = useFormatting();

  // Fetch groups
  const { data: groupsData } = useWorkQueueGroups(namespace, workqueue);
  const groups = useMemo(() => groupsData?.groups || ['default'], [groupsData]);

  // Fetch data for each state
  const { data: readyData, isLoading: readyLoading } = useReadyMessages(namespace, workqueue, 0, limit, true);
  const { data: pendingData, isLoading: pendingLoading } = usePending(namespace, workqueue, selectedGroup, limit);
  const { data: completedData, isLoading: completedLoading } = useCompletedMessages(namespace, workqueue, selectedGroup, 0, limit);

  // Get counts for state tabs
  const readyCount = readyData?.messages?.length || 0;
  const pendingCount = pendingData?.entries?.length || 0;
  const completedCount = completedData?.entries?.length || 0;

  // Filter messages by search term
  const filteredReadyMessages = useMemo(() => {
    if (!readyData?.messages) return [];
    if (!searchTerm) return readyData.messages;
    
    const term = searchTerm.toLowerCase();
    return readyData.messages.filter(msg => 
      msg.id_b64?.toLowerCase().includes(term) ||
      msg.seq.toString().includes(term) ||
      JSON.stringify(msg.payload || '').toLowerCase().includes(term)
    );
  }, [readyData, searchTerm]);

  const filteredPendingMessages = useMemo(() => {
    if (!pendingData?.entries) return [];
    if (!searchTerm) return pendingData.entries;
    
    const term = searchTerm.toLowerCase();
    return pendingData.entries.filter(entry => 
      entry.id_b64?.toLowerCase().includes(term) ||
      entry.consumer_id.toLowerCase().includes(term)
    );
  }, [pendingData, searchTerm]);

  const filteredCompletedMessages = useMemo(() => {
    if (!completedData?.entries) return [];
    if (!searchTerm) return completedData.entries;
    
    const term = searchTerm.toLowerCase();
    return completedData.entries.filter(entry => 
      entry.id_b64?.toLowerCase().includes(term) ||
      entry.consumer_id.toLowerCase().includes(term)
    );
  }, [completedData, searchTerm]);

  const renderReadyTab = () => {
    if (readyLoading) {
      return <div className="p-6 text-center text-muted-foreground">Loading...</div>;
    }

    if (filteredReadyMessages.length === 0) {
      return (
        <Card>
          <CardContent className="pt-6">
            <div className="text-center py-12">
              <PackageOpen className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
              <h3 className="text-lg font-semibold mb-2">No Ready Messages</h3>
              <p className="text-sm text-muted-foreground">
                {searchTerm ? 'No messages match your search.' : 'Queue is empty.'}
              </p>
            </div>
          </CardContent>
        </Card>
      );
    }

    return (
      <div className="space-y-3">
        {filteredReadyMessages.map((msg) => {
          let payloadDisplay = msg.payload || '';
          try {
            const parsed = JSON.parse(msg.payload || '{}');
            payloadDisplay = JSON.stringify(parsed, null, 2);
          } catch {
            // Keep as-is if not valid JSON
          }

          return (
            <Card key={`${msg.seq}-${msg.id_b64}`}>
              <CardHeader className="pb-3">
                <div className="flex items-start justify-between">
                  <div className="flex-1 min-w-0">
                    <CardTitle className="text-sm font-mono truncate flex items-center gap-2">
                      <FileText className="h-4 w-4 flex-shrink-0" />
                      <span className="truncate">Seq: {msg.seq}</span>
                    </CardTitle>
                    <p className="text-xs text-muted-foreground mt-1">
                      ID: {msg.id_b64 || 'N/A'}
                    </p>
                  </div>
                  <Badge variant="default" className="flex-shrink-0 ml-2">
                    Priority: {msg.priority}
                  </Badge>
                </div>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div>
                    <div className="text-muted-foreground mb-1">Partition</div>
                    <div className="font-semibold">{msg.partition}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground mb-1">Size</div>
                    <div className="font-semibold">{formatBytes(msg.payload_size)}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground mb-1">Delay</div>
                    <div className="font-semibold">
                      {msg.delay_until_ms > Date.now() ? `${Math.floor((msg.delay_until_ms - Date.now()) / 1000)}s` : 'None'}
                    </div>
                  </div>
                </div>

                {msg.payload && (
                  <div className="pt-2 border-t">
                    <div className="text-xs text-muted-foreground mb-2">Payload</div>
                    <pre className="text-xs bg-muted p-2 rounded overflow-x-auto max-h-48 overflow-y-auto">
                      {payloadDisplay}
                    </pre>
                  </div>
                )}

                {msg.headers && Object.keys(msg.headers).length > 0 && (
                  <div className="pt-2 border-t">
                    <div className="text-xs text-muted-foreground mb-2">Headers</div>
                    <div className="flex flex-wrap gap-1">
                      {Object.entries(msg.headers).map(([key, value]) => (
                        <Badge key={key} variant="outline" className="text-xs">
                          {key}: {value}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          );
        })}
      </div>
    );
  };

  const renderPendingTab = () => {
    if (pendingLoading) {
      return <div className="p-6 text-center text-muted-foreground">Loading...</div>;
    }

    if (filteredPendingMessages.length === 0) {
      return (
        <Card>
          <CardContent className="pt-6">
            <div className="text-center py-12">
              <Clock className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
              <h3 className="text-lg font-semibold mb-2">No Pending Messages</h3>
              <p className="text-sm text-muted-foreground">
                {searchTerm ? 'No messages match your search.' : 'No messages are currently being processed.'}
              </p>
            </div>
          </CardContent>
        </Card>
      );
    }

    return (
      <div className="space-y-3">
        {filteredPendingMessages.map((entry) => {
          const expiresDate = new Date(entry.lease_expires_at_ms);
          const now = Date.now();
          const timeUntilExpiry = expiresDate.getTime() - now;

          return (
            <Card key={`${entry.id_b64}-${entry.consumer_id}`}>
              <CardHeader className="pb-3">
                <div className="flex items-start justify-between">
                  <div className="flex-1 min-w-0">
                    <CardTitle className="text-sm font-mono truncate flex items-center gap-2">
                      <Users className="h-4 w-4 flex-shrink-0" />
                      <span className="truncate">{entry.consumer_id}</span>
                    </CardTitle>
                    <p className="text-xs text-muted-foreground mt-1">
                      ID: {entry.id_b64 || entry.id}
                    </p>
                  </div>
                  <Badge 
                    variant={timeUntilExpiry > 0 ? "default" : "destructive"} 
                    className="flex-shrink-0 ml-2"
                  >
                    {timeUntilExpiry > 0 ? 'Active' : 'Expired'}
                  </Badge>
                </div>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <div className="text-muted-foreground mb-1">Delivery Count</div>
                    <div className="font-semibold">{entry.delivery_count}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground mb-1">Idle Time</div>
                    <div className="font-semibold">{Math.floor(entry.idle_ms / 1000)}s</div>
                  </div>
                </div>

                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Last Delivery</span>
                    <span className="font-medium">
                      {formatRelativeTime(entry.last_delivery_ms)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Lease Expires</span>
                    <span className="font-medium">
                      {timeUntilExpiry > 0 
                        ? `in ${Math.floor(timeUntilExpiry / 1000)}s`
                        : 'Expired'
                      }
                    </span>
                  </div>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>
    );
  };

  const renderCompletedTab = () => {
    if (completedLoading) {
      return <div className="p-6 text-center text-muted-foreground">Loading...</div>;
    }

    if (filteredCompletedMessages.length === 0) {
      return (
        <Card>
          <CardContent className="pt-6">
            <div className="text-center py-12">
              <PackageCheck className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
              <h3 className="text-lg font-semibold mb-2">No Completed Messages</h3>
              <p className="text-sm text-muted-foreground">
                {searchTerm ? 'No messages match your search.' : 'No messages have been completed yet.'}
              </p>
            </div>
          </CardContent>
        </Card>
      );
    }

    return (
      <div className="space-y-3">
        {filteredCompletedMessages.map((entry) => (
          <Card key={`${entry.seq}-${entry.id_b64}`}>
            <CardHeader className="pb-3">
              <div className="flex items-start justify-between">
                <div className="flex-1 min-w-0">
                  <CardTitle className="text-sm font-mono truncate flex items-center gap-2">
                    <PackageCheck className="h-4 w-4 flex-shrink-0" />
                    <span className="truncate">Seq: {entry.seq}</span>
                  </CardTitle>
                  <p className="text-xs text-muted-foreground mt-1">
                    By: {entry.consumer_id}
                  </p>
                </div>
                <Badge variant="secondary" className="flex-shrink-0 ml-2">
                  {entry.delivery_count}x delivered
                </Badge>
              </div>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <div className="text-muted-foreground mb-1">Duration</div>
                  <div className="font-semibold">{(entry.duration_ms / 1000).toFixed(2)}s</div>
                </div>
                <div>
                  <div className="text-muted-foreground mb-1">Partition</div>
                  <div className="font-semibold">{entry.partition}</div>
                </div>
              </div>

              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Completed</span>
                  <span className="font-medium">
                    {formatRelativeTime(entry.completed_at_ms)}
                  </span>
                </div>
                {entry.dequeued_at_ms > 0 && (
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Dequeued</span>
                    <span className="font-medium">
                      {formatRelativeTime(entry.dequeued_at_ms)}
                    </span>
                  </div>
                )}
              </div>

              {entry.headers && Object.keys(entry.headers).length > 0 && (
                <div className="pt-2 border-t">
                  <div className="text-xs text-muted-foreground mb-2">Headers</div>
                  <div className="flex flex-wrap gap-1">
                    {Object.entries(entry.headers).map(([key, value]) => (
                      <Badge key={key} variant="outline" className="text-xs">
                        {key}: {value}
                      </Badge>
                    ))}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        ))}
      </div>
    );
  };

  return (
    <div className="p-6 space-y-6">
      {/* Header with State Summary */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold">Messages</h2>
          <p className="text-sm text-muted-foreground">
            View messages across different lifecycle states
          </p>
        </div>
      </div>

      {/* State Summary Cards */}
      <div className="grid grid-cols-3 gap-4">
        <Card className={`cursor-pointer transition-all ${activeTab === 'ready' ? 'ring-2 ring-primary' : ''}`} onClick={() => setActiveTab('ready')}>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">🟢 Ready</p>
                <p className="text-2xl font-bold">{formatNumber(readyCount)}</p>
              </div>
              <PackageOpen className="h-8 w-8 text-green-500" />
            </div>
            <p className="text-xs text-muted-foreground mt-2">Waiting to be dequeued</p>
          </CardContent>
        </Card>

        <Card className={`cursor-pointer transition-all ${activeTab === 'pending' ? 'ring-2 ring-primary' : ''}`} onClick={() => setActiveTab('pending')}>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">🟡 Pending</p>
                <p className="text-2xl font-bold">{formatNumber(pendingCount)}</p>
              </div>
              <Clock className="h-8 w-8 text-yellow-500" />
            </div>
            <p className="text-xs text-muted-foreground mt-2">Being processed</p>
          </CardContent>
        </Card>

        <Card className={`cursor-pointer transition-all ${activeTab === 'completed' ? 'ring-2 ring-primary' : ''}`} onClick={() => setActiveTab('completed')}>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">🔵 Completed</p>
                <p className="text-2xl font-bold">{formatNumber(completedCount)}</p>
              </div>
              <PackageCheck className="h-8 w-8 text-blue-500" />
            </div>
            <p className="text-xs text-muted-foreground mt-2">Recently finished</p>
          </CardContent>
        </Card>
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-3">
        {(activeTab === 'pending' || activeTab === 'completed') && (
          <div className="shrink-0">
            <Select value={selectedGroup} onValueChange={setSelectedGroup}>
              <SelectTrigger className="w-[180px] h-8 hover:bg-input-background active:bg-input data-[state=open]:bg-input">
                <span className="flex items-center gap-1 text-xs">
                  <span className="text-muted-foreground">group</span>
                  <span className="inline-block w-1" aria-hidden />
                  <SelectValue />
                </span>
              </SelectTrigger>
              <SelectContent>
                {groups.map((group) => (
                  <SelectItem key={group} value={group} className="text-xs">
                    {group}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        )}

        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search by ID, consumer, or content..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="pl-9"
          />
        </div>
      </div>

      {/* State Tabs */}
      <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as MessageState)}>
        <TabsList className="grid w-full grid-cols-3">
          <TabsTrigger value="ready">Ready ({readyCount})</TabsTrigger>
          <TabsTrigger value="pending">Pending ({pendingCount})</TabsTrigger>
          <TabsTrigger value="completed">Completed ({completedCount})</TabsTrigger>
        </TabsList>

        <TabsContent value="ready" className="mt-6">
          {renderReadyTab()}
        </TabsContent>

        <TabsContent value="pending" className="mt-6">
          {renderPendingTab()}
        </TabsContent>

        <TabsContent value="completed" className="mt-6">
          {renderCompletedTab()}
        </TabsContent>
      </Tabs>
    </div>
  );
}

