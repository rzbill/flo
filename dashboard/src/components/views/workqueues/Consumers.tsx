import React, { useState, useMemo } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../ui/card';
import { Button } from '../../ui/button';
import { Input } from '../../ui/input';
import { Badge } from '../../ui/badge';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '../../ui/select';
import { Users, Search, UserCircle, Activity, Clock } from 'lucide-react';
import { useConsumers, useWorkQueueGroups } from '../../../hooks/useWorkQueues';
import { useFormatting } from '../../../hooks/useFormatting';

interface ConsumersProps {
  namespace: string;
  workqueue: string;
}

export default function Consumers({ namespace, workqueue }: ConsumersProps) {
  const [selectedGroup, setSelectedGroup] = useState<string>('default');
  const [searchTerm, setSearchTerm] = useState('');
  const { formatNumber, formatRelativeTime } = useFormatting();

  // Fetch consumer groups
  const { data: groupsData } = useWorkQueueGroups(namespace, workqueue);
  const groups = useMemo(() => {
    if (!groupsData?.groups) return ['default'];
    return groupsData.groups;
  }, [groupsData]);

  // Fetch consumers for selected group
  const { data: consumersData, isLoading, error } = useConsumers(namespace, workqueue, selectedGroup);

  const consumers = useMemo(() => {
    if (!consumersData?.consumers) return [];
    
    if (!searchTerm) return consumersData.consumers;
    
    const term = searchTerm.toLowerCase();
    return consumersData.consumers.filter(c => 
      c.consumer_id.toLowerCase().includes(term)
    );
  }, [consumersData, searchTerm]);

  const activeCount = useMemo(() => 
    consumers.filter(c => c.is_alive).length,
    [consumers]
  );

  if (isLoading) {
    return (
      <div className="p-6 space-y-4">
        <div className="animate-pulse space-y-4">
          <div className="h-12 bg-muted rounded" />
          <div className="h-64 bg-muted rounded" />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6">
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 p-6">
          <h3 className="font-semibold text-destructive mb-2">Error Loading Consumers</h3>
          <p className="text-sm text-muted-foreground">
            {error.message || 'Failed to load consumer information'}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold flex items-center gap-2">
            <Users className="h-5 w-5" />
            Consumers
          </h2>
          <p className="text-sm text-muted-foreground">
            {activeCount} of {consumers.length} consumers active
          </p>
        </div>
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-3">
        {/* Group Selector */}
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

        {/* Search */}
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search by consumer ID..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="pl-9"
          />
        </div>
      </div>

      {/* Consumers List */}
      {consumers.length === 0 ? (
        <Card>
          <CardContent className="pt-6">
            <div className="text-center py-12">
              <UserCircle className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
              <h3 className="text-lg font-semibold mb-2">No Consumers Found</h3>
              <p className="text-sm text-muted-foreground">
                {searchTerm
                  ? 'No consumers match your search criteria.'
                  : `No active consumers in group "${selectedGroup}".`}
              </p>
            </div>
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {consumers.map((consumer) => {
            const isActive = consumer.is_alive;
            const expiresDate = new Date(consumer.expires_at_ms);
            const now = Date.now();
            const timeUntilExpiry = expiresDate.getTime() - now;

            return (
              <Card key={consumer.consumer_id} className="relative">
                <CardHeader className="pb-3">
                  <div className="flex items-start justify-between">
                    <div className="flex-1 min-w-0">
                      <CardTitle className="text-base flex items-center gap-2 truncate">
                        <UserCircle className="h-4 w-4 flex-shrink-0" />
                        <span className="truncate">{consumer.consumer_id}</span>
                      </CardTitle>
                    </div>
                    <Badge
                      variant={isActive ? "default" : "secondary"}
                      className="flex-shrink-0 ml-2"
                    >
                      {isActive ? (
                        <>
                          <Activity className="h-3 w-3 mr-1" />
                          Active
                        </>
                      ) : (
                        <>
                          <Clock className="h-3 w-3 mr-1" />
                          Inactive
                        </>
                      )}
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-3">
                  {/* Capacity Info */}
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <div className="text-muted-foreground mb-1">Capacity</div>
                      <div className="font-semibold">{formatNumber(consumer.capacity)}</div>
                    </div>
                    <div>
                      <div className="text-muted-foreground mb-1">In-Flight</div>
                      <div className="font-semibold">{formatNumber(consumer.in_flight)}</div>
                    </div>
                  </div>

                  {/* Timing Info */}
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Last Seen</span>
                      <span className="font-medium">
                        {formatRelativeTime(consumer.last_seen_ms)}
                      </span>
                    </div>
                    {isActive && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Expires In</span>
                        <span className="font-medium">
                          {timeUntilExpiry > 0 
                            ? `${Math.floor(timeUntilExpiry / 1000)}s`
                            : 'Expired'
                          }
                        </span>
                      </div>
                    )}
                  </div>

                  {/* Labels */}
                  {consumer.labels && Object.keys(consumer.labels).length > 0 && (
                    <div className="pt-2 border-t">
                      <div className="text-xs text-muted-foreground mb-2">Labels</div>
                      <div className="flex flex-wrap gap-1">
                        {Object.entries(consumer.labels).map(([key, value]) => (
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
      )}
    </div>
  );
}
