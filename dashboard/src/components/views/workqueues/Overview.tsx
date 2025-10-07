import React, { useMemo } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../ui/card';
import { Button } from '../../ui/button';
import { Plus, Package, Users, Clock, AlertTriangle, XCircle } from 'lucide-react';
import { useWorkQueueStats } from '../../../hooks/useWorkQueues';
import { useFormatting } from '../../../hooks/useFormatting';

interface OverviewProps {
  namespace: string;
  workqueue: string;
  onShowEnqueueModal: () => void;
}

export default function Overview({ namespace, workqueue, onShowEnqueueModal }: OverviewProps) {
  const { formatNumber, formatRelativeTime } = useFormatting();
  
  const { data: stats, isLoading, error } = useWorkQueueStats(namespace, workqueue);

  const metrics = useMemo(() => ({
    totalEnqueued: stats?.total_enqueued || 0,
    totalCompleted: stats?.total_completed || 0,
    totalFailed: stats?.total_failed || 0,
    pending: stats?.pending_count || 0,
    inFlight: stats?.in_flight_count || 0,
    retry: stats?.retry_count || 0,
    dlq: stats?.dlq_count || 0,
    groups: stats?.groups || [],
    successRate: stats?.total_enqueued 
      ? ((stats.total_completed / stats.total_enqueued) * 100).toFixed(1)
      : '0.0'
  }), [stats]);

  if (isLoading) {
    return (
      <div className="p-6 space-y-4">
        <div className="animate-pulse space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="h-32 bg-muted rounded-lg" />
            ))}
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6">
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 p-6">
          <h3 className="font-semibold text-destructive mb-2">Error Loading Stats</h3>
          <p className="text-sm text-muted-foreground">
            {error.message || 'Failed to load workqueue statistics'}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      {/* Quick Actions */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold">Overview</h2>
          <p className="text-sm text-muted-foreground">
            WorkQueue metrics and statistics
          </p>
        </div>
        <Button onClick={onShowEnqueueModal}>
          <Plus className="h-4 w-4 mr-2" />
          Enqueue Message
        </Button>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Pending</CardTitle>
            <Package className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{formatNumber(metrics.pending)}</div>
            <p className="text-xs text-muted-foreground">Ready to dequeue</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">In-Flight</CardTitle>
            <Clock className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{formatNumber(metrics.inFlight)}</div>
            <p className="text-xs text-muted-foreground">Currently processing</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Retry Queue</CardTitle>
            <AlertTriangle className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{formatNumber(metrics.retry)}</div>
            <p className="text-xs text-muted-foreground">Awaiting retry</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Dead Letter Queue</CardTitle>
            <XCircle className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{formatNumber(metrics.dlq)}</div>
            <p className="text-xs text-muted-foreground">Failed permanently</p>
          </CardContent>
        </Card>
      </div>

      {/* Lifetime Stats */}
      <Card>
        <CardHeader>
          <CardTitle>Lifetime Statistics</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
            <div>
              <div className="text-sm text-muted-foreground mb-1">Total Enqueued</div>
              <div className="text-2xl font-semibold">{formatNumber(metrics.totalEnqueued)}</div>
            </div>
            <div>
              <div className="text-sm text-muted-foreground mb-1">Total Completed</div>
              <div className="text-2xl font-semibold text-green-600">
                {formatNumber(metrics.totalCompleted)}
              </div>
            </div>
            <div>
              <div className="text-sm text-muted-foreground mb-1">Total Failed</div>
              <div className="text-2xl font-semibold text-red-600">
                {formatNumber(metrics.totalFailed)}
              </div>
            </div>
            <div>
              <div className="text-sm text-muted-foreground mb-1">Success Rate</div>
              <div className="text-2xl font-semibold">{metrics.successRate}%</div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Consumer Groups */}
      {metrics.groups.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Users className="h-5 w-5" />
              Consumer Groups
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {metrics.groups.map((group) => (
                <div
                  key={group.group}
                  className="flex items-center justify-between p-4 border rounded-lg hover:bg-muted/50 transition-colors"
                >
                  <div className="flex-1">
                    <h4 className="font-medium">{group.group}</h4>
                    <div className="flex items-center gap-4 mt-2 text-sm text-muted-foreground">
                      <span>{formatNumber(group.consumer_count)} consumers</span>
                      <span>·</span>
                      <span>{formatNumber(group.pending_count)} pending</span>
                      <span>·</span>
                      <span>{formatNumber(group.in_flight_count)} in-flight</span>
                      {group.retry_count > 0 && (
                        <>
                          <span>·</span>
                          <span className="text-orange-600">
                            {formatNumber(group.retry_count)} retry
                          </span>
                        </>
                      )}
                      {group.dlq_count > 0 && (
                        <>
                          <span>·</span>
                          <span className="text-red-600">
                            {formatNumber(group.dlq_count)} DLQ
                          </span>
                        </>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Empty State */}
      {metrics.totalEnqueued === 0 && (
        <Card>
          <CardContent className="pt-6">
            <div className="text-center py-12">
              <Package className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
              <h3 className="text-lg font-semibold mb-2">No Messages Yet</h3>
              <p className="text-sm text-muted-foreground mb-6">
                Start by enqueuing your first message to this workqueue.
              </p>
              <Button onClick={onShowEnqueueModal}>
                <Plus className="h-4 w-4 mr-2" />
                Enqueue Message
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
