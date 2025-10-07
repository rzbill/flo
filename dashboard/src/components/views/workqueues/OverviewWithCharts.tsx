import React, { useMemo } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../../ui/card';
import { Button } from '../../ui/button';
import { ScrollArea } from '../../ui/scroll-area';
import { Plus, Package, Users, Clock, AlertTriangle, XCircle, TrendingUp } from 'lucide-react';
import { ChartContainer, ChartTooltip, ChartTooltipContent, ChartConfig } from '../../ui/chart';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, AreaChart, Area } from 'recharts';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { useWorkQueueStats, useWorkQueueMetrics } from '../../../hooks/useWorkQueues';
import { useFormatting } from '../../../hooks/useFormatting';

interface OverviewProps {
  namespace: string;
  workqueue: string;
  onShowEnqueueModal: () => void;
}

export default function Overview({ namespace, workqueue, onShowEnqueueModal }: OverviewProps) {
  const { formatNumber, formatRelativeTime } = useFormatting();
  const [timeRange, setTimeRange] = React.useState("90d");
  
  const { data: stats, isLoading, error } = useWorkQueueStats(namespace, workqueue);

  // Calculate step by range
  const stepMs = React.useMemo(() => {
    if (timeRange === '7d') return 60 * 60 * 1000; // 1h
    if (timeRange === '30d') return 4 * 60 * 60 * 1000; // 4h
    return 12 * 60 * 60 * 1000; // 12h
  }, [timeRange]);

  // Fetch metrics
  const { data: enqueueCount } = useWorkQueueMetrics(namespace, workqueue, 'enqueue_count', { range: timeRange as any, stepMs });
  const { data: completeCount } = useWorkQueueMetrics(namespace, workqueue, 'complete_count', { range: timeRange as any, stepMs });
  const { data: enqueueRate } = useWorkQueueMetrics(namespace, workqueue, 'enqueue_rate', { range: timeRange as any, stepMs });
  const { data: queueDepth } = useWorkQueueMetrics(namespace, workqueue, 'queue_depth', { range: timeRange as any, stepMs });

  // Transform metrics → chart data
  const messageFlowData = useMemo(() => {
    const ec = enqueueCount?.series?.[0]?.points || [];
    const cc = completeCount?.series?.[0]?.points || [];
    const len = Math.max(ec.length, cc.length);
    const out: { date: string; enqueued: number; completed: number }[] = [];
    for (let i = 0; i < len; i++) {
      const t = ec[i]?.[0] ?? cc[i]?.[0];
      if (t == null) continue;
      out.push({
        date: new Date(t).toISOString(),
        enqueued: ec[i]?.[1] ?? 0,
        completed: cc[i]?.[1] ?? 0,
      });
    }
    return out;
  }, [enqueueCount, completeCount]);

  const enqueueRateSeries = useMemo(() => {
    const pts = enqueueRate?.series?.[0]?.points || [];
    return pts.map(([t, v]) => ({ time: new Date(t).toISOString(), rate: v }));
  }, [enqueueRate]);

  const queueDepthSeries = useMemo(() => {
    const pts = queueDepth?.series?.[0]?.points || [];
    return pts.map(([t, v]) => ({ time: new Date(t).toISOString(), depth: v }));
  }, [queueDepth]);

  const chartConfig = {
    enqueued: { label: 'Enqueued', color: 'var(--chart-4)' },
    completed: { label: 'Completed', color: 'var(--chart-2)' },
  } satisfies ChartConfig;

  const metrics = useMemo(() => ({
    totalEnqueued: stats?.total_enqueued || 0,
    totalCompleted: stats?.total_completed || 0,
    totalFailed: stats?.total_failed || 0,
    pending: stats?.pending_count || 0,
    inFlight: stats?.in_flight_count || 0,
    retry: stats?.retry_count || 0,
    dlq: stats?.dlq_count || 0,
    successRate: stats?.total_enqueued 
      ? ((stats.total_completed / stats.total_enqueued) * 100).toFixed(1)
      : '0.0'
  }), [stats]);

  if (isLoading) {
    return <div className="p-6"><div className="animate-pulse">Loading...</div></div>;
  }

  if (error) {
    return (
      <div className="p-6">
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 p-6">
          <h3 className="font-semibold text-destructive mb-2">Error Loading Stats</h3>
          <p className="text-sm text-muted-foreground">{error.message || 'Failed to load workqueue statistics'}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      <ScrollArea className="flex-1">
        <div className="p-6 space-y-6">
          {/* Header */}
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-lg font-semibold">Overview</h2>
              <p className="text-sm text-muted-foreground">WorkQueue metrics and statistics</p>
            </div>
            <div className="flex items-center gap-3">
              <Select value={timeRange} onValueChange={setTimeRange}>
                <SelectTrigger className="w-[160px] rounded-lg">
                  <SelectValue placeholder="Last 3 months" />
                </SelectTrigger>
                <SelectContent className="rounded-xl">
                  <SelectItem value="90d" className="rounded-lg">Last 3 months</SelectItem>
                  <SelectItem value="30d" className="rounded-lg">Last 30 days</SelectItem>
                  <SelectItem value="7d" className="rounded-lg">Last 7 days</SelectItem>
                </SelectContent>
              </Select>
              <Button onClick={onShowEnqueueModal}>
                <Plus className="h-4 w-4 mr-2" />
                Enqueue Message
              </Button>
            </div>
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
                <CardTitle className="text-sm font-medium">Success Rate</CardTitle>
                <TrendingUp className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{metrics.successRate}%</div>
                <p className="text-xs text-muted-foreground">{formatNumber(metrics.totalCompleted)} completed</p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Failed</CardTitle>
                <XCircle className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{formatNumber(metrics.totalFailed)}</div>
                <p className="text-xs text-muted-foreground">
                  {formatNumber(metrics.retry)} retry / {formatNumber(metrics.dlq)} DLQ
                </p>
              </CardContent>
            </Card>
          </div>

          {/* Message Flow Chart */}
          <Card>
            <CardHeader>
              <CardTitle>Message Flow</CardTitle>
              <CardDescription>Enqueued vs. Completed messages over time</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer config={chartConfig} className="h-[300px] w-full">
                <LineChart data={messageFlowData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="date" tick={{ fontSize: 10 }} tickFormatter={(value) => new Date(value).toLocaleDateString()} />
                  <YAxis tick={{ fontSize: 10 }} />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <Line type="monotone" dataKey="enqueued" stroke="var(--chart-4)" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="completed" stroke="var(--chart-2)" strokeWidth={2} dot={false} />
                </LineChart>
              </ChartContainer>
            </CardContent>
          </Card>

          {/* Enqueue Rate Chart */}
          <Card>
            <CardHeader>
              <CardTitle>Enqueue Rate</CardTitle>
              <CardDescription>Messages enqueued per second</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer config={{ rate: { label: 'Rate', color: 'var(--chart-1)' } }} className="h-[200px] w-full">
                <AreaChart data={enqueueRateSeries} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="time" tick={{ fontSize: 10 }} tickFormatter={(value) => new Date(value).toLocaleDateString()} />
                  <YAxis tick={{ fontSize: 10 }} />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <Area type="monotone" dataKey="rate" fill="var(--chart-1)" stroke="var(--chart-1)" fillOpacity={0.3} />
                </AreaChart>
              </ChartContainer>
            </CardContent>
          </Card>

          {/* Queue Depth Chart */}
          <Card>
            <CardHeader>
              <CardTitle>Queue Depth</CardTitle>
              <CardDescription>Pending messages over time</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer config={{ depth: { label: 'Depth', color: 'var(--chart-3)' } }} className="h-[200px] w-full">
                <AreaChart data={queueDepthSeries} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="time" tick={{ fontSize: 10 }} tickFormatter={(value) => new Date(value).toLocaleDateString()} />
                  <YAxis tick={{ fontSize: 10 }} />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <Area type="monotone" dataKey="depth" fill="var(--chart-3)" stroke="var(--chart-3)" fillOpacity={0.3} />
                </AreaChart>
              </ChartContainer>
            </CardContent>
          </Card>
        </div>
      </ScrollArea>
    </div>
  );
}

