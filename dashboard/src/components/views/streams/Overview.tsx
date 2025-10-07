import React from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../../ui/card';
import { Button } from '../../ui/button';
import { ScrollArea } from '../../ui/scroll-area';
import { ChartContainer, ChartTooltip, ChartTooltipContent, ChartConfig, ChartLegend, ChartLegendContent } from '../../ui/chart';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, AreaChart, Area } from 'recharts';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { useStreamStats, useStreamMetrics } from '../../../hooks/useStreams';
import { useFormatting } from '../../../hooks/useFormatting';

interface StreamOverviewProps {
  namespace: string;
  stream: string;
  onNavigate: (view: import('../../../types/navigation').View, namespace?: string, stream?: string, tab?: string) => void;
  onShowPublishModal: () => void;
}

export default function StreamOverview({ namespace, stream, onNavigate, onShowPublishModal }: StreamOverviewProps) {
  const { data, isLoading, error } = useStreamStats(namespace, stream);
  const { formatNumber, formatBytes, formatRelativeTime } = useFormatting();
  const [timeRange, setTimeRange] = React.useState("90d");

  const kpi = {
    total_count: data?.total_count || 0,
    total_bytes: data?.total_bytes || 0,
    active_subscribers: data?.active_subscribers || 0,
    groups_count: data?.groups_count || 0,
    last_publish_ms: data?.last_publish_ms || 0,
    last_delivered_ms: data?.last_delivered_ms || 0,
  };

  // Compute step by range
  const stepMs = React.useMemo(() => {
    if (timeRange === '7d') return 60 * 60 * 1000; // 1h
    if (timeRange === '30d') return 4 * 60 * 60 * 1000; // 4h
    return 12 * 60 * 60 * 1000; // 12h
  }, [timeRange]);

  // Metrics hooks
  const { data: publishCount } = useStreamMetrics(namespace, stream, 'publish_count', { range: timeRange as any, stepMs });
  const { data: ackCount } = useStreamMetrics(namespace, stream, 'ack_count', { range: timeRange as any, stepMs });
  const { data: publishRate } = useStreamMetrics(namespace, stream, 'publish_rate', { range: timeRange as any, stepMs });
  const { data: bytesRate } = useStreamMetrics(namespace, stream, 'bytes_rate', { range: timeRange as any, stepMs });

  // Transform metrics → chart data
  const messageFlowData = React.useMemo(() => {
    const pc = publishCount?.series?.[0]?.points || [];
    const ac = ackCount?.series?.[0]?.points || [];
    // Merge by index; both series should align in step
    const len = Math.max(pc.length, ac.length);
    const out: { date: string; published: number; acknowledged: number }[] = [];
    for (let i = 0; i < len; i++) {
      const t = pc[i]?.[0] ?? ac[i]?.[0];
      if (t == null) continue;
      out.push({
        date: new Date(t).toISOString(),
        published: pc[i]?.[1] ?? 0,
        acknowledged: ac[i]?.[1] ?? 0,
      });
    }
    return out;
  }, [publishCount, ackCount]);

  const publishRateSeries = React.useMemo(() => {
    const pts = publishRate?.series?.[0]?.points || [];
    return pts.map(([t, v]) => ({ time: t, rate: v }));
  }, [publishRate]);

  const bytesRateSeries = React.useMemo(() => {
    const pts = bytesRate?.series?.[0]?.points || [];
    return pts.map(([t, v]) => ({ time: t, bytes: v }));
  }, [bytesRate]);

  const chartConfig = {
    published: { 
      label: 'Published', 
      color: 'var(--chart-4)' 
    },
    acknowledged: { 
      label: 'Acknowledged', 
      color: 'var(--chart-2)' 
    },
  } satisfies ChartConfig;

  const filteredData = messageFlowData;

  return (
    <div className="h-full flex flex-col">
      {/* Scrollable Content */}
      <ScrollArea className="flex-1">
        <div className="p-6 space-y-6">
          {/* Global Time Control */}
          <div className="flex items-center gap-3 mb-2">
            <Select value={timeRange} onValueChange={setTimeRange}>
              <SelectTrigger className="w-[160px] rounded-lg">
                <SelectValue placeholder="Last 3 months" />
              </SelectTrigger>
              <SelectContent className="rounded-xl">
                <SelectItem value="90d" className="rounded-lg">
                  Last 3 months
                </SelectItem>
                <SelectItem value="30d" className="rounded-lg">
                  Last 30 days
                </SelectItem>
                <SelectItem value="7d" className="rounded-lg">
                  Last 7 days
                </SelectItem>
              </SelectContent>
            </Select>
            <span className="text-sm text-muted-foreground">
              Statistics for last {timeRange === "90d" ? "3 months" : timeRange === "30d" ? "30 days" : "7 days"}
            </span>
          </div>

          {/* Main Content Grid - 6:4 ratio */}
          <div className="grid grid-cols-10 gap-6">
            {/* Message Flow - 6 columns */}
            <div className="col-span-6">
              <Card className="pt-0">
                <CardHeader className="pb-2 border-b">
                  <CardTitle className="text-sm">Message Flow</CardTitle>
                  <CardDescription>
                    Published and acknowledged messages over time
                  </CardDescription>
                </CardHeader>
                <CardContent className="px-2 pt-4 sm:px-6 sm:pt-6">
                  <ChartContainer
                    config={chartConfig}
                    className="aspect-auto h-[250px] w-full"
                  >
                    <AreaChart data={filteredData}>
                      <defs>
                        <linearGradient id="fillPublished" x1="0" y1="0" x2="0" y2="1">
                          <stop
                            offset="5%"
                            stopColor="var(--color-published)"
                            stopOpacity={0.8}
                          />
                          <stop
                            offset="95%"
                            stopColor="var(--color-published)"
                            stopOpacity={0.1}
                          />
                        </linearGradient>
                        <linearGradient id="fillAcknowledged" x1="0" y1="0" x2="0" y2="1">
                          <stop
                            offset="5%"
                            stopColor="var(--color-acknowledged)"
                            stopOpacity={0.8}
                          />
                          <stop
                            offset="95%"
                            stopColor="var(--color-acknowledged)"
                            stopOpacity={0.1}
                          />
                        </linearGradient>
                      </defs>
                      <CartesianGrid vertical={false} />
                      <XAxis
                        dataKey="date"
                        tickLine={false}
                        axisLine={false}
                        tickMargin={8}
                        minTickGap={32}
                        tickFormatter={(value) => {
                          const date = new Date(value);
                          return date.toLocaleDateString("en-US", {
                            month: "short",
                            day: "numeric",
                          });
                        }}
                      />
                      <ChartTooltip
                        cursor={false}
                        content={
                          <ChartTooltipContent
                            labelFormatter={(value) => {
                              return new Date(value).toLocaleDateString("en-US", {
                                month: "short",
                                day: "numeric",
                              });
                            }}
                            indicator="dot"
                          />
                        }
                      />
                      
                      <Area
                        dataKey="published"
                        type="natural"
                        fill="url(#fillPublished)"
                        stroke="var(--color-published)"
                        stackId="a"
                      />
                      <Area
                        dataKey="acknowledged"
                        type="natural"
                        fill="url(#fillAcknowledged)"
                        stroke="var(--color-acknowledged)"
                        stackId="a"
                      />
                      <ChartLegend content={<ChartLegendContent />} />
                    </AreaChart>
                  </ChartContainer>
                </CardContent>
              </Card>
            </div>

            {/* Metrics Grid - 4 columns */}
            <div className="col-span-4">
              <div className="grid grid-cols-2 gap-4 h-96">
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-xs text-muted-foreground">Messages</CardTitle>
                  </CardHeader>
                  <CardContent className="pt-0">
                    <div className="text-2xl font-semibold">{formatNumber(kpi.total_count)}</div>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-xs text-muted-foreground">Bytes</CardTitle>
                  </CardHeader>
                  <CardContent className="pt-0">
                    <div className="text-2xl font-semibold">{formatBytes(kpi.total_bytes)}</div>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-xs text-muted-foreground">Subscribers</CardTitle>
                  </CardHeader>
                  <CardContent className="pt-0">
                    <div className="text-2xl font-semibold">{formatNumber(kpi.active_subscribers)}</div>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-xs text-muted-foreground">Groups</CardTitle>
                  </CardHeader>
                  <CardContent className="pt-0">
                    <div className="text-2xl font-semibold">{formatNumber(kpi.groups_count)}</div>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-xs text-muted-foreground">Last Published</CardTitle>
                  </CardHeader>
                  <CardContent className="pt-0">
                    <div className="text-sm">{kpi.last_publish_ms ? formatRelativeTime(kpi.last_publish_ms) : '–'}</div>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-xs text-muted-foreground">Last Delivered</CardTitle>
                  </CardHeader>
                  <CardContent className="pt-0">
                    <div className="text-sm">{kpi.last_delivered_ms ? formatRelativeTime(kpi.last_delivered_ms) : '–'}</div>
                  </CardContent>
                </Card>
              </div>
            </div>
          </div>

          {/* Bottom Row - Rate Charts + Issues */}
          <div className="grid grid-cols-12 gap-6">
            {/* Rate Charts - 4 columns */}
            <div className="col-span-6">
              <div className="grid grid-cols-2 gap-4">
                <Card>
                  <CardHeader>
                    <CardTitle className="text-muted-foreground">Publish Rate</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <ChartContainer config={{ rate: { label: 'events/sec', color: '#3b82f6' } }}>
                      <LineChart data={publishRateSeries} margin={{ left: 4, right: 4, top: 4, bottom: 4 }}>
                        <CartesianGrid strokeDasharray="2 2" />
                        <XAxis dataKey="time" hide />
                        <YAxis width={24} tickFormatter={(v) => String(v)} />
                        <Line type="monotone" dataKey="rate" stroke="#3b82f6" strokeWidth={2} dot={false} />
                        <ChartTooltip content={<ChartTooltipContent />} />
                      </LineChart>
                    </ChartContainer>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader>
                    <CardTitle className="text-muted-foreground">Bytes Rate</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <ChartContainer config={{ bytes: { label: 'bytes/sec', color: '#6b7280' } }}>
                      <LineChart data={bytesRateSeries} margin={{ left: 4, right: 4, top: 4, bottom: 4 }}>
                        <CartesianGrid strokeDasharray="2 2" />
                        <XAxis dataKey="time" hide />
                        <YAxis width={28} tickFormatter={(v) => formatBytes(Number(v))} />
                        <Line type="monotone" dataKey="bytes" stroke="#6b7280" strokeWidth={2} dot={false} />
                        <ChartTooltip content={<ChartTooltipContent />} />
                      </LineChart>
                    </ChartContainer>
                  </CardContent>
                </Card>
              </div>
            </div>
          </div>
        </div>
      </ScrollArea>
    </div>
  );
}


