import React, { useState } from 'react';
import { useRetryMessages, useDLQMessages, useRetryDLQStats, useStreamGroups, RetryMessage, DLQMessage, RetryDLQGroupStat, RetryDLQStatsResponse } from '../../../hooks/useStreams';
import { decodeMessageId } from '../../../utils/messageUtils';

import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../../ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../ui/table';
import { Badge } from '../../ui/badge';
import { Button } from '../../ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../ui/tabs';
import { Progress } from '../../ui/progress';
import {
    AlertTriangle,
    Clock,
    RotateCcw,
    Trash2,
    Pause,
    Eye,
    Info
} from 'lucide-react';
import RetryQueueFilter from '../../custom/RetryQueueFilter';

interface StreamsRetryDLQProps {
    namespace: string;
    stream: string;
}

// Phase 4: Real API Integration - Mock data removed, using real API calls

export default function StreamsRetryDLQ({ namespace, stream }: StreamsRetryDLQProps) {
    const [selectedRetryMessages, setSelectedRetryMessages] = useState<string[]>([]);
    const [selectedDLQMessages, setSelectedDLQMessages] = useState<string[]>([]);
    const [retryFilter, setRetryFilter] = useState<string>('');
    const [dlqFilter, setDlqFilter] = useState<string>('');
    const [retryFilterTags, setRetryFilterTags] = useState<string[]>([]);
    const [dlqFilterTags, setDlqFilterTags] = useState<string[]>([]);
    const [selectedGroup, setSelectedGroup] = useState<string>('all');
    const [showQueueNames, setShowQueueNames] = useState<boolean>(false);
    
    // Filter parameters for API calls
    const [retryLimit, setRetryLimit] = useState<number>(100);
    const [retryReverse, setRetryReverse] = useState<boolean>(true);
    const [dlqLimit, setDlqLimit] = useState<number>(100);
    const [dlqReverse, setDlqReverse] = useState<boolean>(true);


    // Real API calls
    const { data: retryData, isLoading: retryLoading, error: retryError } = useRetryMessages(
        namespace,
        stream,
        selectedGroup === 'all' ? undefined : selectedGroup,
        { limit: retryLimit, reverse: retryReverse }
    );

    const { data: dlqData, isLoading: dlqLoading, error: dlqError } = useDLQMessages(
        namespace,
        stream,
        selectedGroup === 'all' ? undefined : selectedGroup,
        { limit: dlqLimit, reverse: dlqReverse }
    );

    const { data: statsData, isLoading: statsLoading, error: statsError } = useRetryDLQStats(
        namespace,
        stream,
        selectedGroup === 'all' ? undefined : selectedGroup
    );

    // Get available groups from the new groups API
    const { data: groupsData, isLoading: groupsLoading, error: groupsError } = useStreamGroups(
        namespace,
        stream
    );

    // Extract data from API responses
    const retryMessages: RetryMessage[] = (retryData?.items && Array.isArray(retryData.items)) ? retryData.items as RetryMessage[] : [];
    const dlqMessages: DLQMessage[] = (dlqData?.items && Array.isArray(dlqData.items)) ? dlqData.items as DLQMessage[] : [];
    const statsGroups: RetryDLQGroupStat[] = statsData?.groups || [];

    // Get available groups from the groups API (normalize to names)
    const availableGroups = groupsData?.groups ? [...groupsData.groups.map(g => (typeof g === 'string' ? g : g.name))] : [];

    // Parse filter expression and update state
    const parseFilterExpression = (expr: string, isRetry: boolean) => {
        if (!expr) return;
        
        const filters = expr.split(',');
        filters.forEach(filter => {
            const [key, value] = filter.split(':');
            if (!key || !value) return;
            
            switch (key.trim()) {
                case 'group':
                    if (isRetry) {
                        const groupValue = value.trim();
                        setSelectedGroup(groupValue === 'all' ? 'all' : groupValue);
                    }
                    break;
                case 'limit':
                    const limit = parseInt(value.trim(), 10);
                    if (!isNaN(limit) && limit > 0) {
                        if (isRetry) {
                            setRetryLimit(limit);
                        } else {
                            setDlqLimit(limit);
                        }
                    }
                    break;
                case 'reverse':
                    if (value.trim() === 'true') {
                        if (isRetry) {
                            setRetryReverse(true);
                        } else {
                            setDlqReverse(true);
                        }
                    }
                    break;
            }
        });
    };

    const formatTime = (timestamp: number) => {
        const now = Date.now();
        const diff = timestamp - now;
        if (diff < 0) return 'Overdue';
        const minutes = Math.floor(diff / 60000);
        return `${minutes}m`;
    };

    const formatBytes = (bytes: number) => {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    };

    const getRetryProgress = (retryCount: number, maxRetries: number) => {
        return (retryCount / maxRetries) * 100;
    };


    const calculateNextRetryTime = (retryCount: number, createdAt: number) => {
        // Exponential backoff: 200ms * (2^(attempts-1)) with max 30s
        const baseDelay = 200; // ms
        const delay = Math.min(baseDelay * Math.pow(2, retryCount - 1), 30000); // Max 30s
        return createdAt + delay;
    };

    const formatHeaders = (headers: Record<string, string> | null | undefined) => {
        if (!headers) return '';
        return Object.entries(headers)
            .map(([key, value]) => `${key}: ${value}`)
            .join(', ');
    };

    const getRetryReason = (retryCount: number, maxRetries: number) => {
        if (retryCount === 0) return 'PENDING';
        if (retryCount < maxRetries) return 'RETRYING';
        return 'MAX_RETRIES_EXCEEDED';
    };

    // Calculate success rate from real data
    const calculateSuccessRate = () => {
        if (!statsData) return 0;
        return Math.round(statsData.summary.success_rate);
    };

    // Get unique groups from real data
    const getUniqueGroups = () => {
        return availableGroups;
    };

    // Filter messages based on search
    const filteredRetryMessages = (retryMessages || []).filter(msg => {
        if (!retryFilter) return true;
        const searchTerm = retryFilter.toLowerCase();
        return (
            msg.id.toLowerCase().includes(searchTerm) ||
            msg.payload.toLowerCase().includes(searchTerm) ||
            msg.last_error.toLowerCase().includes(searchTerm) ||
            msg.group.toLowerCase().includes(searchTerm)
        );
    });

    const filteredDLQMessages = (dlqMessages || []).filter(msg => {
        if (!dlqFilter) return true;
        const searchTerm = dlqFilter.toLowerCase();
        return (
            msg.id.toLowerCase().includes(searchTerm) ||
            msg.payload.toLowerCase().includes(searchTerm) ||
            msg.last_error.toLowerCase().includes(searchTerm) ||
            msg.group.toLowerCase().includes(searchTerm)
        );
    });

    // Filter messages by group
    const filterMessagesByGroup = (messages: any[], group: string) => {
        if (group === 'all') return messages;
        return messages.filter(msg => msg.group === group);
    };

    // Calculate real next retry time for each message
    const getCalculatedNextRetryTime = (message: any) => {
        return calculateNextRetryTime(message.retryCount, message.createdAt);
    };


    return (
        <div className="grid grid-cols-12 gap-4 px-4">

            <div className="col-span-12 lg:col-span-8 lg:col-start-3 space-y-4">
               
                <div className="flex items-end justify-between py-6 my-10 border-b-1">
                    <h1 className="text-2xl md:text-3xl font-light">Retry & DLQ</h1>
                    <div className="flex gap-8 md:gap-10">
                        <div>
                            <div className="uppercase text-[11px] tracking-wide text-muted-foreground mb-1">Retrying</div>
                            <div className="text-2xl font-light leading-none">
                                {retryLoading ? '...' : retryMessages.length}
                            </div>
                        </div>
                        <div>
                            <div className="uppercase text-[11px] tracking-wide text-muted-foreground mb-1">Dead Letter</div>
                            <div className="text-2xl font-light leading-none">
                                {dlqLoading ? '...' : dlqMessages.length}
                            </div>
                        </div>
                        <div>
                            <div className="uppercase text-[11px] tracking-wide text-muted-foreground mb-1">Total Retries</div>
                            <div className="text-2xl font-light leading-none">
                                {statsLoading ? '...' : (statsData?.summary.total_retries ?? 0)}
                            </div>
                        </div>
                        <div>
                            <div className="uppercase text-[11px] tracking-wide text-muted-foreground mb-1">Success Rate</div>
                            <div className="text-2xl font-light leading-none">{calculateSuccessRate()}%</div>
                        </div>
                    </div>
                </div>

                {/* Loading and Error States */}
                {(retryLoading || dlqLoading || statsLoading) && (
                    <div className="flex items-center justify-center py-8">
                        <div className="text-sm text-muted-foreground">Loading retry/DLQ data...</div>
                    </div>
                )}

                {(retryError || dlqError || statsError) && (
                    <div className="bg-red-50 border border-red-200 rounded-md p-4">
                        <div className="text-sm text-red-800">
                            Error loading data: {retryError?.message || dlqError?.message || statsError?.message}
                        </div>
                        <div className="text-xs text-red-600 mt-2">
                            Make sure the Flo server is running on port 8080: <code>go run ./cmd/flo server --http-port 8080</code>
                        </div>
                        <div className="text-xs text-gray-600 mt-1">
                            Debug info: retryError={retryError?.message}, dlqError={dlqError?.message}, statsError={statsError?.message}
                        </div>
                    </div>
                )}


                {/* Main Content */}
                <Tabs defaultValue="retries" className="space-y-4">
                    {/* Tabs */}
                    <div className="border-b border-gray-200">
                        <TabsList className="h-auto p-0 bg-transparent border-0">
                            <TabsTrigger
                                value="retries"
                                className="flex items-center space-x-2 px-0 py-3 mr-8 text-sm font-medium data-[state=active]:border-b-2 data-[state=active]:border-gray-900 data-[state=active]:text-gray-900 data-[state=inactive]:text-gray-500 data-[state=inactive]:border-b-0 rounded-none border-0 bg-transparent"
                            >
                                <Clock className="h-4 w-4" />
                                <span>Retry Queue</span>
                            </TabsTrigger>
                            <TabsTrigger
                                value="dlq"
                                className="flex items-center space-x-2 px-0 py-3 text-sm font-medium data-[state=active]:border-b-2 data-[state=active]:border-gray-900 data-[state=active]:text-gray-900 data-[state=inactive]:text-gray-500 data-[state=inactive]:border-b-0 rounded-none border-0 bg-transparent"
                            >
                                <AlertTriangle className="h-4 w-4" />
                                <span>Dead Letter Queue</span>
                            </TabsTrigger>
                        </TabsList>
                    </div>

                    {/* Retry Queue Tab */}
                    <TabsContent value="retries" className="space-y-4">
                        <Card className="border-none p-0">
                            {filteredRetryMessages.length > 0 && (
                                <CardHeader className="px-0">
                                    <div className="flex items-center justify-between">
                                        <div>
                                            <CardTitle className="text-sm">Retry Queue ({filteredRetryMessages.length})</CardTitle>
                                            <CardDescription className="text-xs">
                                                Messages currently being retried for {namespace}/{stream}
                                            </CardDescription>
                                        </div>
                                        <div className="flex space-x-2">
                                            <RetryQueueFilter
                                                value={retryFilter}
                                                tags={retryFilterTags}
                                                groups={availableGroups}
                                                onApply={(expr, tags) => {
                                                    setRetryFilter(expr);
                                                    setRetryFilterTags(tags);
                                                    parseFilterExpression(expr, true);
                                                }}
                                            />
                                        </div>
                                    </div>
                                </CardHeader>
                            )}
                            <CardContent className="p-0">
                                {filteredRetryMessages.length === 0 ? (
                                    <div className="flex items-center justify-center py-12">
                                        <div className="text-center">
                                            <div className="text-sm text-muted-foreground mb-2">No retry messages found</div>
                                            <div className="text-xs text-muted-foreground">
                                                Messages will appear here when they fail processing and are being retried.
                                            </div>
                                        </div>
                                    </div>
                                ) : (
                                    <div className="overflow-x-auto rounded-lg border border-gray-200">
                                        <Table className="table-fixed w-full">
                                            <TableHeader>
                                                <TableRow className="bg-muted/40">
                                                    <TableHead className="w-12 p-2">
                                                        <input
                                                            type="checkbox"
                                                            className="h-4 w-4"
                                                            onChange={(e) => {
                                                                if (e.target.checked) {
                                                                    setSelectedRetryMessages(retryMessages.map(m => m.id));
                                                                } else {
                                                                    setSelectedRetryMessages([]);
                                                                }
                                                            }}
                                                        />
                                                    </TableHead>
                                                    <TableHead className="w-[80px] p-2 text-xs uppercase font-light">ID</TableHead>
                                                    <TableHead className="w-[120px] p-2 text-xs uppercase font-light">GROUP</TableHead>
                                                    <TableHead className="w-[100px] p-2 text-xs uppercase font-light">RETRIES</TableHead>
                                                    <TableHead className="w-[100px] p-2 text-xs uppercase font-light">NEXT RETRY</TableHead>
                                                    <TableHead className="w-[200px] p-2 text-xs uppercase font-light">LAST ERROR</TableHead>
                                                    <TableHead className="w-[100px] p-2 text-xs uppercase font-light">PARTITION</TableHead>
                                                    <TableHead className="p-2 text-xs uppercase font-light">PAYLOAD</TableHead>
                                                    <TableHead className="w-[120px] p-2 text-xs uppercase font-light">ACTIONS</TableHead>
                                                </TableRow>
                                            </TableHeader>
                                            <TableBody>
                                                {filteredRetryMessages.map((message) => {
                                                    const calculatedNextRetry = message.next_retry_at_ms;
                                                    return (
                                                    <TableRow key={message.id} className="text-xs">
                                                        <TableCell className="p-2">
                                                            <input
                                                                type="checkbox"
                                                                className="h-4 w-4"
                                                                checked={selectedRetryMessages.includes(message.id)}
                                                                onChange={(e) => {
                                                                    if (e.target.checked) {
                                                                        setSelectedRetryMessages([...selectedRetryMessages, message.id]);
                                                                    } else {
                                                                        setSelectedRetryMessages(selectedRetryMessages.filter(id => id !== message.id));
                                                                    }
                                                                }}
                                                            />
                                                        </TableCell>
                                                        <TableCell className="p-2 font-mono">{decodeMessageId(message.id)}</TableCell>
                                                        <TableCell className="p-2">
                                                            <Badge variant="outline" className="text-xs">
                                                                {message.group}
                                                            </Badge>
                                                        </TableCell>
                                                        <TableCell className="p-2">
                                                            <div className="space-y-1">
                                                                <div className="text-xs">{message.retry_count}/{message.max_retries}</div>
                                                                <Progress
                                                                    value={getRetryProgress(message.retry_count, message.max_retries)}
                                                                    className="h-1"
                                                                />
                                                            </div>
                                                        </TableCell>
                                                        <TableCell className="p-2 font-mono">{formatTime(calculatedNextRetry)}</TableCell>
                                                        <TableCell className="p-2 text-red-600">{message.last_error}</TableCell>
                                                        <TableCell className="p-2 font-mono">{message.partition}</TableCell>
                                                        <TableCell className="p-2">
                                                            <div className="max-w-[200px]">
                                                                <div className="truncate text-xs" title={message.payload}>
                                                                    {message.payload}
                                                                </div>
                                                                <div className="text-xs text-muted-foreground mt-1">
                                                                    Headers: {message.headers ? Object.keys(message.headers).length : 0} items
                                                                    {showQueueNames && (
                                                                        <div className="mt-1 text-red-600">
                                                                            DLQ: dlq/{stream}/{message.group}
                                                                        </div>
                                                                    )}
                                                                </div>
                                                            </div>
                                                        </TableCell>
                                                        <TableCell className="p-2">
                                                            <div className="flex space-x-1">
                                                                <Button variant="ghost" size="sm" className="h-6 w-6 p-0">
                                                                    <Eye className="h-3 w-3" />
                                                                </Button>
                                                                <Button variant="ghost" size="sm" className="h-6 w-6 p-0">
                                                                    <RotateCcw className="h-3 w-3" />
                                                                </Button>
                                                                <Button variant="ghost" size="sm" className="h-6 w-6 p-0 text-red-600">
                                                                    <Trash2 className="h-3 w-3" />
                                                                </Button>
                                                            </div>
                                                        </TableCell>
                                                    </TableRow>
                                                    );
                                                })}
                                            </TableBody>
                                        </Table>
                                    </div>
                                )}

                                {selectedRetryMessages.length > 0 && (
                                    <div className="p-4 border-t bg-muted/20">
                                        <div className="flex items-center justify-between">
                                            <span className="text-sm text-muted-foreground">
                                                {selectedRetryMessages.length} message(s) selected
                                            </span>
                                            <div className="flex space-x-2">
                                                <Button variant="outline" size="sm">
                                                    <RotateCcw className="h-4 w-4 mr-2" />
                                                    Retry Now
                                                </Button>
                                                <Button variant="outline" size="sm">
                                                    <Pause className="h-4 w-4 mr-2" />
                                                    Pause
                                                </Button>
                                                <Button variant="destructive" size="sm">
                                                    <Trash2 className="h-4 w-4 mr-2" />
                                                    Delete
                                                </Button>
                                            </div>
                                        </div>
                                    </div>
                                )}
                            </CardContent>
                        </Card>
                    </TabsContent>

                    {/* Dead Letter Queue Tab */}
                    <TabsContent value="dlq" className="space-y-4">
                        <Card className="border-none p-0">
                            {filteredDLQMessages.length > 0 && (
                                <CardHeader className="px-0">
                                    <div className="flex items-center justify-between">
                                        <div>
                                            <CardTitle className="text-sm">Dead Letter Queue ({filteredDLQMessages.length})</CardTitle>
                                            <CardDescription className="text-xs">
                                                Messages that have exceeded maximum retry attempts for {namespace}/{stream}
                                            </CardDescription>
                                        </div>
                                        <div className="flex space-x-2">
                                            <RetryQueueFilter
                                                value={dlqFilter}
                                                tags={dlqFilterTags}
                                                groups={availableGroups}
                                                onApply={(expr, tags) => {
                                                    setDlqFilter(expr);
                                                    setDlqFilterTags(tags);
                                                    parseFilterExpression(expr, false);
                                                }}
                                            />
                                        </div>
                                    </div>
                                </CardHeader>
                            )}
                            <CardContent className="p-0">
                                {filteredDLQMessages.length === 0 ? (
                                    <div className="flex items-center justify-center py-12">
                                        <div className="text-center">
                                            <div className="text-sm text-muted-foreground mb-2">No DLQ messages found</div>
                                            <div className="text-xs text-muted-foreground">
                                                Messages will appear here when they exceed maximum retry attempts.
                                            </div>
                                        </div>
                                    </div>
                                ) : (
                                    <div className="overflow-x-auto rounded-lg border border-gray-200">
                                        <Table className="table-fixed w-full">
                                            <TableHeader>
                                                <TableRow className="bg-muted/40">
                                                    <TableHead className="w-12 p-2">
                                                        <input
                                                            type="checkbox"
                                                            className="h-4 w-4"
                                                            onChange={(e) => {
                                                                if (e.target.checked) {
                                                                    setSelectedDLQMessages(dlqMessages.map(m => m.id));
                                                                } else {
                                                                    setSelectedDLQMessages([]);
                                                                }
                                                            }}
                                                        />
                                                    </TableHead>
                                                    <TableHead className="w-[80px] p-2 text-xs uppercase font-light">ID</TableHead>
                                                    <TableHead className="w-[120px] p-2 text-xs uppercase font-light">GROUP</TableHead>
                                                    <TableHead className="w-[120px] p-2 text-xs uppercase font-light">FAILED AT</TableHead>
                                                    <TableHead className="w-[200px] p-2 text-xs uppercase font-light">ERROR</TableHead>
                                                    <TableHead className="w-[100px] p-2 text-xs uppercase font-light">PARTITION</TableHead>
                                                    <TableHead className="p-2 text-xs uppercase font-light">PAYLOAD</TableHead>
                                                    <TableHead className="w-[120px] p-2 text-xs uppercase font-light">ACTIONS</TableHead>
                                                </TableRow>
                                            </TableHeader>
                                            <TableBody>
                                                {filteredDLQMessages.map((message) => (
                                                    <TableRow key={message.id} className="text-xs">
                                                        <TableCell className="p-2">
                                                            <input
                                                                type="checkbox"
                                                                className="h-4 w-4"
                                                                checked={selectedDLQMessages.includes(message.id)}
                                                                onChange={(e) => {
                                                                    if (e.target.checked) {
                                                                        setSelectedDLQMessages([...selectedDLQMessages, message.id]);
                                                                    } else {
                                                                        setSelectedDLQMessages(selectedDLQMessages.filter(id => id !== message.id));
                                                                    }
                                                                }}
                                                            />
                                                        </TableCell>
                                                        <TableCell className="p-2 font-mono">{decodeMessageId(message.id)}</TableCell>
                                                        <TableCell className="p-2">
                                                            <Badge variant="outline" className="text-xs">
                                                                {message.group}
                                                            </Badge>
                                                        </TableCell>
                                                        <TableCell className="p-2 font-mono">
                                                            {new Date(message.failed_at_ms).toLocaleTimeString()}
                                                        </TableCell>
                                                        <TableCell className="p-2 text-red-600">{message.last_error}</TableCell>
                                                        <TableCell className="p-2 font-mono">{message.partition}</TableCell>
                                                        <TableCell className="p-2">
                                                            <div className="max-w-[200px]">
                                                                <div className="truncate text-xs" title={message.payload}>
                                                                    {message.payload}
                                                                </div>
                                                                <div className="text-xs text-muted-foreground mt-1">
                                                                    Headers: {message.headers ? Object.keys(message.headers).length : 0} items
                                                                    {showQueueNames && (
                                                                        <div className="mt-1 text-red-600">
                                                                            DLQ: dlq/{stream}/{message.group}
                                                                        </div>
                                                                    )}
                                                                </div>
                                                            </div>
                                                        </TableCell>
                                                        <TableCell className="p-2">
                                                            <div className="flex space-x-1">
                                                                <Button variant="ghost" size="sm" className="h-6 w-6 p-0">
                                                                    <Eye className="h-3 w-3" />
                                                                </Button>
                                                                <Button variant="ghost" size="sm" className="h-6 w-6 p-0">
                                                                    <RotateCcw className="h-3 w-3" />
                                                                </Button>
                                                                <Button variant="ghost" size="sm" className="h-6 w-6 p-0 text-red-600">
                                                                    <Trash2 className="h-3 w-3" />
                                                                </Button>
                                                            </div>
                                                        </TableCell>
                                                    </TableRow>
                                                ))}
                                            </TableBody>
                                        </Table>
                                    </div>
                                )}

                                {selectedDLQMessages.length > 0 && (
                                    <div className="p-4 border-t bg-muted/20">
                                        <div className="flex items-center justify-between">
                                            <span className="text-sm text-muted-foreground">
                                                {selectedDLQMessages.length} message(s) selected
                                            </span>
                                            <div className="flex space-x-2">
                                                <Button variant="outline" size="sm">
                                                    <RotateCcw className="h-4 w-4 mr-2" />
                                                    Requeue
                                                </Button>
                                                <Button variant="destructive" size="sm">
                                                    <Trash2 className="h-4 w-4 mr-2" />
                                                    Purge
                                                </Button>
                                            </div>
                                        </div>
                                    </div>
                                )}
                            </CardContent>
                        </Card>
                    </TabsContent>
                </Tabs>
            </div>
        </div>
    );
}


