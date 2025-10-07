import React, { useEffect, useRef, useState, useMemo, useCallback, Suspense } from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import { useAppStore } from '../../../store/app';;
import { Button } from '../../ui/button';
import { Input } from '../../ui/input';
import { Textarea } from '../../ui/textarea';
import { AlertDialog, AlertDialogContent, AlertDialogHeader, AlertDialogTitle, AlertDialogDescription, AlertDialogFooter, AlertDialogCancel } from '../../ui/alert-dialog';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '../../ui/tabs';
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetFooter } from '../../ui/sheet';
import {
  Plus,
  Send,
  Trash2,
  Trash,
  ArrowLeft
} from 'lucide-react';
import { toast } from 'sonner';
const StreamDetailsPerPartitionView = React.lazy(() => import('./PartitionView'));
const StreamsRetryDLQ = React.lazy(() => import('./RetryDLQ'));
const StreamOverview = React.lazy(() => import('./Overview'));
const StreamsSubscriptions = React.lazy(() => import('./Subscriptions'));
import { StreamsMessages } from './Messages';
import { useStreamStats, useFlushStream, usePublishMessage } from '../../../hooks/useStreams';
import { useFormatting } from '../../../hooks/useFormatting';

interface StreamDetailsViewProps {
  onNavigate: (view: import('../../../types/navigation').View, namespace?: string, stream?: string, tab?: string) => void;
}

interface HeaderEntry {
  key: string;
  value: string;
}

export const StreamDetailsView = React.memo(function StreamDetailsView({ onNavigate }: StreamDetailsViewProps) {
  const { tab } = useParams<{ tab: string }>();
  const [searchParams] = useSearchParams();
  const { setNamespace } = useAppStore();

  const namespace = searchParams.get('namespace') || '';
  const stream = searchParams.get('stream') || '';
  const defaultTab = tab || 'messages';
  const [payload, setPayload] = useState('');
  const [partitionKey, setPartitionKey] = useState('');
  const [headers, setHeaders] = useState([{ key: '', value: '' }] as HeaderEntry[]);
  const [showFlushConfirm, setShowFlushConfirm] = useState(false);
  const [showPublishModal, setShowPublishModal] = useState(false);

  // Sync URL namespace into global store
  useEffect(() => {
    if (namespace) setNamespace(namespace);
  }, [namespace, setNamespace]);

  // Use custom hooks
  const { formatNumber, formatBytes, formatRelativeTime } = useFormatting();

  // TanStack Query hooks
  const {
    data: statsData,
    isLoading: statsLoading,
    error: statsError
  } = useStreamStats(namespace, stream);

  const flushStreamMutation = useFlushStream();
  const publishMessageMutation = usePublishMessage();

  // Memoized values
  const partitions = useMemo(() => statsData?.partitions || [], [statsData]);

  const relTime = useCallback((ms?: number) => ms ? formatRelativeTime(ms) : '–', [formatRelativeTime]);

  const handlePublish = useCallback(async () => {
    if (!payload.trim()) {
      toast.error('Payload is required');
      return;
    }

    try {
      const headersMap: Record<string, string> = {};
      for (const h of headers) {
        if (h.key) headersMap[h.key] = h.value;
      }

      await publishMessageMutation.mutateAsync({
        namespace,
        stream,
        payload,
        headers: headersMap,
        key: partitionKey
      });

      const eventId = `accepted_${Date.now()}`;
      toast.success('Event published successfully', {
        description: `Event ID: ${eventId}`,
        action: { label: 'Copy ID', onClick: () => navigator.clipboard.writeText(eventId) }
      });
      setPayload('');
      setPartitionKey('');
      setHeaders([{ key: '', value: '' }]);
    } catch (error) {
      toast.error('Failed to publish event');
    }
  }, [payload, headers, partitionKey, namespace, stream, publishMessageMutation]);

  const handleFlush = useCallback(async () => {
    try {
      const data = await flushStreamMutation.mutateAsync({
        namespace,
        stream
      });

      toast.success(`Stream flushed successfully`, {
        description: `${data.deleted_count} messages deleted`,
      });
      setShowFlushConfirm(false);
    } catch (error) {
      toast.error('Failed to flush stream', {
        description: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  }, [namespace, stream, flushStreamMutation]);

  const addHeader = () => {
    setHeaders([...headers, { key: '', value: '' }]);
  };

  const updateHeader = (index: number, field: 'key' | 'value', value: string) => {
    const newHeaders = [...headers];
    newHeaders[index][field] = value;
    setHeaders(newHeaders);
  };

  const removeHeader = (index: number) => {
    if (headers.length > 1) {
      setHeaders(headers.filter((_, i) => i !== index));
    }
  };

  return (
    <div className="h-screen flex flex-col">
      {/* Header Section - Larger */}
      <div className="flex-shrink-0 bg-muted/50">
        <div className="px-6 py-4">
          <div className="flex items-center gap-3">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => onNavigate('streams', namespace)}
              className=" text-gray-500 hover:text-gray-700 cursor-pointer"
              title="Back to Streams"
            >
              <ArrowLeft className="w-4 h-4" />
            </Button>
            <h1 className="text-xl font-normal text-gray-900">{namespace}/{stream}</h1>
          </div>
        </div>
      </div>

      {/* Tabs Section */}
      <Tabs value={defaultTab} onValueChange={(value) => onNavigate('stream-details', namespace, stream, value)} className="flex-1 flex flex-col">
        <div className="flex-shrink-0 border-b bg-muted/30">
          <div className="px-6 py-3">
            <div className="flex items-center justify-between">
              <TabsList className="h-8 bg-transparent p-0 gap-2">
              
                <TabsTrigger
                  value="overview"
                  className="h-8 px-4 text-sm font-medium bg-transparent text-gray-700 hover:text-gray-900 data-[state=active]:bg-gray-900 data-[state=active]:text-white data-[state=active]:rounded-md transition-all duration-200"
                >
                  Overview
                </TabsTrigger>
                <TabsTrigger
                  value="messages"
                  className="h-8 px-4 text-xs font-medium bg-transparent text-gray-700 hover:text-gray-900 data-[state=active]:bg-gray-900 data-[state=active]:text-white data-[state=active]:rounded-md transition-all duration-200"
                >
                  Messages
                </TabsTrigger>
                <TabsTrigger
                  value="retry-dlq"
                  className="h-8 px-4 text-xs font-medium bg-transparent text-gray-700 hover:text-gray-900 data-[state=active]:bg-gray-900 data-[state=active]:text-white data-[state=active]:rounded-md transition-all duration-200"
                >
                  Retry & DLQ
                </TabsTrigger>
                <TabsTrigger
                  value="subscriptions"
                  className="h-8 px-4 text-xs font-medium bg-transparent text-gray-700 hover:text-gray-900 data-[state=active]:bg-gray-900 data-[state=active]:text-white data-[state=active]:rounded-md transition-all duration-200"
                >
                  Subscriptions
                </TabsTrigger>
                <TabsTrigger
                  value="partitions"
                  className="h-8 px-4 text-xs font-medium bg-transparent text-gray-700 hover:text-gray-900 data-[state=active]:bg-gray-900 data-[state=active]:text-white data-[state=active]:rounded-md transition-all duration-200"
                >
                  Partitions
                </TabsTrigger>
              </TabsList>

              {/* Right Side Actions */}
              <div className="flex items-center gap-2">
                <Button
                  variant="default"
                  size="sm"
                  onClick={() => setShowPublishModal(true)}
                  className="text-xs font-medium bg-gray-900 hover:bg-gray-800 text-white cursor-pointer"
                >
                  <Send className="w-3.5 h-3.5 mr-1.5" />
                  Publish
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setShowFlushConfirm(true)}
                  disabled={flushStreamMutation.isPending || (statsData?.total_count || 0) === 0}
                  className="text-xs font-medium text-red-600 hover:text-red-700 hover:bg-red-50 cursor-pointer"
                >
                  <Trash className="w-3.5 h-3.5 mr-1.5" />
                  {flushStreamMutation.isPending ? 'Flushing...' : 'Flush Stream'}
                </Button>
              </div>
            </div>
          </div>
        </div>

        {/* Content Section - Flexible Height with Scroll */}
        <div className="flex-1 overflow-hidden">
          <TabsContent value="overview" className="h-full m-0">
            <Suspense fallback={<div className="p-6 text-sm text-muted-foreground">Loading Overview…</div>}>
              <StreamOverview
                namespace={namespace}
                stream={stream}
                onNavigate={onNavigate}
                onShowPublishModal={() => setShowPublishModal(true)}
              />
            </Suspense>
          </TabsContent>

          <TabsContent value="messages" className="h-full m-0">
            <StreamsMessages namespace={namespace} stream={stream} />
          </TabsContent>

          <TabsContent value="retry-dlq" className="h-full m-0">
            <Suspense fallback={<div className="p-6 text-sm text-muted-foreground">Loading Retry & DLQ…</div>}>
              <StreamsRetryDLQ namespace={namespace} stream={stream} />
            </Suspense>
          </TabsContent>

          <TabsContent value="subscriptions" className="h-full m-0">
            <Suspense fallback={<div className="p-6 text-sm text-muted-foreground">Loading Subscriptions…</div>}>
              <StreamsSubscriptions namespace={namespace} stream={stream} />
            </Suspense>
          </TabsContent>

          <TabsContent value="partitions" className="h-full m-0">
            <Suspense fallback={<div className="p-6 text-sm text-muted-foreground">Loading Partitions…</div>}>
              <StreamDetailsPerPartitionView
                partitions={partitions}
                formatNumber={formatNumber}
                formatBytes={formatBytes}
                relTime={relTime}
              />
            </Suspense>
          </TabsContent>
        </div>
      </Tabs>

      {/* Publish Modal - Right Side Sheet */}
      <Sheet open={showPublishModal} onOpenChange={setShowPublishModal}>
        <SheetContent side="right" className="w-full max-w-2xl sm:max-w-xl">
          <SheetHeader className="pb-6 border-b">
            <SheetTitle className="text-xl font-semibold text-gray-900">Publish Message</SheetTitle>
            <p className="text-sm text-gray-600">Send a message to the {namespace}/{stream} stream</p>
          </SheetHeader>

          <div className="flex-1 space-y-6 overflow-y-auto px-5">
            {/* Payload Section */}
            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="text-sm font-semibold text-gray-900">Payload</label>
                <span className="text-xs text-gray-500">Required</span>
              </div>
              <div className="relative">
                <Textarea
                  placeholder="Enter your event payload (JSON, text, etc.)"
                  value={payload}
                  onChange={(e) => setPayload(e.target.value)}
                  className="min-h-[200px] shadow-none font-mono text-xs placeholder:text-xs placeholder:text-gray-400 resize-none border-gray-200 focus:border-blue-500 focus:ring-blue-500"
                />
                <div className="absolute bottom-2 right-2 text-xs text-gray-400">
                  {payload.length} characters
                </div>
              </div>
            </div>

            {/* Partition Key Section */}
            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="text-sm font-semibold text-gray-900">Partition Key</label>
                <span className="text-xs text-gray-500">Optional</span>
              </div>
              <Input
                placeholder="Key for consistent partitioning"
                value={partitionKey}
                onChange={(e) => setPartitionKey(e.target.value)}
                className="text-sm placeholder:text-gray-400 border-gray-200 focus:border-blue-500 focus:ring-blue-500"
              />
              <p className="text-xs text-gray-500 mt-1">
                Messages with the same partition key will be delivered to the same partition
              </p>
            </div>

            {/* Headers Section */}
            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="text-sm font-semibold text-gray-900">Headers</label>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-gray-500">Optional</span>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={addHeader}
                    className="h-8 px-3 text-xs gap-1.5 border-gray-200 cursor-pointer"
                  >
                    <Plus className="w-4 h-4" />
                    Add Header
                  </Button>
                </div>
              </div>
              
              <div className="space-y-2">
                {headers.map((header, index) => (
                  <div key={index} className="flex items-center gap-2">
                    <Input
                      placeholder="Header key"
                      value={header.key}
                      onChange={(e) => updateHeader(index, 'key', e.target.value)}
                      className="flex-1 text-xs placeholder:text-gray-400 border-gray-200 focus:border-blue-500 focus:ring-blue-500"
                    />
                    <Input
                      placeholder="Header value"
                      value={header.value}
                      onChange={(e) => updateHeader(index, 'value', e.target.value)}
                      className="flex-1 text-xs placeholder:text-gray-400 border-gray-200 focus:border-blue-500 focus:ring-blue-500"
                    />
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => removeHeader(index)}
                      disabled={headers.length === 1}
                      className="h-8 w-8 p-0 text-gray-400 hover:text-red-500 hover:bg-red-50 cursor-pointer"
                    >
                      <Trash2 className="w-4 h-4" />
                    </Button>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <SheetFooter className="pt-6 border-t border-gray-200">
            <div className="flex gap-3 w-full">
              <div className="flex-1"></div>
              <Button
                variant="outline"
                onClick={() => setShowPublishModal(false)}
                className="flex-1 text-xs h-10 cursor-pointer"
              >
                Cancel
              </Button>
              <Button
                onClick={async () => {
                  await handlePublish();
                  setShowPublishModal(false);
                }}
                disabled={!payload.trim() || publishMessageMutation.isPending}
                className="flex-1 text-xs h-10 cursor-pointer"
              >
                <Send className="w-4 h-4 mr-2" />
                {publishMessageMutation.isPending ? 'Publishing...' : 'Publish Message'}
              </Button>
            </div>
          </SheetFooter>
        </SheetContent>
      </Sheet>

      {/* Flush Confirmation Dialog - Shadcn AlertDialog */}
      <AlertDialog open={showFlushConfirm} onOpenChange={setShowFlushConfirm}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2 text-red-600">
              <Trash className="w-5 h-5" />
              Flush Stream
            </AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete all messages from <strong>{namespace}/{stream}</strong>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-800 rounded-lg p-4">
            <div className="text-sm text-red-800 dark:text-red-200">
              <strong>Warning:</strong> This action cannot be undone. All {formatNumber(statsData?.total_count || 0)} messages
              ({formatBytes(statsData?.total_bytes || 0)}) will be permanently deleted.
            </div>
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={flushStreamMutation.isPending}>Cancel</AlertDialogCancel>
            <Button
              variant="destructive"
              onClick={handleFlush}
              disabled={flushStreamMutation.isPending}
              className="gap-2"
            >
              {flushStreamMutation.isPending ? (
                <>
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  Flushing...
                </>
              ) : (
                <>
                  <Trash className="w-4 h-4" />
                  Flush Stream
                </>
              )}
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div >
  );
});