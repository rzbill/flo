import React, { useState, useMemo, Suspense } from 'react';
import { useParams } from 'react-router-dom';
import { useAppStore } from '../../../store/app';
import { Button } from '../../ui/button';
import { Input } from '../../ui/input';
import { Textarea } from '../../ui/textarea';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '../../ui/tabs';
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetFooter, SheetDescription } from '../../ui/sheet';
import { ArrowLeft, Plus, Send } from 'lucide-react';
import { toast } from 'sonner';
import { useEnqueueMessage, useWorkQueueStats } from '../../../hooks/useWorkQueues';
import { useFormatting } from '../../../hooks/useFormatting';

const WorkQueueOverview = React.lazy(() => import('./Overview'));
const WorkQueueMessages = React.lazy(() => import('./Messages'));
const WorkQueueConsumers = React.lazy(() => import('./Consumers'));

interface WorkQueueDetailsViewProps {
  onNavigate: (view: 'workqueues' | 'workqueue-details', namespace?: string, workqueue?: string, tab?: string) => void;
}

interface HeaderEntry {
  key: string;
  value: string;
}

export const WorkQueueDetailsView = React.memo(function WorkQueueDetailsView({ onNavigate }: WorkQueueDetailsViewProps) {
  const { tab } = useParams<{ tab: string }>();
  const { namespace } = useAppStore();
  
  const [searchParams, setSearchParams] = React.useState(() => {
    const params = new URLSearchParams(window.location.search);
    return {
      namespace: params.get('namespace') || namespace,
      workqueue: params.get('workqueue') || ''
    };
  });

  const workqueue = searchParams.workqueue;
  const currentTab = tab || 'overview';

  const [showEnqueueModal, setShowEnqueueModal] = useState(false);
  const [enqueueData, setEnqueueData] = useState('');
  const [enqueuePriority, setEnqueuePriority] = useState('0');
  const [enqueueDelay, setEnqueueDelay] = useState('0');
  const [enqueueKey, setEnqueueKey] = useState('');
  const [headers, setHeaders] = useState<HeaderEntry[]>([]);

  const { formatNumber, formatBytes } = useFormatting();
  const { data: stats } = useWorkQueueStats(namespace, workqueue);
  const enqueueMutation = useEnqueueMessage();

  const handleBack = () => {
    onNavigate('workqueues', namespace);
  };

  const handleTabChange = (newTab: string) => {
    onNavigate('workqueue-details', namespace, workqueue, newTab);
  };

  const handleEnqueue = () => {
    if (!enqueueData.trim()) {
      toast.error('Message payload is required');
      return;
    }

    const priority = parseInt(enqueuePriority, 10);
    const delay = parseInt(enqueueDelay, 10);

    if (isNaN(priority) || priority < 0) {
      toast.error('Priority must be a non-negative number');
      return;
    }

    if (isNaN(delay) || delay < 0) {
      toast.error('Delay must be a non-negative number');
      return;
    }

    // Build headers object
    const headersObj: Record<string, string> = {};
    headers.forEach(h => {
      if (h.key.trim() && h.value.trim()) {
        headersObj[h.key.trim()] = h.value.trim();
      }
    });

    enqueueMutation.mutate(
      {
        namespace,
        name: workqueue,
        payload: enqueueData,
        headers: Object.keys(headersObj).length > 0 ? headersObj : undefined,
        priority: priority !== 0 ? priority : undefined,
        delay_ms: delay !== 0 ? delay : undefined,
        key: enqueueKey.trim() || undefined
      },
      {
        onSuccess: (data) => {
          toast.success('Message enqueued successfully');
          setShowEnqueueModal(false);
          setEnqueueData('');
          setEnqueuePriority('0');
          setEnqueueDelay('0');
          setEnqueueKey('');
          setHeaders([]);
        },
        onError: (error: Error) => {
          toast.error(`Failed to enqueue message: ${error.message}`);
        }
      }
    );
  };

  const addHeader = () => {
    setHeaders([...headers, { key: '', value: '' }]);
  };

  const removeHeader = (index: number) => {
    setHeaders(headers.filter((_, i) => i !== index));
  };

  const updateHeader = (index: number, field: 'key' | 'value', value: string) => {
    const newHeaders = [...headers];
    newHeaders[index][field] = value;
    setHeaders(newHeaders);
  };

  return (
    <Tabs value={currentTab} onValueChange={handleTabChange} className="h-full flex flex-col">
      {/* Fixed Header */}
      <div className="flex-none border-b">
        <div className="px-6 py-4">
          {/* Breadcrumb */}
          <div className="flex items-center gap-2 text-sm text-muted-foreground mb-4">
            <Button
              variant="ghost"
              size="sm"
              onClick={handleBack}
              className="h-8 gap-2"
            >
              <ArrowLeft className="h-4 w-4" />
              WorkQueues
            </Button>
            <span>/</span>
            <span className="text-foreground font-medium">{workqueue}</span>
          </div>

          {/* Title and Tabs */}
          <div className="flex items-end justify-between">
            <div className="flex-1">
              <h1 className="text-2xl font-semibold mb-4">{workqueue}</h1>
              <TabsList>
                <TabsTrigger value="overview">Overview</TabsTrigger>
                <TabsTrigger value="messages">Messages</TabsTrigger>
                <TabsTrigger value="consumers">Consumers</TabsTrigger>
              </TabsList>
            </div>
            
            {/* Stats Summary */}
            {stats && (
              <div className="flex gap-6 text-sm">
                <div>
                  <div className="text-muted-foreground mb-1">Pending</div>
                  <div className="text-xl font-semibold">{formatNumber(stats.pending_count || 0)}</div>
                </div>
                <div>
                  <div className="text-muted-foreground mb-1">In-Flight</div>
                  <div className="text-xl font-semibold">{formatNumber(stats.in_flight_count || 0)}</div>
                </div>
                <div>
                  <div className="text-muted-foreground mb-1">Completed</div>
                  <div className="text-xl font-semibold text-green-600">
                    {formatNumber(stats.total_completed || 0)}
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Content Section */}
      <div className="flex-1 overflow-hidden">
        <TabsContent value="overview" className="h-full m-0">
          <Suspense fallback={<div className="p-6 text-sm text-muted-foreground">Loading Overview…</div>}>
            <WorkQueueOverview
              namespace={namespace}
              workqueue={workqueue}
              onShowEnqueueModal={() => setShowEnqueueModal(true)}
            />
          </Suspense>
        </TabsContent>

        <TabsContent value="messages" className="h-full m-0 overflow-y-auto">
          <Suspense fallback={<div className="p-6 text-sm text-muted-foreground">Loading Messages…</div>}>
            <WorkQueueMessages namespace={namespace} workqueue={workqueue} />
          </Suspense>
        </TabsContent>

        <TabsContent value="consumers" className="h-full m-0">
          <Suspense fallback={<div className="p-6 text-sm text-muted-foreground">Loading Consumers…</div>}>
            <WorkQueueConsumers namespace={namespace} workqueue={workqueue} />
          </Suspense>
        </TabsContent>
      </div>

      {/* Enqueue Modal */}
      <Sheet open={showEnqueueModal} onOpenChange={setShowEnqueueModal}>
        <SheetContent className="sm:max-w-xl overflow-y-auto">
          <SheetHeader>
            <SheetTitle>Enqueue Message</SheetTitle>
            <SheetDescription>Add a new message to the workqueue</SheetDescription>
          </SheetHeader>

          <div className="space-y-6 py-6">
            {/* Payload */}
            <div className="space-y-2">
              <label className="text-sm font-medium">Payload *</label>
              <Textarea
                placeholder="Enter message payload (JSON, text, etc.)"
                value={enqueueData}
                onChange={(e) => setEnqueueData(e.target.value)}
                rows={6}
                className="font-mono text-sm"
              />
            </div>

            {/* Priority */}
            <div className="space-y-2">
              <label className="text-sm font-medium">Priority</label>
              <Input
                type="number"
                min="0"
                placeholder="0"
                value={enqueuePriority}
                onChange={(e) => setEnqueuePriority(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Higher values are processed first (default: 0)
              </p>
            </div>

            {/* Delay */}
            <div className="space-y-2">
              <label className="text-sm font-medium">Delay (ms)</label>
              <Input
                type="number"
                min="0"
                placeholder="0"
                value={enqueueDelay}
                onChange={(e) => setEnqueueDelay(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Delay before message becomes available (default: 0)
              </p>
            </div>

            {/* Partition Key */}
            <div className="space-y-2">
              <label className="text-sm font-medium">Partition Key (optional)</label>
              <Input
                placeholder="order-123"
                value={enqueueKey}
                onChange={(e) => setEnqueueKey(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Messages with same key go to same partition
              </p>
            </div>

            {/* Headers */}
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <label className="text-sm font-medium">Headers (optional)</label>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={addHeader}
                >
                  <Plus className="h-4 w-4 mr-1" />
                  Add Header
                </Button>
              </div>
              {headers.map((header, index) => (
                <div key={index} className="flex gap-2">
                  <Input
                    placeholder="key"
                    value={header.key}
                    onChange={(e) => updateHeader(index, 'key', e.target.value)}
                    className="flex-1"
                  />
                  <Input
                    placeholder="value"
                    value={header.value}
                    onChange={(e) => updateHeader(index, 'value', e.target.value)}
                    className="flex-1"
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    onClick={() => removeHeader(index)}
                  >
                    ×
                  </Button>
                </div>
              ))}
            </div>
          </div>

          <SheetFooter>
            <Button
              variant="outline"
              onClick={() => setShowEnqueueModal(false)}
              disabled={enqueueMutation.isPending}
            >
              Cancel
            </Button>
            <Button
              onClick={handleEnqueue}
              disabled={enqueueMutation.isPending || !enqueueData.trim()}
            >
              {enqueueMutation.isPending ? (
                'Enqueuing...'
              ) : (
                <>
                  <Send className="h-4 w-4 mr-2" />
                  Enqueue
                </>
              )}
            </Button>
          </SheetFooter>
        </SheetContent>
      </Sheet>
    </Tabs>
  );
});
