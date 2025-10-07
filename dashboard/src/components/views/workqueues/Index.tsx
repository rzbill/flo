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
import { Search, Plus, Package } from 'lucide-react';
import { toast } from 'sonner';
import { useWorkQueues, useCreateWorkQueue, WorkQueue } from '../../../hooks/useWorkQueues';
import { useNamespaces } from '../../../hooks/useStreams';
import { useFormatting } from '../../../hooks/useFormatting';
import { useAppStore } from '../../../store/app';

interface WorkQueuesViewProps {
  onNavigate: (view: 'workqueues' | 'workqueue-details', namespace?: string, workqueue?: string, tab?: string) => void;
}

export const WorkQueuesView = React.memo(function WorkQueuesView({ onNavigate }: WorkQueuesViewProps) {
  const [searchParams] = useSearchParams();
  const urlNamespace = searchParams.get('namespace') || 'default';
  const { namespace, setNamespace } = useAppStore();
  
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [newQueueName, setNewQueueName] = useState('');
  const [newQueuePartitions, setNewQueuePartitions] = useState('16');
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedNamespace, setSelectedNamespace] = useState(urlNamespace || namespace);

  const [namespaces, setNamespaces] = useState(['all', 'default'] as string[]);
  const { data: nsData } = useNamespaces();

  // Use custom hooks
  const { formatNumber } = useFormatting();
  
  // TanStack Query hooks
  const { 
    data: workQueuesData, 
    isLoading: workQueuesLoading, 
    error: workQueuesError 
  } = useWorkQueues(selectedNamespace);
  
  const createWorkQueueMutation = useCreateWorkQueue();

  // Memoized values
  const workQueues = useMemo(() => {
    if (!workQueuesData?.workqueues) return [];
    if (!Array.isArray(workQueuesData.workqueues)) return [];
    
    // Convert string array to WorkQueue objects
    return workQueuesData.workqueues.map((name: string): WorkQueue => ({
      name,
      partitions: 16, // Default partitions
      namespace: selectedNamespace
    }));
  }, [workQueuesData, selectedNamespace]);

  // Load namespaces via hook
  useEffect(() => {
    const list = Array.isArray(nsData?.namespaces) ? (nsData!.namespaces as string[]) : [];
    if (list.length) setNamespaces((['all', ...list] as string[]));
  }, [nsData]);

  // Sync selected namespace to global store
  useEffect(() => {
    if (selectedNamespace !== 'all') {
      setNamespace(selectedNamespace);
    }
  }, [selectedNamespace, setNamespace]);

  // Filter workqueues
  const filteredWorkQueues = useMemo(() => {
    if (!searchTerm) return workQueues;
    const term = searchTerm.toLowerCase();
    return workQueues.filter(wq => wq.name.toLowerCase().includes(term));
  }, [workQueues, searchTerm]);

  // Handlers
  const handleCreateQueue = useCallback(() => {
    if (!newQueueName.trim()) {
      toast.error('Queue name is required');
      return;
    }

    const partitions = parseInt(newQueuePartitions, 10);
    if (isNaN(partitions) || partitions < 1 || partitions > 256) {
      toast.error('Partitions must be between 1 and 256');
      return;
    }

    createWorkQueueMutation.mutate(
      {
        namespace: selectedNamespace,
        name: newQueueName.trim(),
        partitions
      },
      {
        onSuccess: () => {
          toast.success(`WorkQueue "${newQueueName}" created`);
          setShowCreateModal(false);
          setNewQueueName('');
          setNewQueuePartitions('16');
        },
        onError: (error: Error) => {
          toast.error(`Failed to create workqueue: ${error.message}`);
        }
      }
    );
  }, [newQueueName, newQueuePartitions, selectedNamespace, createWorkQueueMutation]);

  const handleRowClick = useCallback((wq: WorkQueue) => {
    onNavigate('workqueue-details', wq.namespace, wq.name, 'overview');
  }, [onNavigate]);

  // Loading state
  if (workQueuesLoading) {
    return (
      <div className="p-6 space-y-4">
        <div className="animate-pulse space-y-4">
          <div className="h-8 bg-muted rounded w-48" />
          <div className="h-12 bg-muted rounded" />
          <div className="h-64 bg-muted rounded" />
        </div>
      </div>
    );
  }

  // Error state
  if (workQueuesError) {
    return (
      <div className="p-6">
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 p-6">
          <h3 className="font-semibold text-destructive mb-2">Error Loading WorkQueues</h3>
          <p className="text-sm text-muted-foreground">
            {workQueuesError.message || 'An unknown error occurred'}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-4 mx-auto lg:max-w-3xl xl:max-w-4xl">
      {/* Header */}
      <div className="flex items-end justify-between py-6 my-12 border-b-1">
        <h1 className="text-2xl md:text-3xl font-light">WorkQueues</h1>
        <div className="flex gap-8 md:gap-10">
          <div>
            <div className="uppercase text-[11px] tracking-wide text-muted-foreground mb-1">Total Queues</div>
            <div className="text-2xl font-light leading-none">{formatNumber(workQueues.length)}</div>
          </div>
        </div>
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-3">
        {/* Namespace Selector */}
        <Select value={selectedNamespace} onValueChange={setSelectedNamespace}>
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="Select namespace" />
          </SelectTrigger>
          <SelectContent>
            {namespaces.map((ns) => (
              <SelectItem key={ns} value={ns}>
                {ns}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        {/* Search */}
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search workqueues..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="pl-9"
          />
        </div>

        {/* Create Button */}
        <Button onClick={() => setShowCreateModal(true)}>
          <Plus className="h-4 w-4 mr-2" />
          Create
        </Button>
      </div>

      {/* WorkQueues Table */}
      {filteredWorkQueues.length === 0 ? (
        <div className="border rounded-lg p-12 text-center">
          <Package className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-lg font-semibold mb-2">No WorkQueues Found</h3>
          <p className="text-sm text-muted-foreground mb-6">
            {searchTerm
              ? 'No workqueues match your search criteria.'
              : 'Get started by creating your first workqueue.'}
          </p>
          {!searchTerm && (
            <Button onClick={() => setShowCreateModal(true)}>
              <Plus className="h-4 w-4 mr-2" />
              Create WorkQueue
            </Button>
          )}
        </div>
      ) : (
        <div className="border rounded-lg overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Partitions</TableHead>
                <TableHead>Namespace</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredWorkQueues.map((wq) => (
                <TableRow
                  key={`${wq.namespace}:${wq.name}`}
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() => handleRowClick(wq)}
                >
                  <TableCell className="font-medium">{wq.name}</TableCell>
                  <TableCell>{wq.partitions}</TableCell>
                  <TableCell className="text-muted-foreground">{wq.namespace}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}

      {/* Create WorkQueue Modal */}
      <Sheet open={showCreateModal} onOpenChange={setShowCreateModal}>
        <SheetContent>
          <SheetHeader>
            <SheetTitle>Create WorkQueue</SheetTitle>
            <SheetDescription>
              Create a new workqueue for task processing
            </SheetDescription>
          </SheetHeader>

          <div className="space-y-6 py-6">
            <div className="space-y-2">
              <label className="text-sm font-medium">Name</label>
              <Input
                placeholder="payments"
                value={newQueueName}
                onChange={(e) => setNewQueueName(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleCreateQueue()}
              />
              <p className="text-xs text-muted-foreground">
                A unique name for your workqueue
              </p>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Partitions</label>
              <Input
                type="number"
                min="1"
                max="256"
                placeholder="16"
                value={newQueuePartitions}
                onChange={(e) => setNewQueuePartitions(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Number of partitions (1-256). More partitions allow higher parallelism.
              </p>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Namespace</label>
              <div className="text-sm text-muted-foreground">
                {selectedNamespace}
              </div>
            </div>
          </div>

          <SheetFooter>
            <Button
              variant="outline"
              onClick={() => setShowCreateModal(false)}
              disabled={createWorkQueueMutation.isPending}
            >
              Cancel
            </Button>
            <Button
              onClick={handleCreateQueue}
              disabled={createWorkQueueMutation.isPending || !newQueueName.trim()}
            >
              {createWorkQueueMutation.isPending ? 'Creating...' : 'Create'}
            </Button>
          </SheetFooter>
        </SheetContent>
      </Sheet>
    </div>
  );
});
