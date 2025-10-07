import React, { useEffect, useState } from 'react';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { 
  Dialog, 
  DialogContent, 
  DialogDescription, 
  DialogFooter, 
  DialogHeader, 
  DialogTitle 
} from '../ui/dialog';
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from '../ui/table';
import { Search, Plus, Database, ExternalLink } from 'lucide-react';
import { toast } from 'sonner';

interface Namespace {
  name: string;
  created: Date | null;
}

interface NamespacesViewProps {
  onNavigate: (view: import('../../types/navigation').View, namespace?: string, stream?: string, tab?: string) => void;
}

export function NamespacesView({ onNavigate }: NamespacesViewProps) {
  const [namespaces, setNamespaces] = useState<Namespace[]>([]);
  
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [newNamespaceName, setNewNamespaceName] = useState('');
  const [searchTerm, setSearchTerm] = useState('');
  const [isCreating, setIsCreating] = useState(false);

  useEffect(() => {
    (async () => {
      try {
        const res = await fetch('/v1/namespaces');
        const d = await res.json().catch(() => ({}));
        const list = Array.isArray(d?.namespaces) ? (d.namespaces as string[]) : [];
        setNamespaces(list.map(name => ({ name, created: null })));
      } catch {
        setNamespaces([]);
      }
    })();
  }, []);

  const filteredNamespaces = namespaces.filter(ns => 
    ns.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const handleCreateNamespace = async () => {
    if (!newNamespaceName.trim()) return;
    
    setIsCreating(true);
    try {
      await fetch('/v1/ns/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: newNamespaceName.trim() })
      });
      setNamespaces(prev => [...prev, { name: newNamespaceName.trim(), created: new Date() }]);
      toast.success(`Namespace "${newNamespaceName.trim()}" created successfully`);
    } catch {
      toast.error('Failed to create namespace');
    }
    setShowCreateModal(false);
    setNewNamespaceName('');
    setIsCreating(false);
  };

  const handleOpenStreams = (namespace: string) => {
    onNavigate('streams', namespace);
  };

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1>Namespaces</h1>
          <p className="text-muted-foreground">
            Organize streams by namespace for better isolation
          </p>
        </div>
        <Button onClick={() => setShowCreateModal(true)} className="gap-2">
          <Plus className="w-4 h-4" />
          Create Namespace
        </Button>
      </div>

      {/* Search */}
      <div className="flex items-center space-x-4">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <Input
            placeholder="Search namespaces..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="pl-10"
          />
        </div>
      </div>

      {/* Namespaces Table */}
      {filteredNamespaces.length > 0 ? (
        <div className="border rounded-lg">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Namespace</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredNamespaces.map((namespace) => (
                <TableRow key={namespace.name}>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <Database className="w-4 h-4 text-muted-foreground" />
                      <span className="font-medium">{namespace.name}</span>
                    </div>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {namespace.created ? namespace.created.toLocaleDateString() : '–'}
                  </TableCell>
                  <TableCell className="text-right">
                    <Button 
                      variant="outline" 
                      size="sm"
                      onClick={() => handleOpenStreams(namespace.name)}
                      className="gap-2"
                    >
                      <ExternalLink className="w-4 h-4" />
                      Open Streams
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      ) : (
        <div className="border rounded-lg p-12 text-center">
          <Database className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
          <h3 className="font-medium mb-2">
            {searchTerm ? 'No matching namespaces' : 'No namespaces yet'}
          </h3>
          <p className="text-muted-foreground mb-4">
            {searchTerm 
              ? 'Try adjusting your search terms'
              : 'Create your first namespace to organize streams'
            }
          </p>
          {!searchTerm && (
            <Button onClick={() => setShowCreateModal(true)} className="gap-2">
              <Plus className="w-4 h-4" />
              Create Namespace
            </Button>
          )}
        </div>
      )}

      {/* Create Namespace Modal */}
      <Dialog open={showCreateModal} onOpenChange={setShowCreateModal}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Namespace</DialogTitle>
            <DialogDescription>
              Namespaces provide logical isolation for streams with independent configuration.
            </DialogDescription>
          </DialogHeader>
          
          <div className="space-y-4">
            <div>
              <label htmlFor="namespace-name" className="block text-sm font-medium mb-2">
                Namespace Name
              </label>
              <Input
                id="namespace-name"
                placeholder="e.g., orders, analytics, logs"
                value={newNamespaceName}
                onChange={(e) => setNewNamespaceName(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    handleCreateNamespace();
                  }
                }}
              />
            </div>
            
            <div className="bg-muted/50 rounded-lg p-4">
              <h4 className="font-medium mb-2">Default Configuration</h4>
              <ul className="text-sm text-muted-foreground space-y-1">
                <li>• 4 partitions per stream</li>
                <li>• 1MB max payload size</li>
                <li>• 64KB max header size</li>
                <li>• At-least-once delivery guarantee</li>
              </ul>
            </div>
          </div>
          
          <DialogFooter>
            <Button 
              variant="outline" 
              onClick={() => setShowCreateModal(false)}
              disabled={isCreating}
            >
              Cancel
            </Button>
            <Button 
              onClick={handleCreateNamespace}
              disabled={!newNamespaceName.trim() || isCreating}
            >
              {isCreating ? 'Creating...' : 'Create Namespace'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}