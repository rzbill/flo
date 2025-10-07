import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../ui/card';
import { Button } from '../ui/button';
import { Trash } from 'lucide-react';

interface DeleteConfirmDialogProps {
  open: boolean;
  selectedCount: number;
  onCancel: () => void;
  onConfirm: () => void;
  loading?: boolean;
  title?: string;
  description?: string;
}

export function DeleteConfirmDialog({ open, selectedCount, onCancel, onConfirm, loading, title = 'Delete Messages', description = 'This action cannot be undone.' }: DeleteConfirmDialogProps) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <Card className="w-full max-w-md mx-4">
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-red-600 font-bold text-sm">
            <Trash className="w-4 h-4 font-bold" />
            {title}
          </CardTitle>
          <CardDescription className="text-sm">
            {description}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-800 rounded-lg p-4 text-xs">
            <div className="text-sm text-red-800 dark:text-red-200 text-xs">
              <strong>Warning:</strong> You are about to permanently delete {selectedCount} message{selectedCount === 1 ? '' : 's'}.
            </div>
          </div>
          <div className="flex gap-2 justify-end">
            <Button variant="outline" onClick={onCancel} disabled={!!loading} className="text-xs">Cancel</Button>
            <Button variant="destructive" onClick={onConfirm} disabled={!!loading} className="gap-2 text-xs">
              {loading ? (
                <>
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin text-xs" />
                  Deleting...
                </>
              ) : (
                <>
                  <Trash className="w-4 h-4 text-xs" />
                  Delete
                </>
              )}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

export default DeleteConfirmDialog;


