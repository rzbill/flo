import React from 'react';
import { Card } from '../ui/card';
import TabSelect from './TabSelect';
import TabButton from './TabButton';
import { Play, Pause } from 'lucide-react';
import TabFilter from './TabFilter';

export interface LiveTailTabProps {
  defaultNamespace?: string;
  defaultStream?: string;
  onStart?: (namespace: string, stream: string) => void;
  onToggle?: (running: boolean, namespace: string, stream: string) => void;
  onFilterApply?: (expr: string, tags?: string[]) => void;
}

export function LiveTailTab({ defaultNamespace, defaultStream, onStart, onToggle, onFilterApply }: LiveTailTabProps) {
  const [ns, setNs] = React.useState(() => defaultNamespace ?? 'default');
  const [ch, setCh] = React.useState(() => defaultStream ?? 'events');
  const [running, setRunning] = React.useState(false);

  // Keep local state in sync if caller changes defaults later
  React.useEffect(() => { if (defaultNamespace !== undefined) setNs(defaultNamespace); }, [defaultNamespace]);
  React.useEffect(() => { if (defaultStream !== undefined) setCh(defaultStream); }, [defaultStream]);

  const fetchNamespaces = React.useCallback(async (): Promise<string[]> => {
    try {
      const res = await fetch('/v1/namespaces');
      const d = await res.json().catch(() => ({}));
      return Array.isArray(d?.namespaces) ? (d.namespaces as string[]) : [];
    } catch {
      return [];
    }
  }, []);

  const fetchStreams = React.useCallback(async (): Promise<string[]> => {
    try {
      if (!ns) return [];
      const res = await fetch(`/v1/streams?namespace=${encodeURIComponent(ns)}`);
      const d = await res.json().catch(() => ({}));
      return Array.isArray(d?.streams) ? (d.streams as string[]) : [];
    } catch {
      return [];
    }
  }, [ns]);

  return (
    <Card className="p-1 border-none space-y-4">
      <div className="flex items-center gap-3">
        <div className="inline-flex items-center rounded-md border overflow-hidden w-fit">
          <TabSelect
            value={ns}
            label="Namespace"
            onChange={setNs}
            itemsFetcher={fetchNamespaces}
            buttonClassName="gap-1 h-8 px-3 min-w-[180px] justify-between text-xs border-none shadow-none rounded-none"
          />
          <div aria-hidden className="w-px self-stretch bg-border" />
          <TabSelect
            value={ch}
            label="Stream"
            onChange={setCh}
            itemsFetcher={fetchStreams}
            buttonClassName="gap-1 h-8 px-3 min-w-[180px] justify-between text-xs border-none shadow-none rounded-none"
          />
          <div aria-hidden className="w-px self-stretch bg-border" />
          <TabButton
            label={running ? 'Pause' : 'Start Tailing'}
            icon={running ? Pause : Play}
            onClick={() => {
              const next = !running;
              if (next) { if (onStart) onStart(ns, ch); }
              if (onToggle) onToggle(next, ns, ch);
              setRunning(next);
            }}
            className={running
              ? 'gap-2 h-8 cursor-pointer px-3 rounded-none border-none shadow-none bg-amber-100 text-amber-700 hover:bg-amber-200'
              : 'gap-2 h-8 cursor-pointer px-3 rounded-none border-none shadow-none bg-emerald-100 text-emerald-700 hover:bg-emerald-200'}
          />
        </div>
        <TabFilter onApply={(expr, tags) => { if (onFilterApply) onFilterApply(expr, tags); }} />
      </div>
    </Card>
  );
}

export default LiveTailTab;


