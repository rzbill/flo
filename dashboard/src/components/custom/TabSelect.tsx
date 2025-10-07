import React from 'react';
import { Button } from '../ui/button';
import { Popover, PopoverContent, PopoverTrigger } from '../ui/popover';
import { 
  Command,
  CommandInput,
  CommandList,
  CommandEmpty,
  CommandGroup,
  CommandItem
} from '../ui/command';
import { ChevronsUpDown, Check, Plus } from 'lucide-react';

export interface TabSelectProps {
  label: string;
  items?: string[];
  itemsFetcher?: () => Promise<string[]>;
  value: string;
  onChange: (value: string) => void;
  onCreate?: () => void;
  buttonClassName?: string;
}

export function TabSelect({
  label,
  items,
  itemsFetcher,
  value,
  onChange,
  onCreate,
  buttonClassName
}: TabSelectProps) {
  const [open, setOpen] = React.useState(false);
  const [data, setData] = React.useState<string[]>(items ?? []);
  const [loading, setLoading] = React.useState(false);
  // Sync provided static items only when the prop changes
  React.useEffect(() => { if (Array.isArray(items)) setData(items); }, [items]);

  // Keep a stable reference to the fetcher to avoid effect loops when parent recreates it
  const fetcherRef = React.useRef<typeof itemsFetcher>();
  React.useEffect(() => { fetcherRef.current = itemsFetcher; }, [itemsFetcher]);
  React.useEffect(() => {
    if (!open || !fetcherRef.current) return;
    let cancelled = false;
    setLoading(true);
    // Clear any stale items while fetching fresh data
    setData([]);
    fetcherRef.current()
      .then((res) => { if (!cancelled) setData(res || []); })
      .catch(() => { if (!cancelled) setData([]); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [open]);
  const pluralize = (s: string) => {
    const lower = s.toLowerCase();
    if (/(s|x|z|ch|sh)$/i.test(lower)) return s.charAt(0).toUpperCase() + s.slice(1) + 'es';
    if (/[a-z]y$/i.test(lower) && !/[aeiou]y$/i.test(lower)) return s.charAt(0).toUpperCase() + s.slice(1, -1) + 'ies';
    return s.charAt(0).toUpperCase() + s.slice(1) + 's';
  };
  const heading = pluralize(label);
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          className={buttonClassName ?? 'gap-1 h-7 px-2 min-w-[180px] justify-between text-xs border-none shadow-none'}
          aria-expanded={open}
        >
          <span className="truncate max-w-[160px] text-left inline-flex items-center gap-1">
            <span className="text-muted-foreground">{label}</span>
            <span className="text-foreground font-medium">{value}</span>
          </span>
          <ChevronsUpDown className="w-3 h-3 opacity-60 text-xs" />
        </Button>
      </PopoverTrigger>
      <PopoverContent side="bottom" align="start" sideOffset={6} className="p-0 w-[320px]">
        <Command>
          <CommandInput placeholder={`Find ${label.toLowerCase()}...`} className="text-xs" />
          <CommandList>
            <CommandEmpty>{loading ? 'Loading…' : `No ${label.toLowerCase()} found.`}</CommandEmpty>
            <CommandGroup heading={heading}>
              {data.map((p) => (
                <CommandItem key={p} value={p} className="text-xs" onSelect={() => { onChange(p); setOpen(false); }}>
                  <Check className={`w-4 h-4 mr-2 shrink-0 transition-opacity ${value === p ? 'opacity-100 text-primary' : 'opacity-0'}`} />
                  {p}
                </CommandItem>
              ))}
            </CommandGroup>
            {onCreate && (
              <CommandGroup>
                <CommandItem value="__new__" className="text-xs" onSelect={() => { setOpen(false); onCreate(); }}>
                  <Plus className="w-4 h-4 mr-2" /> New project
                </CommandItem>
              </CommandGroup>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

export default TabSelect;


