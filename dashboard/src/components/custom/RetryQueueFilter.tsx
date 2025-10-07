import React from 'react';
import { CirclePlus } from 'lucide-react';
import { Popover, PopoverContent, PopoverTrigger } from '../ui/popover';
import { Button } from '../ui/button';
import { Label } from '../ui/label';
import { Input } from '../ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';

export interface RetryQueueFilterProps {
  value?: string; // filter expression (simplified)
  tags?: string[];
  groups?: string[]; // available groups for selection
  onApply?: (expr: string, tags: string[]) => void;
  className?: string;
}

export default function RetryQueueFilter({ value, tags, groups = [], onApply, className }: RetryQueueFilterProps) {
  const [open, setOpen] = React.useState(false);
  const [groupFilter, setGroupFilter] = React.useState('all');
  const [limit, setLimit] = React.useState('100');
  const [reverse, setReverse] = React.useState(false);
  const [summaryTags, setSummaryTags] = React.useState<string[]>(Array.isArray(tags) ? tags : []);

  const hasTags = summaryTags.length > 0;

  const apply = () => {
    const tagList: string[] = [];
    
    // Build filter expression based on server-supported parameters
    const filters: string[] = [];
    
    // Group filter
    if (groupFilter.trim() && groupFilter !== 'all') {
      filters.push(`group:${groupFilter.trim()}`);
      tagList.push(`group: ${groupFilter.trim()}`);
    }
    
    // Limit
    if (limit.trim() && limit !== '100') {
      filters.push(`limit:${limit.trim()}`);
      tagList.push(`limit: ${limit.trim()}`);
    }
    
    // Reverse order
    if (reverse) {
      filters.push('reverse:true');
      tagList.push('newest first');
    }
    
    const expr = filters.join(',');
    setSummaryTags(tagList);
    onApply?.(expr, tagList);
    setOpen(false);
  };

  const resetAll = () => {
    setGroupFilter('all');
    setLimit('100');
    setReverse(false);
    setSummaryTags([]);
    onApply?.('', []);
    setOpen(false);
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant={hasTags ? 'default' : 'outline'}
          className={hasTags
            ? `inline-flex items-center gap-2 h-8 pl-3 pr-3 rounded-full border border-emerald-600/30 bg-emerald-400/30 text-emerald-800 text-xs hover:bg-emerald-400/30 hover:border-emerald-700 ${className ?? ''}`
            : `inline-flex items-center gap-2 h-8 px-3 rounded-full border border-dashed text-xs text-foreground/80 ${className ?? ''}`}
        >
          <CirclePlus className="w-4 h-4" />
          {hasTags ? (
            <>
              <span className="text-xs font-medium">Filtered by</span>
              <span className="flex items-center gap-1">
                {summaryTags.slice(0, 3).map((t) => (
                  <span key={t} className="text-xs rounded-full border border-emerald-600/40 bg-emerald-400/40 px-3 py-[3px] hover:border-emerald-800">
                    {t}
                  </span>
                ))}
                {summaryTags.length > 3 && (
                  <span className="text-xs rounded-full border border-emerald-600/40 bg-emerald-400/40 px-2 py-[3px] hover:border-emerald-800">+{summaryTags.length - 3}</span>
                )}
              </span>
            </>
          ) : (
            <span className="text-xs">Filter retry messages</span>
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" sideOffset={8} className="w-[320px] p-3 text-xs">
        <div className="text-xs font-medium pb-2 mb-3 border-b -mx-3 px-3">Filter retry messages</div>

        <div className="space-y-4">
          {/* Group filter */}
          <div className="grid grid-cols-4 items-center gap-2">
            <Label className="col-span-1 text-right text-muted-foreground text-xs">Group</Label>
            <div className="col-span-3">
              <Select value={groupFilter} onValueChange={setGroupFilter}>
                <SelectTrigger className="h-7 text-xs shadow-none">
                  <SelectValue placeholder="Select group (optional)" />
                </SelectTrigger>
                <SelectContent className="text-xs">
                  <SelectItem className="text-xs py-1" value="all">All Groups</SelectItem>
                  {groups.map(group => (
                    <SelectItem key={group} className="text-xs py-1" value={group}>
                      {group}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          
          {/* Limit */}
          <div className="grid grid-cols-4 items-center gap-2">
            <Label className="col-span-1 text-right text-muted-foreground text-xs">Limit</Label>
            <Input 
              className="col-span-3 h-7 text-xs md:text-xs placeholder:text-xs shadow-none focus-visible:ring-0" 
              value={limit} 
              onChange={(e) => setLimit(e.target.value)} 
              placeholder="number of messages to return" 
              type="number"
              min="1"
              max="1000"
            />
          </div>
          
          {/* Sort order */}
          <div className="grid grid-cols-4 items-center gap-2">
            <Label className="col-span-1 text-right text-muted-foreground text-xs">Sort</Label>
            <div className="col-span-3">
              <div className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  id="reverse"
                  checked={reverse}
                  onChange={(e) => setReverse(e.target.checked)}
                  className="h-4 w-4"
                />
                <Label htmlFor="reverse" className="text-xs text-muted-foreground">
                  Show newest first
                </Label>
              </div>
            </div>
          </div>
        </div>

        <div className="mt-3 -mx-3 px-3 pt-3 border-t flex justify-between gap-2">
          <Button variant="outline" size="sm" className="h-7 text-xs cursor-pointer" onClick={resetAll}>Reset</Button>
          <div className="flex gap-2">
            <Button size="sm" className="h-7 text-xs cursor-pointer" onClick={apply}>Apply</Button>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}


