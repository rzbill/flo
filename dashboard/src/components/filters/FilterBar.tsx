import React from 'react';
import { Card } from '../ui/card';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { Search } from 'lucide-react';
import { DateRangeFilter } from './DateRangeFilter';

interface FilterBarProps {
  initialStart?: Date;
  initialEnd?: Date;
  onApply?: (range: { start: Date; end: Date }) => void;
  onReset?: () => void;
  className?: string;
  searchPlaceholder?: string;
}

export function FilterBar({ initialStart, initialEnd, onApply, onReset, className, searchPlaceholder }: FilterBarProps) {
  const [range] = React.useState(() => ({
    start: initialStart ?? new Date(Date.now() - 24 * 60 * 60 * 1000),
    end: initialEnd ?? new Date(),
  }));
  const [value, setValue] = React.useState(range);
  const [query, setQuery] = React.useState('');

  // Auto-apply whenever the range changes
  React.useEffect(() => { onApply?.(value); }, [value, onApply]);
  const handleReset = () => {
    setValue(range);
    onReset?.();
  };

  return (
    <Card className={className ? `${className} p-3` : 'p-3'}>
      <div className="flex items-center gap-3">
        {/* Left: Date range ~20% width */}
        <div className="basis-1/5 min-w-[220px] max-w-[360px]">
          <DateRangeFilter
            className="w-full"
            range={range}
            value={value}
            onChange={setValue}
            stepSeconds={60}
            trackClassName="bg-zinc-400/50 dark:bg-zinc-500/50"
            rangeClassName="bg-indigo-500"
            thumbClassName="bg-indigo-400"
          />
        </div>

        {/* Right: Growable search aligned to right, inputless style */}
        <div className="flex-1 flex justify-end">
          <div className="relative w-full max-w-[560px]">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 size-4 text-muted-foreground" />
            <Input
              placeholder={searchPlaceholder ?? 'Search'}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="pl-8 border-none shadow-none bg-transparent focus-visible:ring-0"
            />
          </div>
        </div>

        {/* Reset hidden for now */}
      </div>
    </Card>
  );
}

export default FilterBar;


