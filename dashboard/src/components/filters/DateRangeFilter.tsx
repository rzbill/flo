import React from 'react';
import * as SliderPrimitive from "@radix-ui/react-slider";

type DateRange = {
  start: Date;
  end: Date;
};

interface DateRangeFilterProps {
  range: DateRange;
  value: DateRange;
  onChange: (next: DateRange) => void;
  className?: string;
  stepSeconds?: number;
  startLabelClassName?: string;
  endLabelClassName?: string;
  trackClassName?: string;
  rangeClassName?: string;
  thumbClassName?: string;
}

function toSeconds(date: Date): number {
  return Math.floor(date.getTime() / 1000);
}

function fromSeconds(seconds: number): Date {
  return new Date(seconds * 1000);
}

function formatTick(d: Date): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  }).format(d);
}

export function DateRangeFilter({ 
  range, 
  value, 
  onChange, 
  className, 
  stepSeconds = 60,
  trackClassName,
  rangeClassName,
  thumbClassName,
} : DateRangeFilterProps) {
  const min = React.useMemo(() => toSeconds(range.start), [range.start]);
  const max = React.useMemo(() => toSeconds(range.end), [range.end]);
  const current = React.useMemo(() => [toSeconds(value.start), toSeconds(value.end)], [value.start, value.end]);
  const startPct = React.useMemo(() => ((toSeconds(value.start) - min) / Math.max(1, max - min)) * 100, [value.start, min, max]);
  const endPct = React.useMemo(() => ((toSeconds(value.end) - min) / Math.max(1, max - min)) * 100, [value.end, min, max]);
  const startNearLeft = startPct < 15;
  const endNearRight = endPct > 85;

  // Dynamic left/right label placement based on available space
  const containerRef = React.useRef(null);
  const startLabelRef = React.useRef(null);
  const endLabelRef = React.useRef(null);
  const [startPlaceRight, setStartPlaceRight] = React.useState(startNearLeft);
  const [endPlaceLeft, setEndPlaceLeft] = React.useState(endNearRight);

  const recomputeLabelPlacement = React.useCallback(() => {
    const c = containerRef.current;
    if (!c) return;
    const width = c.clientWidth;
    const startX = (startPct / 100) * width;
    const endX = (endPct / 100) * width;
    const startW = startLabelRef.current?.offsetWidth ?? 0;
    const endW = endLabelRef.current?.offsetWidth ?? 0;
    setStartPlaceRight(startX - startW - 8 < 0);
    setEndPlaceLeft(endX + endW + 8 > width);
  }, [startPct, endPct]);

  React.useLayoutEffect(() => {
    recomputeLabelPlacement();
  }, [recomputeLabelPlacement, value.start, value.end]);

  React.useEffect(() => {
    const c = containerRef.current;
    if (!c) return;
    const ro = new ResizeObserver(() => recomputeLabelPlacement());
    ro.observe(c);
    return () => ro.disconnect();
  }, [recomputeLabelPlacement]);

  const handleChange = (vals: number[]) => {
    if (!Array.isArray(vals) || vals.length < 2) return;
    const [left, right] = vals;
    onChange({ start: fromSeconds(left), end: fromSeconds(right) });
  };

  return (
    <div className={className}>
      <div className="relative">
        <div className="py-6 relative overflow-hidden">
          <SliderPrimitive.Root
            className="relative flex w-full items-center select-none"
            min={min}
            max={max}
            step={stepSeconds}
            value={current}
            onValueChange={handleChange as any}
          >
            <SliderPrimitive.Track
              className={`relative grow bg-muted h-2 w-full ${trackClassName ?? ''}`}
              style={{ height: '4px' }}
            >
              <SliderPrimitive.Range
                className={`absolute border-4 h-full bg-primary ${rangeClassName ?? ''}`}
                style={{ height: '4px' }}
              />
            </SliderPrimitive.Track>
            {Array.from({ length: 2 }).map((_, index) => (
              <SliderPrimitive.Thumb
                key={index}
                className={`block bg-primary ${thumbClassName ?? ''}`}
                style={{ 
                    height: '10px', 
                    width: '4px',  
                    transform: `translateY(${index === 0 ? -3 : 3}px) ` }}
              />
            ))}
          </SliderPrimitive.Root>

          {/* Labels positioned near thumbs: default outside the selected range.
              Auto-pan when near container edges to avoid clipping. */}
          <div ref={containerRef} className="pointer-events-none absolute inset-0 overflow-hidden">
            <div
              ref={startLabelRef}
              className="absolute max-w-[45%] truncate text-xs text-muted-foreground"
              style={{ left: `${startPct}%`, top: '50%', transform: startPlaceRight ? 'translate(8px, -140%)' : 'translate(calc(-100% - 8px), -140%)' }}
              title={formatTick(value.start)}
            >
              {formatTick(value.start)}
            </div>
            <div
              ref={endLabelRef}
              className="absolute max-w-[45%] truncate text-xs text-muted-foreground"
              style={{ left: `${endPct}%`, top: '50%', transform: endPlaceLeft ? 'translate(calc(-100% - 8px), 40%)' : 'translate(8px, 40%)' }}
              title={formatTick(value.end)}
            >
              {formatTick(value.end)}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default DateRangeFilter;


