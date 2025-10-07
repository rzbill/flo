import React from 'react';
import { CirclePlus } from 'lucide-react';
import { Popover, PopoverContent, PopoverTrigger } from '../ui/popover';
import { Button } from '../ui/button';
import { Label } from '../ui/label';
import { Input } from '../ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { Calendar } from '../ui/calendar';
import { Calendar as CalendarIcon } from 'lucide-react';

export interface TabFilterProps {
  value?: string; // CEL expression
  tags?: string[];
  onApply?: (expr: string, tags: string[]) => void;
  className?: string;
}

export default function TabFilter({ value, tags, onApply, className }: TabFilterProps) {
  const [open, setOpen] = React.useState(false);
  const [containsText, setContainsText] = React.useState('');
  const [regexText, setRegexText] = React.useState('');
  const [jsonPath, setJsonPath] = React.useState('');
  const [jsonOp, setJsonOp] = React.useState('==');
  const [jsonVal, setJsonVal] = React.useState('');
  const [parts, setParts] = React.useState(''); // e.g. 0,1,2
  const [windowPreset, setWindowPreset] = React.useState('none'); // none,1m,5m,1h,1d
  const [showRange, setShowRange] = React.useState(false);
  const [range, setRange] = React.useState<{ from?: Date; to?: Date }>({});
  const [customExpr, setCustomExpr] = React.useState(value ?? '');
  const [summaryTags, setSummaryTags] = React.useState<string[]>(Array.isArray(tags) ? tags : []);

  const hasTags = summaryTags.length > 0;

  const escapeStr = (s: string) => s.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  const buildJsonPath = (p: string) => p.trim().replace(/\[(\d+)\]/g, '.$1').split('.').filter(Boolean).map(seg => /^(\d+)$/.test(seg) ? `[${seg}]` : `.${seg}`).join('').replace(/^\./, 'json.');
  const isNumber = (v: string) => /^-?\d+(\.\d+)?$/.test(v.trim());
  const formatValue = (v: string) => (isNumber(v) || v === 'true' || v === 'false' || v === 'null') ? v.trim() : `'${escapeStr(v)}'`;
  const toMs = (preset: string): number => {
    if (!preset || preset === 'none') return 0;
    const m = preset.match(/^(\d+)(ms|s|m|h)$/);
    if (!m) return 0;
    const n = parseInt(m[1], 10);
    const unit = m[2];
    return unit === 'ms' ? n : unit === 's' ? n * 1000 : unit === 'm' ? n * 60000 : n * 3600000;
  };

  const apply = () => {
    const clauses: string[] = [];
    const tagList: string[] = [];
    if (containsText.trim()) {
      clauses.push(`text.contains('${escapeStr(containsText.trim())}')`);
      tagList.push(`contains: ${containsText.trim()}`);
    }
    if (regexText.trim()) {
      clauses.push(`text.matches('${escapeStr(regexText.trim())}')`);
      tagList.push('regex');
    }
    if (jsonPath.trim() && jsonVal.trim()) {
      const path = buildJsonPath(jsonPath);
      clauses.push(`json != null && ${path} ${jsonOp} ${formatValue(jsonVal)}`);
      tagList.push(`json: ${jsonPath} ${jsonOp} ${jsonVal}`);
    }
    if (parts.trim()) {
      const list = parts.split(',').map(s => s.trim()).filter(s => s !== '' && /^\d+$/.test(s)).join(',');
      if (list) {
        clauses.push(`partition in [${list}]`);
        tagList.push(`partitions: [${list}]`);
      }
    }
    const ms = toMs(windowPreset);
    if (ms > 0) {
      clauses.push(`ts_ms > now_ms - ${ms}`);
      tagList.push(`window: ${windowPreset}`);
    }
    if (range?.from && range?.to) {
      const startMs = range.from.getTime();
      const endMs = range.to.getTime();
      clauses.push(`ts_ms >= ${startMs} && ts_ms <= ${endMs}`);
      tagList.push(`${range.from.toLocaleDateString()} – ${range.to.toLocaleDateString()}`);
    }
    if (customExpr.trim()) {
      clauses.push(`(${customExpr.trim()})`);
      tagList.push('custom');
    }
    const expr = clauses.join(' && ');
    setSummaryTags(tagList);
    onApply?.(expr, tagList);
    setOpen(false);
  };

  const resetAll = () => {
    setContainsText('');
    setRegexText('');
    setJsonPath('');
    setJsonOp('==');
    setJsonVal('');
    setParts('');
    setWindowPreset('none');
    setCustomExpr('');
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
            <span className="text-xs">Filter messages</span>
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" sideOffset={8} className="w-[520px] p-3 text-xs">
        <div className="text-xs font-medium pb-2 mb-3 border-b -mx-3 px-3">Filter messages</div>

        <div className="space-y-4">
          {/* Message contains */}
          <div className="grid grid-cols-4 items-center gap-2">
            <Label className="col-span-1 text-right text-muted-foreground text-xs">Contains</Label>
            <Input className="col-span-3 h-7 text-xs md:text-xs placeholder:text-xs shadow-none focus-visible:ring-0" value={containsText} onChange={(e) => setContainsText(e.target.value)} placeholder="error, order.created" />
          </div>
          {/* Regex */}
          <div className="grid grid-cols-4 items-center gap-2">
            <Label className="col-span-1 text-right text-muted-foreground text-xs">Regex</Label>
            <Input className="col-span-3 h-7 text-xs md:text-xs placeholder:text-xs shadow-none focus-visible:ring-0" value={regexText} onChange={(e) => setRegexText(e.target.value)} placeholder="^ERR\\d+$" />
          </div>
          {/* JSON field */}
          <div className="grid grid-cols-4 items-center gap-2">
            <Label className="col-span-1 text-right text-muted-foreground text-xs">JSON</Label>
            <Input className="col-span-2 h-7 text-xs md:text-xs placeholder:text-xs shadow-none focus-visible:ring-0" value={jsonPath} onChange={(e) => setJsonPath(e.target.value)} placeholder="a.b[0]" />
            <div className="col-span-1">
              <Select value={jsonOp} onValueChange={setJsonOp}>
                <SelectTrigger className="w-full h-7 text-xs shadow-none"><SelectValue /></SelectTrigger>
                <SelectContent className="text-xs">
                  <SelectItem className="text-xs py-1" value="==">==</SelectItem>
                  <SelectItem className="text-xs py-1" value="!=">!=</SelectItem>
                  <SelectItem className="text-xs py-1" value=">">&gt;</SelectItem>
                  <SelectItem className="text-xs py-1" value=">=">&gt;=</SelectItem>
                  <SelectItem className="text-xs py-1" value="<">&lt;</SelectItem>
                  <SelectItem className="text-xs py-1" value="<=">&lt;=</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="col-span-3 col-start-2">
              <Input className="h-7 text-xs md:text-xs placeholder:text-xs shadow-none focus-visible:ring-0" value={jsonVal} onChange={(e) => setJsonVal(e.target.value)} placeholder="value" />
            </div>
          </div>
          {/* Partitions */}
          <div className="grid grid-cols-4 items-center gap-2">
            <Label className="col-span-1 text-right text-muted-foreground text-xs">Partitions</Label>
            <Input className="col-span-3 h-7 text-xs md:text-xs placeholder:text-xs shadow-none focus-visible:ring-0" value={parts} onChange={(e) => setParts(e.target.value)} placeholder="e.g. 0,1,2" />
          </div>
          {/* Time window */}
          <div className="grid grid-cols-4 items-center gap-2">
            <Label className="col-span-1 text-right text-muted-foreground text-xs">Window</Label>
            <div className="col-span-3">
              <div className="flex items-center gap-2">
                <div className="flex-1">
                  <Select value={windowPreset} onValueChange={setWindowPreset}>
                    <SelectTrigger className="w-full h-7 text-xs shadow-none"><SelectValue placeholder="Window" /></SelectTrigger>
                    <SelectContent className="text-xs">
                      <SelectItem className="text-xs py-1" value="none">None</SelectItem>
                      <SelectItem className="text-xs py-1" value="1m">Last 1 minute</SelectItem>
                      <SelectItem className="text-xs py-1" value="5m">Last 5 minutes</SelectItem>
                      <SelectItem className="text-xs py-1" value="1h">Last 1 hour</SelectItem>
                      <SelectItem className="text-xs py-1" value="1d">Last 1 day</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="h-7 w-8 p-0"
                  onClick={() => setShowRange(v => !v)}
                  title="Pick date range"
                >
                  <CalendarIcon className="w-4 h-4" />
                </Button>
              </div>
            </div>
          </div>
          {showRange && (
            <div className="grid grid-cols-4 items-start gap-2">
              <Label className="col-span-1 text-right text-muted-foreground text-xs">Range</Label>
              <div className="col-span-3">
                <Calendar
                  mode="range"
                  selected={range as any}
                  onSelect={(r: any) => setRange(r ?? {})}
                  numberOfMonths={2}
                  className="border text-xs rounded-md"
                  classNames={{
                    head_cell: "p-1 text-xs font-light",
                    day: "size-6 p-0 font-normal",
                    cell: "p-0",
                    caption_label: "text-xs",
                  }}
                />
              </div>
            </div>
          )}
          {/* Custom expression */}
          <div className="grid grid-cols-4 items-center gap-2">
            <Label className="col-span-1 text-right text-muted-foreground text-xs">Custom</Label>
            <Input className="col-span-3 h-7 text-xs md:text-xs placeholder:text-xs shadow-none focus-visible:ring-0" value={customExpr} onChange={(e) => setCustomExpr(e.target.value)} placeholder="CEL expression (optional)" />
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


