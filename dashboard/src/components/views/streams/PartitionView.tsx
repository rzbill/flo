import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../ui/table';

export interface PartitionStat {
  partition: number;
  first_seq: number;
  last_seq: number;
  count: number;
  bytes: number;
  last_publish_ms?: number;
}

interface StreamDetailsPerPartitionViewProps {
  partitions: PartitionStat[];
  formatNumber: (n: number) => string;
  formatBytes: (n: number) => string;
  relTime: (ms?: number) => string;
}

export function StreamDetailsPerPartitionView({ partitions, formatNumber, formatBytes, relTime }: StreamDetailsPerPartitionViewProps) {
  return (
    <div className="grid grid-cols-12 gap-4 px-4">
      <div className="col-span-12 lg:col-span-8 lg:col-start-3 space-y-4">
        <Card className="border-none p-0 mt-10">
          <CardHeader className="pb-2 px-0">
            <CardTitle className="text-2xl md:text-3xl font-light">Per-Partition Stats</CardTitle>
            <CardDescription className="text-xs">First/last seq, counts, bytes, last publish</CardDescription>
          </CardHeader>
        </Card>


        {/* Explainer Banner */}
        <Card className="border border-gray-200 bg-accent-foreground/3">
          <CardContent className="p-4">
            <div className="space-y-2">
              <h3 className="font-semibold text-sm">Viewing partition statistics for this stream</h3>
              <p className="text-xs text-gray-600">
                Each partition maintains its own sequence numbers and message counts.
                The <span className="bg-gray-100 px-1.5 py-0.5 rounded text-xs font-mono">partition</span> column shows
                the partition ID, while other columns display first/last sequence numbers, message counts, and storage usage.
              </p>
            </div>
          </CardContent>
        </Card>

        <Card className="border-none">
            <CardContent className="p-0">
              <div className="overflow-x-auto rounded-lg border border-gray-200">
                <Table className="table-fixed w-full [&_*]:align-top [&_td]:border-l [&_th]:border-l [&_td:first-child]:border-l-0 [&_th:first-child]:border-l-0 [&_tr]:border-b">
                <TableHeader>
                  <TableRow className="bg-muted/40">
                    <TableHead className="w-[80px] p-2 font-light text-xs">PARTITION</TableHead>
                    <TableHead className="w-[80px] p-2 font-light text-xs">FIRST SEQ</TableHead>
                    <TableHead className="w-[80px] p-2 font-light text-xs">LAST SEQ</TableHead>
                    <TableHead className="w-[80px] p-2 font-light text-xs">COUNT</TableHead>
                    <TableHead className="w-[80px] p-2 font-light text-xs">BYTES</TableHead>
                    <TableHead className="w-[80px] p-2 font-light text-xs">LAST PUBLISH</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {partitions.map((p) => (
                    <TableRow key={p.partition} className="text-xs">
                      <TableCell className="p-2 font-mono">{p.partition}</TableCell>
                      <TableCell className="p-2 font-mono">{formatNumber(p.first_seq)}</TableCell>
                      <TableCell className="p-2 font-mono">{formatNumber(p.last_seq)}</TableCell>
                      <TableCell className="p-2 font-mono">{formatNumber(p.count)}</TableCell>
                      <TableCell className="p-2 font-mono">{formatBytes(p.bytes)}</TableCell>
                      <TableCell className="p-2">{relTime(p.last_publish_ms)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

export default StreamDetailsPerPartitionView;

