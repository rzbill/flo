import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../ui/table';
import { useStreamGroups } from '../../../hooks/useStreams';

interface Props {
  namespace: string;
  stream: string;
}

export default function StreamsSubscriptions({ namespace, stream }: Props) {
  const { data, isLoading, error } = useStreamGroups(namespace, stream);

  return (
    <div className="h-full p-6">
      <Card className="h-full">
        <CardHeader>
          <CardTitle className="text-sm">Subscriptions & Lag</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading && <div className="text-sm text-muted-foreground">Loading…</div>}
          {error && <div className="text-sm text-red-600">Failed to load subscriptions</div>}
          {!isLoading && !error && (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Group</TableHead>
                  <TableHead className="text-right">Active Subscribers</TableHead>
                  <TableHead className="text-right">Total Lag</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(data?.groups || []).map((g) => (
                  <React.Fragment key={g.name}>
                    <TableRow>
                      <TableCell className="font-medium">{g.name}</TableCell>
                      <TableCell className="text-right">{g.active_subscribers ?? 0}</TableCell>
                      <TableCell className="text-right">{g.total_lag ?? 0}</TableCell>
                    </TableRow>
                    {(g.partitions || []).map((p) => (
                      <TableRow key={`${g.name}-${p.partition}`} className="bg-muted/30">
                        <TableCell className="pl-6 text-xs text-muted-foreground">Partition {p.partition}</TableCell>
                        <TableCell className="text-right text-xs text-muted-foreground">Cursor {p.cursor_seq} / End {p.end_seq}</TableCell>
                        <TableCell className="text-right text-xs text-muted-foreground">Lag {p.lag}</TableCell>
                      </TableRow>
                    ))}
                  </React.Fragment>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}


