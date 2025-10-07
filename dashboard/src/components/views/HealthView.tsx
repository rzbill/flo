import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../ui/card';
import { Badge } from '../ui/badge';
import { Button } from '../ui/button';
import { RefreshCw, ExternalLink, CheckCircle, AlertCircle } from 'lucide-react';

interface HealthStatus {
  status: 'ok' | 'not_serving';
  lastChecked: Date;
}

interface ConfigSummary {
  defaultPartitions: number;
  maxPayloadSize: string;
  maxHeaderSize: string;
}

interface HealthViewProps {
  onNavigate?: (view: import('../../types/navigation').View, namespace?: string, stream?: string, tab?: string) => void;
}

export function HealthView({ onNavigate }: HealthViewProps) {
  const [health, setHealth] = useState<HealthStatus>({
    status: 'ok',
    lastChecked: new Date()
  });
  const [isRefreshing, setIsRefreshing] = useState(false);
  
  // Static config hints (server does not expose these yet)
  const config: ConfigSummary = {
    defaultPartitions: 4,
    maxPayloadSize: '1MB',
    maxHeaderSize: '64KB'
  };

  const refreshHealth = async () => {
    try {
      setIsRefreshing(true);
      const res = await fetch('/v1/healthz');
      let status: 'ok' | 'not_serving' = 'ok';
      if (!res.ok) {
        status = 'not_serving';
      } else {
        const d = await res.json().catch(() => ({ status: 'not_serving' }));
        status = (d?.status === 'ok') ? 'ok' : 'not_serving';
      }
      setHealth({ status, lastChecked: new Date() });
    } catch {
      setHealth({ status: 'not_serving', lastChecked: new Date() });
    } finally {
      setIsRefreshing(false);
    }
  };

  useEffect(() => {
    refreshHealth();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const isHealthy = health.status === 'ok';

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1>System Health</h1>
          <p className="text-muted-foreground">Runtime status and configuration</p>
        </div>
        <Button 
          onClick={refreshHealth} 
          disabled={isRefreshing}
          variant="outline"
          className="gap-2"
        >
          <RefreshCw className={`w-4 h-4 ${isRefreshing ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Health Status */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              {isHealthy ? (
                <CheckCircle className="w-5 h-5 text-green-600" />
              ) : (
                <AlertCircle className="w-5 h-5 text-red-600" />
              )}
              Runtime Status
            </CardTitle>
            <CardDescription>
              Last checked {health.lastChecked.toLocaleTimeString()}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center gap-3">
              <Badge 
                variant={isHealthy ? "default" : "destructive"}
                className={isHealthy ? "bg-green-100 text-green-800 hover:bg-green-100" : ""}
              >
                {isHealthy ? 'OK' : 'Not Serving'}
              </Badge>
              <span className="text-sm text-muted-foreground">
                {isHealthy ? 'All systems operational' : 'Service unavailable'}
              </span>
            </div>

            {!isHealthy && (
              <div className="bg-destructive/10 border border-destructive/20 rounded-md p-4">
                <h4 className="font-medium text-destructive mb-2">Troubleshooting</h4>
                <ul className="text-sm space-y-1 text-muted-foreground">
                  <li>• Check if the FLO server is running</li>
                  <li>• Verify the data directory is accessible</li>
                  <li>• Ensure port 8080 is not blocked</li>
                  <li>• Review server logs for errors</li>
                </ul>
                <Button variant="outline" size="sm" className="mt-3 gap-2">
                  <ExternalLink className="w-4 h-4" />
                  View Documentation
                </Button>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Configuration Summary */}
        <Card>
          <CardHeader>
            <CardTitle>Configuration</CardTitle>
            <CardDescription>
              Current namespace defaults and limits
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <div className="text-sm text-muted-foreground">Default Partitions</div>
                <div className="font-medium">{config.defaultPartitions}</div>
              </div>
              <div>
                <div className="text-sm text-muted-foreground">Max Payload</div>
                <div className="font-medium">{config.maxPayloadSize}</div>
              </div>
              <div>
                <div className="text-sm text-muted-foreground">Max Headers</div>
                <div className="font-medium">{config.maxHeaderSize}</div>
              </div>
              <div>
                <div className="text-sm text-muted-foreground">Runtime Mode</div>
                <div className="font-medium">Single-node</div>
              </div>
            </div>
            
            <div className="pt-2 border-t">
              <div className="text-sm text-muted-foreground mb-2">Storage</div>
              <div className="font-medium">Embedded LSM Engine</div>
              <div className="text-sm text-muted-foreground">
                Event logs and work queues persisted locally
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Quick Stats */}
      <Card>
        <CardHeader>
          <CardTitle>Quick Overview</CardTitle>
          <CardDescription>
            Key metrics at a glance
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            <button 
              className="text-center p-4 rounded-lg hover:bg-accent transition-colors group"
              onClick={() => onNavigate?.('namespaces')}
            >
              <div className="text-2xl font-semibold group-hover:text-primary">3</div>
              <div className="text-sm text-muted-foreground group-hover:text-accent-foreground">Active Namespaces</div>
            </button>
            <div className="text-center">
              <div className="text-2xl font-semibold">12</div>
              <div className="text-sm text-muted-foreground">Total Streams</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-semibold">1.2K</div>
              <div className="text-sm text-muted-foreground">Events Today</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-semibold">99.9%</div>
              <div className="text-sm text-muted-foreground">Uptime</div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}