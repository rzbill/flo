import { useMemo } from 'react';

// Formatting utilities as custom hooks for memoization
export function useFormatting() {
  const formatNumber = useMemo(
    () => (n: number) => new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(n),
    []
  );

  const formatBytes = useMemo(
    () => (n: number) => {
      const units = ["B", "KB", "MB", "GB", "TB"] as const;
      let v = n;
      let u = 0;
      while (v >= 1024 && u < units.length - 1) { 
        v /= 1024; 
        u++; 
      }
      return `${v.toFixed(v < 10 && u > 0 ? 1 : 0)} ${units[u]}`;
    },
    []
  );

  const formatRelativeTime = useMemo(
    () => (ms?: number) => {
      if (!ms) return '–';
      const d = Date.now() - ms;
      const s = Math.round(d / 1000);
      const m = Math.round(s / 60);
      const h = Math.round(m / 60);
      const dd = Math.round(h / 24);
      return s < 60 ? `${s}s ago` : m < 60 ? `${m}m ago` : h < 24 ? `${h}h ago` : `${dd}d ago`;
    },
    []
  );

  const formatTime = useMemo(
    () => (timestamp: number) => {
      const now = Date.now();
      const diff = timestamp - now;
      if (diff < 0) return 'Overdue';
      const minutes = Math.floor(diff / 60000);
      return `${minutes}m`;
    },
    []
  );

  const decodeMessageId = useMemo(
    () => (b64: string): string => {
      try {
        const bin = atob(b64);
        if (bin.length !== 8) return b64;
        let seq = 0n;
        for (let i = 0; i < 8; i++) {
          seq = (seq << 8n) | BigInt(bin.charCodeAt(i));
        }
        return seq.toString();
      } catch {
        return b64;
      }
    },
    []
  );

  const decodeHeaderDisplay = useMemo(
    () => (headerB64: string): string | null => {
      if (!headerB64) return null;
      try {
        const bin = atob(headerB64);
        if (bin.length === 8) {
          // Timestamp
          const ts = new Date(Number(bin.charCodeAt(0) << 24) + (bin.charCodeAt(1) << 16) + (bin.charCodeAt(2) << 8) + bin.charCodeAt(3));
          return ts.toLocaleString();
        } else {
          // Try UTF-8 decode
          return new TextDecoder('utf-8', { fatal: false }).decode(new Uint8Array(bin.split('').map(c => c.charCodeAt(0))));
        }
      } catch {
        // Fallback to hex bytes
        try {
          const bin = atob(headerB64);
          return Array.from(bin).map(c => c.charCodeAt(0).toString(16).padStart(2, '0')).join(' ');
        } catch {
          return headerB64;
        }
      }
    },
    []
  );

  return {
    formatNumber,
    formatBytes,
    formatRelativeTime,
    formatTime,
    decodeMessageId,
    decodeHeaderDisplay,
  };
}
