export function getApiBase(): string {
  // Allow overriding via Vite env
  const fromEnv = (import.meta as any)?.env?.VITE_API_BASE as string | undefined;
  return fromEnv || '';
}

export function apiUrl(path: string): string {
  const base = getApiBase();
  if (!base) return path;
  return `${base}${path}`;
}

export async function fetchJSON<T>(input: string, init?: RequestInit): Promise<T> {
  const response = await fetch(apiUrl(input), init);
  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`HTTP ${response.status}: ${text || response.statusText}`);
  }
  return response.json() as Promise<T>;
}


