export function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const index = Math.min(
    units.length - 1,
    Math.floor(Math.log(value) / Math.log(1024))
  );
  const scaled = value / Math.pow(1024, index);
  return `${scaled.toFixed(scaled >= 10 ? 0 : 1)} ${units[index]}`;
}

export function formatPercent(ratio: number, fractionDigits = 1): string {
  return `${(ratio * 100).toFixed(fractionDigits)}%`;
}

export function numberish(input: unknown, fallback = 0): number {
  if (typeof input === 'number' && Number.isFinite(input)) return input;
  if (typeof input === 'string') {
    const parsed = Number(input);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallback;
}

export function formatTimestamp(value?: string | Date | null): string {
  if (!value) return '';
  const date = typeof value === 'string' ? new Date(value) : value;
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return '';
  return `${date.toLocaleDateString()} ${date.toLocaleTimeString()}`;
}
