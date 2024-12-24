export function formatNumber(value: number | null | undefined): string {
  return new Intl.NumberFormat('en-GB').format(Number(value ?? 0));
}

export function formatCurrency(value: number | null | undefined): string {
  return new Intl.NumberFormat('en-GB', {
    style: 'currency',
    currency: 'EUR',
    maximumFractionDigits: 2,
  }).format(Number(value ?? 0));
}

export function formatPercent(value: number | null | undefined, digits = 1): string {
  return `${Number(value ?? 0).toFixed(digits)}%`;
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString('en-GB');
}

export function formatSeconds(value: number | null | undefined): string {
  if (value === null || value === undefined) return '—';
  if (value < 1) return `${Math.round(value * 1000)} ms`;
  return `${value.toFixed(2)} s`;
}
