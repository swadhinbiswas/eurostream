import { Badge } from '@databricks/appkit-ui/react';
import { cn } from '@/lib/utils';

interface StatusBadgeProps {
  value: string;
}

export function StatusBadge({ value }: StatusBadgeProps) {
  const normalized = value.toUpperCase();
  const positive = ['COMPLETED', 'PASSED', 'TERMINATED', 'SUCCEEDED', 'VERIFIED', 'FRAUD_SINK_PURGED'].includes(
    normalized
  );
  const negative = ['FAILED', 'INTERNAL_ERROR', 'SKIPPED'].includes(normalized);
  const warning = ['RUNNING', 'PENDING', 'QUEUED', 'SUBMITTED', 'VERIFYING', 'DEFERRED'].includes(normalized);

  return (
    <Badge
      variant="outline"
      className={cn(
        'font-mono text-[10px] tracking-wide',
        positive && 'border-emerald-200 bg-emerald-50 text-emerald-700',
        negative && 'border-rose-200 bg-rose-50 text-rose-700',
        warning && 'border-amber-200 bg-amber-50 text-amber-700',
        !positive && !negative && !warning && 'border-slate-200 bg-slate-50 text-slate-600'
      )}
    >
      {normalized.replaceAll('_', ' ')}
    </Badge>
  );
}
