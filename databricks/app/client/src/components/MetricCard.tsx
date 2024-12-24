import type { LucideIcon } from 'lucide-react';
import { Card, CardContent } from '@databricks/appkit-ui/react';
import { cn } from '@/lib/utils';

interface MetricCardProps {
  label: string;
  value: string;
  detail: string;
  icon: LucideIcon;
  tone?: 'blue' | 'emerald' | 'amber' | 'violet' | 'rose';
}

const tones = {
  blue: 'bg-sky-50 text-sky-700 ring-sky-100',
  emerald: 'bg-emerald-50 text-emerald-700 ring-emerald-100',
  amber: 'bg-amber-50 text-amber-700 ring-amber-100',
  violet: 'bg-violet-50 text-violet-700 ring-violet-100',
  rose: 'bg-rose-50 text-rose-700 ring-rose-100',
};

export function MetricCard({ label, value, detail, icon: Icon, tone = 'blue' }: MetricCardProps) {
  return (
    <Card className="border-slate-200/80 shadow-sm">
      <CardContent className="flex items-start justify-between gap-4 p-5">
        <div className="min-w-0 space-y-2">
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">{label}</p>
          <p className="text-3xl font-bold tracking-tight text-slate-950">{value}</p>
          <p className="truncate text-xs text-slate-500">{detail}</p>
        </div>
        <span className={cn('grid size-10 shrink-0 place-items-center rounded-xl ring-1', tones[tone])}>
          <Icon className="size-5" />
        </span>
      </CardContent>
    </Card>
  );
}
