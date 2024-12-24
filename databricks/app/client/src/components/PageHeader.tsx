import type { ReactNode } from 'react';

interface PageHeaderProps {
  eyebrow: string;
  title: string;
  description: string;
  action?: ReactNode;
}

export function PageHeader({ eyebrow, title, description, action }: PageHeaderProps) {
  return (
    <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
      <div className="max-w-3xl space-y-2">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-sky-600">{eyebrow}</p>
        <h1 className="text-3xl font-bold tracking-tight text-slate-950">{title}</h1>
        <p className="text-sm leading-6 text-slate-600">{description}</p>
      </div>
      {action}
    </div>
  );
}
