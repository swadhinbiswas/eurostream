import { AlertCircle, Inbox, LoaderCircle } from 'lucide-react';
import { Alert, AlertDescription, AlertTitle, Skeleton } from '@databricks/appkit-ui/react';
import type { ReactNode } from 'react';

interface DataStateProps {
  loading: boolean;
  error: string | null;
  empty?: boolean;
  emptyTitle?: string;
  emptyDescription?: string;
  children: ReactNode;
}

export function DataState({
  loading,
  error,
  empty = false,
  emptyTitle = 'No governed data yet',
  emptyDescription = 'Run the showcase pipelines from Jobs & Pipelines, then refresh this view.',
  children,
}: DataStateProps) {
  if (loading) {
    return (
      <div className="space-y-3" aria-label="Loading governed data">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-28 w-full" />
        <Skeleton className="h-20 w-full" />
      </div>
    );
  }

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="size-4" />
        <AlertTitle>Databricks query failed</AlertTitle>
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }

  if (empty) {
    return (
      <div className="flex min-h-44 flex-col items-center justify-center rounded-2xl border border-dashed border-slate-300 bg-white/60 p-8 text-center">
        <Inbox className="mb-3 size-8 text-slate-400" />
        <p className="font-semibold text-slate-800">{emptyTitle}</p>
        <p className="mt-1 max-w-md text-sm text-slate-500">{emptyDescription}</p>
      </div>
    );
  }

  return <>{children}</>;
}

export function InlineSpinner({ label }: { label: string }) {
  return (
    <span className="inline-flex items-center gap-2 text-sm text-slate-500">
      <LoaderCircle className="size-4 animate-spin" />
      {label}
    </span>
  );
}
