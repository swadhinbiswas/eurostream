import { useMemo } from 'react';
import { Link } from 'react-router';
import { ArrowRight, BadgeCheck, ClipboardCheck, FileClock, ShieldAlert } from 'lucide-react';
import { sql } from '@databricks/appkit-ui/js';
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { DataState } from '@/components/DataState';
import { MetricCard } from '@/components/MetricCard';
import { PageHeader } from '@/components/PageHeader';
import { StatusBadge } from '@/components/StatusBadge';
import { formatDateTime, formatNumber, formatPercent, formatSeconds } from '@/lib/format';

const EMPTY_PARAMS: Record<string, never> = {};

export function GovernancePage() {
  const quality = useAnalyticsQuery('quality_latest', EMPTY_PARAMS);
  const auditParams = useMemo(() => ({ limit: sql.int(25) }), []);
  const audit = useAnalyticsQuery('erasure_audit', auditParams);
  const checks = quality.data ?? [];
  const requests = audit.data ?? [];
  const passCount = checks.filter((check) => check.passed).length;
  const completed = requests.filter((request) => request.status.toLowerCase() === 'completed').length;
  const failed = requests.filter((request) => request.status.toLowerCase() === 'failed').length;

  return (
    <div className="space-y-8">
      <PageHeader
        eyebrow="Evidence and controls"
        title="Governance center"
        description="Quality-gate outcomes and fail-closed Article 17 evidence are read directly from governed Delta tables through on-behalf-of SQL."
        action={
          <Link
            to="/erasure"
            className="inline-flex h-9 items-center justify-center rounded-md bg-slate-950 px-4 text-sm font-medium text-white transition hover:bg-slate-800"
          >
            Open erasure console <ArrowRight className="ml-2 size-4" />
          </Link>
        }
      />

      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label="Latest checks passed"
          value={`${passCount}/${checks.length}`}
          detail="Most recent result for each named gate"
          icon={BadgeCheck}
          tone="emerald"
        />
        <MetricCard
          label="Pass rate"
          value={checks.length ? formatPercent((passCount / checks.length) * 100) : 'No data'}
          detail="Current quality snapshot"
          icon={ClipboardCheck}
          tone="violet"
        />
        <MetricCard
          label="Completed erasures"
          value={formatNumber(completed)}
          detail={`${formatNumber(requests.length)} recent command records`}
          icon={FileClock}
          tone="blue"
        />
        <MetricCard
          label="Failed erasures"
          value={formatNumber(failed)}
          detail="Failures remain visible for reconciliation"
          icon={ShieldAlert}
          tone="rose"
        />
      </section>

      <Card className="border-slate-200 shadow-sm">
        <CardHeader>
          <CardTitle>Latest quality-gate results</CardTitle>
        </CardHeader>
        <CardContent>
          <DataState loading={quality.loading} error={quality.error} empty={checks.length === 0}>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Check</TableHead>
                    <TableHead>Result</TableHead>
                    <TableHead>Detail</TableHead>
                    <TableHead>Checked</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {checks.map((check) => (
                    <TableRow key={check.check_name}>
                      <TableCell className="font-mono text-xs font-semibold">{check.check_name}</TableCell>
                      <TableCell>
                        <StatusBadge value={check.passed ? 'PASSED' : 'FAILED'} />
                      </TableCell>
                      <TableCell className="max-w-2xl text-sm text-slate-600">{check.detail ?? '—'}</TableCell>
                      <TableCell className="whitespace-nowrap text-xs text-slate-500">
                        {formatDateTime(check.checked_at)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </DataState>
        </CardContent>
      </Card>

      <Card className="border-slate-200 shadow-sm">
        <CardHeader>
          <CardTitle>Recent Article 17 commands</CardTitle>
        </CardHeader>
        <CardContent>
          <DataState loading={audit.loading} error={audit.error} empty={requests.length === 0}>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Request</TableHead>
                    <TableHead>Customer / ticket</TableHead>
                    <TableHead>Actor</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Latency</TableHead>
                    <TableHead>Logical</TableHead>
                    <TableHead>Export</TableHead>
                    <TableHead>Fraud sink files</TableHead>
                    <TableHead>Requested</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {requests.map((request) => (
                    <TableRow key={request.request_id}>
                      <TableCell className="font-mono text-[11px]">{request.request_id.slice(0, 12)}…</TableCell>
                      <TableCell>
                        <p className="font-mono text-xs font-semibold">{request.customer_id}</p>
                        <p className="text-[11px] text-slate-500">{request.ticket_id}</p>
                      </TableCell>
                      <TableCell className="max-w-48 truncate text-xs">{request.requested_by}</TableCell>
                      <TableCell>
                        <StatusBadge value={request.status} />
                        {request.blocked_layer && (
                          <p className="mt-1 max-w-52 text-[10px] leading-4 text-amber-700">
                            {request.next_action ?? request.blocked_layer}
                          </p>
                        )}
                      </TableCell>
                      <TableCell className="font-mono text-xs">{formatSeconds(request.latency_seconds)}</TableCell>
                      <TableCell>
                        <StatusBadge value={request.logical_verified ? 'VERIFIED' : 'PENDING'} />
                      </TableCell>
                      <TableCell>
                        <StatusBadge value={request.export_verified ? 'VERIFIED' : 'PENDING'} />
                      </TableCell>
                      <TableCell>
                        <StatusBadge value={request.physical_purge_status} />
                      </TableCell>
                      <TableCell className="whitespace-nowrap text-xs text-slate-500">
                        {formatDateTime(request.requested_at)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </DataState>
        </CardContent>
      </Card>
    </div>
  );
}
