import { Activity, MapPinned, ShieldAlert, TrendingUp } from 'lucide-react';
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Progress,
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
import { formatDateTime, formatNumber } from '@/lib/format';

const EMPTY_PARAMS: Record<string, never> = {};

export function FraudPage() {
  const query = useAnalyticsQuery('fraud_summary', EMPTY_PARAMS);
  const rows = query.data ?? [];
  const totals = rows.reduce(
    (current, row) => ({
      velocity: current.velocity + row.velocity_alerts,
      zscore: current.zscore + row.zscore_alerts,
      geo: current.geo + row.geo_alerts,
    }),
    { velocity: 0, zscore: 0, geo: 0 }
  );
  const total = totals.velocity + totals.zscore + totals.geo;
  const max = Math.max(1, ...rows.map((row) => row.total_alerts));

  return (
    <div className="space-y-8">
      <PageHeader
        eyebrow="Governed aggregates"
        title="Fraud detection"
        description="A portfolio view of the Databricks fraud pipeline output. It uses Gold summaries rather than exposing raw payment or network data."
      />

      <DataState loading={query.loading} error={query.error} empty={rows.length === 0}>
        <div className="space-y-6">
          <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            <MetricCard
              label="Total alerts"
              value={formatNumber(total)}
              detail={`${formatNumber(rows.length)} customers in the top set`}
              icon={ShieldAlert}
              tone="amber"
            />
            <MetricCard
              label="Velocity"
              value={formatNumber(totals.velocity)}
              detail="More than five payments in the configured window"
              icon={TrendingUp}
              tone="rose"
            />
            <MetricCard
              label="Amount z-score"
              value={formatNumber(totals.zscore)}
              detail="Configured amount anomaly threshold"
              icon={Activity}
              tone="violet"
            />
            <MetricCard
              label="Geo mismatch"
              value={formatNumber(totals.geo)}
              detail="Billing and merchant country differ"
              icon={MapPinned}
              tone="blue"
            />
          </section>

          <Card className="border-slate-200 shadow-sm">
            <CardHeader>
              <CardTitle>Top affected customers</CardTitle>
            </CardHeader>
            <CardContent className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Customer</TableHead>
                    <TableHead>Velocity</TableHead>
                    <TableHead>Z-score</TableHead>
                    <TableHead>Geo</TableHead>
                    <TableHead>Total</TableHead>
                    <TableHead>Max score</TableHead>
                    <TableHead>Last alert</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((row) => (
                    <TableRow key={row.customer_id}>
                      <TableCell className="font-mono text-xs font-semibold text-sky-700">{row.customer_id}</TableCell>
                      <TableCell>{formatNumber(row.velocity_alerts)}</TableCell>
                      <TableCell>{formatNumber(row.zscore_alerts)}</TableCell>
                      <TableCell>{formatNumber(row.geo_alerts)}</TableCell>
                      <TableCell className="min-w-40">
                        <div className="flex items-center gap-3">
                          <Progress value={(row.total_alerts / max) * 100} className="h-2" />
                          <span className="w-8 text-right font-mono text-xs">{formatNumber(row.total_alerts)}</span>
                        </div>
                      </TableCell>
                      <TableCell className="font-mono">{row.max_score?.toFixed(2) ?? '—'}</TableCell>
                      <TableCell className="whitespace-nowrap text-xs text-slate-500">
                        {formatDateTime(row.last_alert_at)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </div>
      </DataState>
    </div>
  );
}
