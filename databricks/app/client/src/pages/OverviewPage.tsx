import {
  Activity,
  BadgeCheck,
  CircleDollarSign,
  Clock3,
  Database,
  ShieldCheck,
  UserRoundCheck,
  UsersRound,
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { DataState } from '@/components/DataState';
import { MetricCard } from '@/components/MetricCard';
import { PageHeader } from '@/components/PageHeader';
import { formatCurrency, formatDateTime, formatNumber, formatPercent, formatSeconds } from '@/lib/format';

const EMPTY_PARAMS: Record<string, never> = {};

export function OverviewPage() {
  const overview = useAnalyticsQuery('overview', EMPTY_PARAMS);
  const consent = useAnalyticsQuery('consent_by_country', EMPTY_PARAMS);
  const row = overview.data?.[0];

  return (
    <div className="space-y-8">
      <PageHeader
        eyebrow="Portfolio showcase"
        title="Databricks-native EuroStream"
        description="A governed Unity Catalog lakehouse with Lakeflow ingestion, quality controls, fraud summaries, auditable Article 17 workflows, and this custom AppKit dashboard. It runs independently from the existing local FastAPI demo."
      />

      <DataState loading={overview.loading} error={overview.error} empty={!row}>
        {row && (
          <>
            <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4" aria-label="Key metrics">
              <MetricCard
                label="Governed customers"
                value={formatNumber(row.customer_count)}
                detail={`${formatNumber(row.consented_customers)} consented to marketing`}
                icon={UsersRound}
                tone="blue"
              />
              <MetricCard
                label="Lifetime spend"
                value={formatCurrency(row.lifetime_spend_eur)}
                detail={`Latest order ${formatDateTime(row.latest_order_at)}`}
                icon={CircleDollarSign}
                tone="emerald"
              />
              <MetricCard
                label="Fraud alerts"
                value={formatNumber(row.fraud_alert_count)}
                detail={`${formatNumber(row.velocity_alerts)} velocity · ${formatNumber(row.zscore_alerts)} z-score · ${formatNumber(row.geo_alerts)} geo`}
                icon={Activity}
                tone="amber"
              />
              <MetricCard
                label="24h quality"
                value={row.quality_pass_rate === null ? 'No run' : formatPercent(row.quality_pass_rate * 100)}
                detail={`Checked ${formatDateTime(row.last_quality_check_at)}`}
                icon={BadgeCheck}
                tone="violet"
              />
              <MetricCard
                label="Erasure requests"
                value={formatNumber(row.erasure_requests)}
                detail={`${formatNumber(row.completed_erasures)} completed · ${formatNumber(row.failed_erasures)} failed`}
                icon={ShieldCheck}
                tone="rose"
              />
              <MetricCard
                label="Average latency"
                value={formatSeconds(row.average_latency_seconds)}
                detail="Workflow execution, internal SLO 60 seconds"
                icon={Clock3}
                tone="blue"
              />
              <MetricCard
                label="Suppressed IDs"
                value={formatNumber(row.suppressed_customers)}
                detail="Blocked from future governed materialization"
                icon={UserRoundCheck}
                tone="violet"
              />
              <MetricCard
                label="Freshness"
                value={row.latest_order_at ? formatDateTime(row.latest_order_at) : 'No data'}
                detail={`Latest fraud ${formatDateTime(row.latest_alert_at)}`}
                icon={Database}
                tone="emerald"
              />
            </section>

            <section className="grid gap-6 lg:grid-cols-2">
              <Card className="border-slate-200 shadow-sm">
                <CardHeader>
                  <CardTitle>Consent distribution</CardTitle>
                  <p className="text-sm text-slate-500">Aggregated Gold data only; no raw identifiers.</p>
                </CardHeader>
                <CardContent>
                  <DataState loading={consent.loading} error={consent.error} empty={(consent.data?.length ?? 0) === 0}>
                    <div className="space-y-5">
                      {consent.data?.map((item) => {
                        const width = Math.max(2, Math.min(100, item.consent_rate_pct));
                        return (
                          <div key={item.country} className="space-y-2">
                            <div className="flex items-center justify-between text-sm">
                              <span className="font-semibold text-slate-800">{item.country || 'Unknown'}</span>
                              <span className="font-mono text-xs text-slate-500">
                                {formatNumber(item.customer_count)} customers · {formatPercent(item.consent_rate_pct)}
                              </span>
                            </div>
                            <div className="h-2.5 overflow-hidden rounded-full bg-slate-100">
                              <div
                                className="h-full rounded-full bg-gradient-to-r from-sky-500 to-emerald-500"
                                style={{ width: `${width}%` }}
                              />
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </DataState>
                </CardContent>
              </Card>

              <Card className="overflow-hidden border-slate-200 bg-slate-950 text-white shadow-sm">
                <CardHeader>
                  <CardTitle className="text-white">What this showcase proves</CardTitle>
                </CardHeader>
                <CardContent className="grid gap-4 sm:grid-cols-2">
                  {[
                    ['Lakeflow', 'Batch and continuous ingestion with schema expectations.'],
                    ['Unity Catalog', 'Delta tables, grants, tags, masks, lineage, and audit evidence.'],
                    ['Lakeflow Jobs', 'Task dependencies, schedules, retries, and parameterized operations.'],
                    ['Databricks App', 'React + TypeScript + AppKit with on-behalf-of SQL queries.'],
                  ].map(([title, description]) => (
                    <div key={title} className="rounded-xl border border-white/10 bg-white/5 p-4">
                      <p className="font-semibold">{title}</p>
                      <p className="mt-1 text-sm leading-5 text-slate-300">{description}</p>
                    </div>
                  ))}
                </CardContent>
              </Card>
            </section>
          </>
        )}
      </DataState>
    </div>
  );
}
