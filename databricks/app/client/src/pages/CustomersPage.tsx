import { useDeferredValue, useMemo, useState } from 'react';
import { Search, ShieldCheck, UsersRound } from 'lucide-react';
import {
  Badge,
  Button,
  Card,
  CardContent,
  Input,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { sql } from '@databricks/appkit-ui/js';
import { DataState } from '@/components/DataState';
import { PageHeader } from '@/components/PageHeader';
import { StatusBadge } from '@/components/StatusBadge';
import { formatCurrency, formatDateTime, formatNumber, formatPercent } from '@/lib/format';

const PAGE_SIZE = 25;

export function CustomersPage() {
  const [search, setSearch] = useState('');
  const [country, setCountry] = useState('ALL');
  const [consent, setConsent] = useState('ALL');
  const [offset, setOffset] = useState(0);
  const deferredSearch = useDeferredValue(search);
  const parameters = useMemo(
    () => ({
      searchTerm: sql.string(deferredSearch.trim()),
      country: sql.string(country === 'ALL' ? '' : country),
      consentFilter: sql.string(consent),
      limit: sql.int(PAGE_SIZE),
      offset: sql.int(offset),
    }),
    [consent, country, deferredSearch, offset]
  );
  const query = useAnalyticsQuery('customers', parameters);
  const rows = query.data ?? [];
  const totalConsented = rows.filter((row) => row.marketing_consent).length;

  return (
    <div className="space-y-8">
      <PageHeader
        eyebrow="Gold layer"
        title="Customer 360"
        description="Search consent-aware customer aggregates. The App intentionally returns no email, IBAN, IP address, or PII hash columns."
      />

      <Card className="border-slate-200 shadow-sm">
        <CardContent className="grid gap-4 p-5 lg:grid-cols-[1fr_180px_220px]">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-400" />
            <Input
              value={search}
              onChange={(event) => {
                setSearch(event.target.value);
                setOffset(0);
              }}
              placeholder="Search customer ID"
              className="pl-9"
              aria-label="Search customer ID"
            />
          </div>
          <Select
            value={country}
            onValueChange={(value) => {
              setCountry(value);
              setOffset(0);
            }}
          >
            <SelectTrigger aria-label="Filter by country">
              <SelectValue placeholder="Country" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">All countries</SelectItem>
              {['DE', 'FR', 'NL', 'ES', 'IT', 'BE', 'AT', 'PL'].map((item) => (
                <SelectItem key={item} value={item}>
                  {item}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select
            value={consent}
            onValueChange={(value) => {
              setConsent(value);
              setOffset(0);
            }}
          >
            <SelectTrigger aria-label="Filter by consent">
              <SelectValue placeholder="Consent" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">All consent states</SelectItem>
              <SelectItem value="OPTED_IN">Opted in</SelectItem>
              <SelectItem value="OPTED_OUT">Opted out</SelectItem>
            </SelectContent>
          </Select>
        </CardContent>
      </Card>

      <DataState loading={query.loading} error={query.error} empty={rows.length === 0}>
        <div className="space-y-4">
          <div className="flex flex-wrap items-center gap-3 text-sm text-slate-600">
            <span className="inline-flex items-center gap-2 font-medium text-slate-800">
              <UsersRound className="size-4 text-sky-600" />
              {formatNumber(rows.length)} rows on this page
            </span>
            <Badge variant="outline">{formatPercent(totalConsented, 0)} opted in on this page</Badge>
          </div>

          <Card className="overflow-hidden border-slate-200 shadow-sm">
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Customer</TableHead>
                    <TableHead>Country</TableHead>
                    <TableHead className="text-right">Orders</TableHead>
                    <TableHead className="text-right">Lifetime spend</TableHead>
                    <TableHead className="text-right">Average order</TableHead>
                    <TableHead>Consent</TableHead>
                    <TableHead>Fraud</TableHead>
                    <TableHead>Last order</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((row) => (
                    <TableRow key={row.customer_id}>
                      <TableCell className="font-mono text-xs font-semibold text-sky-700">{row.customer_id}</TableCell>
                      <TableCell>{row.country || '—'}</TableCell>
                      <TableCell className="text-right font-mono">{formatNumber(row.order_count)}</TableCell>
                      <TableCell className="text-right font-mono">{formatCurrency(row.lifetime_spend_eur)}</TableCell>
                      <TableCell className="text-right font-mono">
                        {formatCurrency(row.average_order_value_eur)}
                      </TableCell>
                      <TableCell>
                        {row.suppressed ? (
                          <StatusBadge value="SUPPRESSED" />
                        ) : row.marketing_consent ? (
                          <Badge className="bg-emerald-100 text-emerald-800">Opted in</Badge>
                        ) : (
                          <Badge variant="secondary">Opted out</Badge>
                        )}
                      </TableCell>
                      <TableCell>
                        {row.fraud_alert_count > 0 ? (
                          <span className="inline-flex items-center gap-1.5 text-amber-700">
                            <ShieldCheck className="size-4" />
                            {formatNumber(row.fraud_alert_count)}
                          </span>
                        ) : (
                          <span className="text-slate-400">0</span>
                        )}
                      </TableCell>
                      <TableCell className="whitespace-nowrap text-xs text-slate-500">
                        {formatDateTime(row.last_order_at)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </Card>

          <div className="flex items-center justify-between">
            <Button
              variant="outline"
              disabled={offset === 0}
              onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
            >
              Previous
            </Button>
            <span className="text-xs font-mono text-slate-500">Offset {offset}</span>
            <Button variant="outline" disabled={rows.length < PAGE_SIZE} onClick={() => setOffset(offset + PAGE_SIZE)}>
              Next
            </Button>
          </div>
        </div>
      </DataState>
    </div>
  );
}
